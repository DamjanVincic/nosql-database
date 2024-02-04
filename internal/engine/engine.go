package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/engine/config"
	"github.com/DamjanVincic/key-value-engine/internal/engine/tokenbucket"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/bloomfilter"
	"github.com/DamjanVincic/key-value-engine/internal/structures/cache"
	"github.com/DamjanVincic/key-value-engine/internal/structures/countminsketch"
	hyperLogLog "github.com/DamjanVincic/key-value-engine/internal/structures/hyperloglog"
	"github.com/DamjanVincic/key-value-engine/internal/structures/memtable"
	"github.com/DamjanVincic/key-value-engine/internal/structures/simhash"
	"github.com/DamjanVincic/key-value-engine/internal/structures/sstable"
	"github.com/DamjanVincic/key-value-engine/internal/structures/wal"
	"strings"
)

const (
	TokenBucketPrefix = "tb_"
	BFPrefix          = "bf_"
	CMSPrefix         = "cms_"
	HLLPrefix         = "hll_"
	SimhashPrefix     = "simhash_"
)

var Prefixes = []string{TokenBucketPrefix, BFPrefix, CMSPrefix, HLLPrefix, SimhashPrefix}

type Engine struct {
	conf     *config.Config
	wal      *wal.WAL
	memtable *memtable.Memtable
	sstable  *sstable.SSTable
	cache    *cache.Cache
}

func NewEngine() (*Engine, error) {
	conf := config.LoadConfig()

	sst, err := sstable.NewSSTable(conf.SSTable.IndexThinningDegree, conf.SSTable.SummaryThinningDegree, conf.SSTable.SingleFile, conf.SSTable.Compression, conf.SSTable.CompactionAlgorithm, conf.SSTable.MaxLevel, conf.SSTable.LevelSize, conf.SSTable.LevelSizeMultiplier, conf.SSTable.BloomFilterFalsePositiveRate)
	if err != nil {
		return nil, err
	}

	che := cache.NewCache(conf.Cache.Size)

	writeAheadLog, err := wal.NewWAL(conf.WAL.SegmentSize, sst, che)
	if err != nil {
		return nil, err
	}

	mt := memtable.NewMemtable(conf.Memtable.TableSize, conf.Memtable.DataStructure, conf.Memtable.NumberOfTables)
	if err != nil {
		return nil, err
	}

	err = loadWAL(writeAheadLog, mt)
	if err != nil {
		return nil, err
	}

	return &Engine{
		conf:     conf,
		wal:      writeAheadLog,
		memtable: mt,
		sstable:  sst,
		cache:    che,
	}, nil
}

func loadWAL(writeAheadLog *wal.WAL, mt *memtable.Memtable) error {
	err := writeAheadLog.ReadRecords(mt)
	if err != nil {
		return err
	}

	return nil
}

func (engine *Engine) Put(key string, value []byte) error {
	used, err := engine.checkTokenBucket()
	if err != nil {
		return err
	}

	if !used {
		return errors.New("no tokens remaining")
	}

	if engine.hasPrefix(key) {
		return errors.New("key not found")
	}

	entry, err := engine.wal.AddRecord(key, value, false)
	if err != nil {
		return err
	}

	toFlush := engine.memtable.Put(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
	if toFlush != nil {
		err := engine.sstable.Write(toFlush)
		if err != nil {
			return err
		}

		err = engine.wal.MoveLowWatermark(engine.memtable.FindNewestTimestamp(toFlush))
		if err != nil {
			return err
		}

		err = engine.wal.DeleteSegments()
		if err != nil {
			return err
		}

		engine.cache.Update(toFlush)
	}

	return nil
}

func (engine *Engine) Get(key string) ([]byte, error) {
	used, err := engine.checkTokenBucket()
	if err != nil {
		return nil, err
	}

	if !used {
		return nil, errors.New("no tokens remaining")
	}

	if engine.hasPrefix(key) {
		return nil, errors.New("key not found")
	}

	var entry *models.Data

	entry = engine.memtable.Get(key)
	if entry == nil {
		entry = engine.cache.Get(key)

		if entry == nil {
			entry_, err := engine.sstable.Get(key)
			if err != nil {
				return nil, err
			}

			if entry_ == nil {
				return nil, errors.New("key not found")
			}

			engine.cache.Put(entry_)
			entry = entry_
		} else if entry.Tombstone {
			return nil, errors.New("key not found")
		}
	} else if entry.Tombstone {
		return nil, errors.New("key not found")
	}

	return entry.Value, nil
}

func (engine *Engine) Delete(key string) error {
	used, err := engine.checkTokenBucket()
	if err != nil {
		return err
	}

	if !used {
		return errors.New("no tokens remaining")
	}

	if engine.hasPrefix(key) {
		return errors.New("key not found")
	}

	entry, err := engine.wal.AddRecord(key, nil, true)
	if err != nil {
		return err
	}

	engine.memtable.Put(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)

	return nil
}

func (engine *Engine) PrefixScan(prefix string, pageNumber, pageSize int) ([]*models.Data, error) {
	if engine.hasPrefix(prefix) {
		return nil, errors.New("invalid prefix")
	}

	entries, err := engine.sstable.PrefixScan(prefix, uint64(pageNumber), uint64(pageSize), engine.memtable)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

func (engine *Engine) RangeScan(minString, maxString string, pageNumber, pageSize int) ([]*models.Data, error) {
	if engine.hasPrefix(minString) || engine.hasPrefix(maxString) {
		return nil, errors.New("invalid prefix")
	}

	entries, err := engine.sstable.RangeScan(minString, maxString, uint64(pageNumber), uint64(pageSize), engine.memtable)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

func (engine *Engine) PrefixIterate(prefix string) (*sstable.PrefixIterator, *models.Data, error) {
	iter, entry, err := engine.sstable.NewPrefixIterator(prefix, engine.memtable)
	if err != nil {
		return nil, nil, err
	}

	return iter, entry, nil
}

func (engine *Engine) RangeIterate(minKey, maxKey string) (*sstable.RangeIterator, *models.Data, error) {
	iter, entry, err := engine.sstable.NewRangeIterator(minKey, maxKey, engine.memtable)
	if err != nil {
		return nil, nil, err
	}

	return iter, entry, nil
}

func (engine *Engine) BloomFilter(key string) error {
	fmt.Println("1. Create bloom filter")
	fmt.Println("2. Delete bloom filter")
	fmt.Println("3. Add element to bloom filter")
	fmt.Println("4. Check element in bloom filter")
	fmt.Println("5. Exit")
	fmt.Print("> ")

	var expr int
	_, err := fmt.Scan(&expr)
	if err != nil {
		return err
	}

	switch expr {
	case 1:
		fmt.Print("Expected elements, false positive rate: ")
		var expectedElements int
		var falsePositiveRate float64
		_, err := fmt.Scanf("%d %f", &expectedElements, &falsePositiveRate)
		if err != nil {
			return err
		}

		key = BFPrefix + key

		bf := bloomfilter.CreateBloomFilter(expectedElements, falsePositiveRate)
		err = engine.put(key, bf.Serialize())
		if err != nil {
			return err
		}
		fmt.Println("Bloom filter created")
	case 2:
		key = BFPrefix + key
		err := engine.delete(key)
		if err != nil {
			return err
		}
		fmt.Println("Bloom filter deleted")
	case 3:
		fmt.Print("Key to add: ")
		var keyToAdd string
		_, err := fmt.Scanf("%s", &keyToAdd)
		if err != nil {
			return err
		}

		key = BFPrefix + key
		bf, err := engine.get(key)
		if err != nil {
			return err
		}

		deserializedBF := bloomfilter.Deserialize(bf)
		err = deserializedBF.AddElement([]byte(keyToAdd))
		if err != nil {
			return err
		}

		err = engine.put(key, deserializedBF.Serialize())
		if err != nil {
			return err
		}

		fmt.Println("Element added to bloom filter")
	case 4:
		fmt.Print("Key to check: ")
		var keyToCheck string
		_, err := fmt.Scanf("%s", &keyToCheck)
		if err != nil {
			return err
		}

		key = BFPrefix + key
		bf, err := engine.get(key)
		if err != nil {
			return err
		}

		deserializedBF := bloomfilter.Deserialize(bf)
		ok, err := deserializedBF.ContainsElement([]byte(keyToCheck))
		if err != nil {
			return err
		}

		if ok {
			fmt.Println("Element is in bloom filter")
		} else {
			fmt.Println("Element is not in bloom filter")
		}
	case 5:
		return nil
	}

	return nil
}

func (engine *Engine) CountMinSketch(key string) error {
	fmt.Println("1. Create cms")
	fmt.Println("2. Delete cms")
	fmt.Println("3. Add element to cms")
	fmt.Println("4. Check element frequency in cms")
	fmt.Println("5. Exit")
	fmt.Print("> ")

	var expr int
	_, err := fmt.Scan(&expr)
	if err != nil {
		return err
	}

	switch expr {
	case 1:
		fmt.Print("Error rate, probability: ")
		var errorRate, probability float64
		_, err := fmt.Scanf("%f %f", &errorRate, &probability)
		if err != nil {
			return err
		}

		key = CMSPrefix + key

		cms := countminsketch.NewCMS(errorRate, probability)
		err = engine.put(key, cms.Serialize())
		if err != nil {
			return err
		}
		fmt.Println("CMS created")
	case 2:
		key = CMSPrefix + key
		err := engine.delete(key)
		if err != nil {
			return err
		}
		fmt.Println("CMS deleted")
	case 3:
		fmt.Print("Key to add: ")
		var keyToAdd string
		_, err := fmt.Scanf("%s", &keyToAdd)
		if err != nil {
			return err
		}

		key = CMSPrefix + key
		cms, err := engine.get(key)
		if err != nil {
			return err
		}

		deserializedCMS := countminsketch.Deserialize(cms)
		err = deserializedCMS.Add([]byte(keyToAdd))
		if err != nil {
			return err
		}

		err = engine.put(key, deserializedCMS.Serialize())
		if err != nil {
			return err
		}

		fmt.Println("Element added to CMS")
	case 4:
		fmt.Print("Key to check: ")
		var keyToCheck string
		_, err := fmt.Scanf("%s", &keyToCheck)
		if err != nil {
			return err
		}

		key = CMSPrefix + key
		cms, err := engine.get(key)
		if err != nil {
			return err
		}

		deserializedCMS := countminsketch.Deserialize(cms)
		frequency, err := deserializedCMS.GetFrequency([]byte(keyToCheck))
		if err != nil {
			return err
		}

		fmt.Println(fmt.Sprintf("Element frequency: %d", frequency))
	case 5:
		return nil
	}

	return nil
}

func (engine *Engine) HyperLogLog(key string) error {
	fmt.Println("1. Create HLL")
	fmt.Println("2. Delete HLL")
	fmt.Println("3. Add element to HLL")
	fmt.Println("4. Check cardinality")
	fmt.Println("5. Exit")
	fmt.Print("> ")

	var expr int
	_, err := fmt.Scan(&expr)
	if err != nil {
		return err
	}

	switch expr {
	case 1:
		fmt.Print("Bucket bits: ")
		var bucketBits int
		_, err := fmt.Scanf("%d", &bucketBits)
		if err != nil {
			return err
		}

		key = HLLPrefix + key

		hll, err := hyperLogLog.NewHyperLogLog(uint8(bucketBits))
		err = engine.put(key, hll.Serialize())
		if err != nil {
			return err
		}
		fmt.Println("HLL created")
	case 2:
		key = HLLPrefix + key
		err := engine.delete(key)
		if err != nil {
			return err
		}
		fmt.Println("HLL deleted")
	case 3:
		fmt.Print("Key to add: ")
		var keyToAdd string
		_, err := fmt.Scanf("%s", &keyToAdd)
		if err != nil {
			return err
		}

		key = HLLPrefix + key
		hll, err := engine.get(key)
		if err != nil {
			return err
		}

		deserializedHLL := hyperLogLog.Deserialize(hll)
		err = deserializedHLL.Add([]byte(keyToAdd))
		if err != nil {
			return err
		}

		err = engine.put(key, deserializedHLL.Serialize())
		if err != nil {
			return err
		}

		fmt.Println("Element added to HLL")
	case 4:
		key = HLLPrefix + key
		hll, err := engine.get(key)
		if err != nil {
			return err
		}

		deserializedHLL := hyperLogLog.Deserialize(hll)
		cardinality := deserializedHLL.Estimate()

		fmt.Println(fmt.Sprintf("Cardinality: %d", cardinality))
	case 5:
		return nil
	}

	return nil
}

func (engine *Engine) Simhash(key string) error {
	fmt.Println("1. Store fingerprint")
	fmt.Println("2. Check similarity")
	fmt.Println("3. Exit")
	fmt.Print("> ")

	var expr int
	_, err := fmt.Scan(&expr)
	if err != nil {
		return err
	}

	switch expr {
	case 1:
		fmt.Print("Text: ")
		var text string
		_, err := fmt.Scanln(&text)
		if err != nil {
			return err
		}

		key = SimhashPrefix + key

		shash, err := simhash.GetFingerprint(text)
		if err != nil {
			return err
		}

		var shashBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(shashBytes, shash)

		err = engine.put(key, shashBytes)
		if err != nil {
			return err
		}
		fmt.Println("Fingerprint stored")
	case 2:
		fmt.Print("Text: ")
		var text string
		_, err := fmt.Scanln(&text)
		if err != nil {
			return err
		}

		key = SimhashPrefix + key
		shash, err := engine.get(key)
		if err != nil {
			return err
		}

		shashInt := binary.BigEndian.Uint64(shash)
		newShash, err := simhash.GetFingerprint(text)
		if err != nil {
			return err
		}

		similarity := simhash.GetHammingDistance(shashInt, newShash)
		fmt.Println(fmt.Sprintf("Similarity: %d", similarity))
	case 3:
		return nil
	}

	return nil
}

func (engine *Engine) hasPrefix(key string) bool {
	for _, prefix := range Prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (engine *Engine) get(key string) ([]byte, error) {
	var entry *models.Data

	entry = engine.memtable.Get(key)
	if entry == nil {
		entry = engine.cache.Get(key)

		if entry == nil {
			entry_, err := engine.sstable.Get(key)
			if err != nil {
				return nil, err
			}

			if entry_ == nil {
				return nil, errors.New("key not found")
			}

			engine.cache.Put(entry_)
			entry = entry_
		} else if entry.Tombstone {
			return nil, errors.New("key not found")
		}
	} else if entry.Tombstone {
		return nil, errors.New("key not found")
	}

	return entry.Value, nil
}

func (engine *Engine) put(key string, value []byte) error {
	used, err := engine.checkTokenBucket()
	if err != nil {
		return err
	}

	if !used {
		return errors.New("no tokens remaining")
	}

	entry, err := engine.wal.AddRecord(key, value, false)
	if err != nil {
		return err
	}

	toFlush := engine.memtable.Put(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
	if toFlush != nil {
		err := engine.sstable.Write(toFlush)
		if err != nil {
			return err
		}

		err = engine.wal.MoveLowWatermark(engine.memtable.FindNewestTimestamp(toFlush))
		if err != nil {
			return err
		}

		err = engine.wal.DeleteSegments()
		if err != nil {
			return err
		}

		engine.cache.Update(toFlush)
	}

	return nil
}

func (engine *Engine) delete(key string) error {
	entry, err := engine.wal.AddRecord(key, nil, true)
	if err != nil {
		return err
	}

	engine.memtable.Put(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)

	return nil
}

func (engine *Engine) tokenBucketGet(key string) ([]byte, error) {
	var entry *models.Data

	entry = engine.memtable.Get(key)
	if entry == nil {
		entry = engine.cache.Get(key)

		if entry == nil {
			entry_, err := engine.sstable.Get(key)
			if err != nil {
				return nil, err
			}

			if entry_ == nil {
				return nil, errors.New("key not found")
			}

			engine.cache.Put(entry_)
			entry = entry_
		} else if entry.Tombstone {
			return nil, errors.New("key not found")
		}
	} else if entry.Tombstone {
		return nil, errors.New("key not found")
	}

	return entry.Value, nil
}

func (engine *Engine) tokenBucketPut(key string, value []byte) error {
	entry, err := engine.wal.AddRecord(key, value, false)
	if err != nil {
		return err
	}

	toFlush := engine.memtable.Put(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
	if toFlush != nil {
		err := engine.sstable.Write(toFlush)
		if err != nil {
			return err
		}

		err = engine.wal.MoveLowWatermark(engine.memtable.FindNewestTimestamp(toFlush))
		if err != nil {
			return err
		}

		err = engine.wal.DeleteSegments()
		if err != nil {
			return err
		}

		engine.cache.Update(toFlush)
	}

	return nil
}

func (engine *Engine) checkTokenBucket() (bool, error) {
	serializedTB, err := engine.tokenBucketGet(TokenBucketPrefix)
	var used bool
	if err != nil {
		if err.Error() == "key not found" {
			tb := tokenbucket.NewTokenBucket(engine.conf.TokenBucket.Capacity, engine.conf.TokenBucket.RefillPeriod)
			used = tb.UseToken()

			err := engine.tokenBucketPut(TokenBucketPrefix, tb.Serialize())
			if err != nil {
				return false, err
			}

			return used, nil
		} else {
			return false, err
		}
	}

	tb := tokenbucket.Deserialize(serializedTB, engine.conf.TokenBucket.Capacity, engine.conf.TokenBucket.RefillPeriod)
	used = tb.UseToken()

	err = engine.tokenBucketPut(TokenBucketPrefix, tb.Serialize())
	if err != nil {
		return false, err
	}

	return used, nil
}
