package engine

import (
	"errors"
	"github.com/DamjanVincic/key-value-engine/internal/engine/config"
	"github.com/DamjanVincic/key-value-engine/internal/engine/tokenbucket"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/cache"
	"github.com/DamjanVincic/key-value-engine/internal/structures/memtable"
	"github.com/DamjanVincic/key-value-engine/internal/structures/sstable"
	"github.com/DamjanVincic/key-value-engine/internal/structures/wal"
	"strings"
)

const (
	TokenBucketPrefix = "tb_"
)

var Prefixes = []string{TokenBucketPrefix}

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

func (engine *Engine) checkTokenBucket() (bool, error) {
	serializedTB, err := engine.get(TokenBucketPrefix)
	var used bool
	if err != nil {
		if err.Error() == "key not found" {
			tb := tokenbucket.NewTokenBucket(engine.conf.TokenBucket.Capacity, engine.conf.TokenBucket.RefillPeriod)
			used = tb.UseToken()

			err := engine.Put(TokenBucketPrefix, tb.Serialize())
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

	err = engine.Put(TokenBucketPrefix, tb.Serialize())
	if err != nil {
		return false, err
	}

	return used, nil
}
