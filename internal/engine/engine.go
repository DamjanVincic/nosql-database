package engine

import (
	"github.com/DamjanVincic/key-value-engine/internal/engine/config"
	"github.com/DamjanVincic/key-value-engine/internal/models"
	"github.com/DamjanVincic/key-value-engine/internal/structures/cache"
	"github.com/DamjanVincic/key-value-engine/internal/structures/memtable"
	"github.com/DamjanVincic/key-value-engine/internal/structures/sstable"
	"github.com/DamjanVincic/key-value-engine/internal/structures/wal"
)

type Engine struct {
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
				return nil, nil
			}

			engine.cache.Put(entry_)
			entry = entry_
		} else if entry.Tombstone {
			return nil, nil
		}
	} else if entry.Tombstone {
		return nil, nil
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
