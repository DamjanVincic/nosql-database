package config

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
	"os"
)

const ConfigFile = "config.yaml"

type Config struct {
	WAL         WALConfig         `yaml:"wal"`
	Memtable    MemtableConfig    `yaml:"memtable"`
	SSTable     SSTableConfig     `yaml:"sstable"`
	Cache       CacheConfig       `yaml:"cache"`
	TokenBucket TokenBucketConfig `yaml:"tokenbucket"`
}

type WALConfig struct {
	SegmentSize uint64 `yaml:"segmentSize" validate:"gte=1"`
}

type MemtableConfig struct {
	TableSize      uint64 `yaml:"tableSize" validate:"gte=1"`
	DataStructure  string `yaml:"dataStructure" validate:"oneof=hashmap skiplist btree"`
	NumberOfTables uint64 `yaml:"numberOfTables" validate:"gte=1"`
}

type SSTableConfig struct {
	IndexThinningDegree   uint16 `yaml:"indexThinningDegree" validate:"gte=1"`
	SummaryThinningDegree uint16 `yaml:"summaryThinningDegree" validate:"gte=1"`
	SingleFile            bool   `yaml:"singleFile"`
	Compression           bool   `yaml:"compression"`

	CompactionAlgorithm string `yaml:"compactionAlgorithm" validate:"oneof=sizetiered leveled"`
	MaxLevel            uint8  `yaml:"maxLevel" validate:"gte=4"`
	LevelSize           uint64 `yaml:"levelSize" validate:"gte=2"`
	LevelSizeMultiplier uint64 `yaml:"levelSizeMultiplier" validate:"gte=10"`
}

type CacheConfig struct {
	Size uint64 `yaml:"size" validate:"gte=1"`
}

type TokenBucketConfig struct {
	Capacity     uint64 `yaml:"capacity" validate:"gte=1"`
	RefillPeriod uint64 `yaml:"refillPeriod"`
}

// Default Config
var config = &Config{
	WAL: WALConfig{
		SegmentSize: 1024,
	},
	Memtable: MemtableConfig{
		TableSize:      1000,
		DataStructure:  "hashmap",
		NumberOfTables: 1,
	},
	SSTable: SSTableConfig{
		IndexThinningDegree:   5,
		SummaryThinningDegree: 5,
		SingleFile:            false,
		Compression:           false,

		CompactionAlgorithm: "sizetiered",
		MaxLevel:            4,
		LevelSize:           2,
		LevelSizeMultiplier: 1,
	},
	Cache: CacheConfig{
		Size: 10,
	},
	TokenBucket: TokenBucketConfig{
		Capacity:     3,
		RefillPeriod: 10,
	},
}

// return default if an error is thrown while loading config
func LoadConfig() (*Config, error) {
	file, err := os.ReadFile(ConfigFile)
	if err != nil {
		fmt.Println("Error while reading config file, using default config")
		return config, err
	}

	var fileConfig = &Config{}

	if err = yaml.Unmarshal(file, fileConfig); err != nil {
		fmt.Println("Error while unmarshalling config, using default config")
		return config, err
	}

	validate := validator.New(validator.WithRequiredStructEnabled())
	if err = validate.Struct(fileConfig); err != nil {
		//return nil, err
		fmt.Println("Error while validating config, using default config")
	} else {
		config = fileConfig
	}

	return config, nil
}
