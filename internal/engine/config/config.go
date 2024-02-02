package config

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
	"os"
)

const ConfigFile = "config.yaml"

type Config struct {
	WAL      WALConfig      `yaml:"wal"`
	Memtable MemtableConfig `yaml:"memtable"`
	SSTable  SSTableConfig  `yaml:"sstable"`
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

// Default Config
var config = &Config{
	WAL: WALConfig{
		SegmentSize: 1024 * 1024 * 16,
	},
	Memtable: MemtableConfig{
		TableSize:      1024 * 1024 * 16,
		DataStructure:  "hashmap",
		NumberOfTables: 1,
	},
	SSTable: SSTableConfig{
		IndexThinningDegree:   10,
		SummaryThinningDegree: 10,
		SingleFile:            false,
		Compression:           false,

		CompactionAlgorithm: "sizetiered",
		MaxLevel:            4,
		LevelSize:           2,
		LevelSizeMultiplier: 10,
	},
}

// return default if an error is thrown while loading config
func LoadConfig() (*Config, error) {
	file, err := os.ReadFile(ConfigFile)
	if err != nil {
		return nil, err
	}

	var fileConfig = &Config{}

	if err = yaml.Unmarshal(file, fileConfig); err != nil {
		return nil, err
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
