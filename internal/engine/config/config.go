package config

import (
	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
	"os"
)

const ConfigFile = "config.yaml"

type Config struct {
	WAL      WALConfig      `yaml:"wal"`
	Memtable MemtableConfig `yaml:"memtable"`
}

type WALConfig struct {
	SegmentSize uint64 `yaml:"segmentSize" validate:"gte=1"`
}

type MemtableConfig struct {
	TableSize      uint64 `yaml:"tableSize" validate:"gte=1"`
	DataStructure  string `yaml:"dataStructure" validate:"oneof=hashmap skiplist btree"`
	NumberOfTables uint64 `yaml:"numberOfTables" validate:"gte=1"`
}

var defaultConfig = &Config{
	WAL: WALConfig{
		SegmentSize: 1024 * 1024 * 16,
	},
	Memtable: MemtableConfig{
		TableSize:      1024 * 1024 * 16,
		DataStructure:  "hashmap",
		NumberOfTables: 1,
	},
}

// return default if an error is thrown while loading config
func LoadConfig() (*Config, error) {
	file, err := os.ReadFile(ConfigFile)
	if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(file, defaultConfig); err != nil {
		return nil, err
	}

	validate := validator.New(validator.WithRequiredStructEnabled())
	if err = validate.Struct(defaultConfig); err != nil {
		return nil, err
	}

	return defaultConfig, nil
}
