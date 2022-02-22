package config

import (
	"encoding/json"
	"io/ioutil"
)

type WalConfig struct {
	SegmentCapacity int `json:"wal_segment_capacity"`
}

type HLLConfig struct {
	HLLPrecision    int `json:"hll_precision"`
}

type CacheConfig struct {
	CacheMaxData int `json:"cache_max_data"`
}

type LSMConfig struct {
	LSMMaxLevel  int `json:"lsm_max_level"`
	LSMLevelSize int `json:"lsm_level_size"`
}

type TokenBucketConfig struct {
	TokenBucketMaxTokens int `json:"token_bucket_max_tokens"`
	TokenBucketInterval  int `json:"token_bucket_interval"`
}

type MemTableConfig struct {
	SkipListMaxHeight int `json:"skip_list_max_height"`
	MaxMemTableSize   int `json:"max_mem_table_size"`
	MemTableThreshold int `json:"mem_table_threshold"`
}

type Config struct {
	WalParameters         WalConfig         `json:"wal_config"`
	HLLParameters         HLLConfig         `json:"hll_config"`
	CacheParameters       CacheConfig       `json:"cache_config"`
	LSMParameters         LSMConfig         `json:"lsm_config"`
	TokenBucketParameters TokenBucketConfig `json:"token_bucket_config"`
	MemTableParameters    MemTableConfig    `json:"mem_table_config"`
}

func GetSystemConfig() (config *Config) {
	config = new(Config)

	jsonBytes, err := ioutil.ReadFile("config/config.json")
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(jsonBytes, config)
	if err != nil {
		panic(err)
	}

	if config.WalParameters.SegmentCapacity == -1 {
		config.WalParameters.SegmentCapacity = 50
	}
	if config.HLLParameters.HLLPrecision == -1 {
		config.HLLParameters.HLLPrecision = 4
	}
	if config.CacheParameters.CacheMaxData == -1 {
		config.CacheParameters.CacheMaxData = 5
	}
	if config.LSMParameters.LSMMaxLevel == -1 {
		config.LSMParameters.LSMMaxLevel = 3
	}
	if config.LSMParameters.LSMLevelSize == -1 {
		config.LSMParameters.LSMLevelSize = 2
	}
	if config.TokenBucketParameters.TokenBucketMaxTokens == -1 {
		config.TokenBucketParameters.TokenBucketMaxTokens = 1000
	}
	if config.TokenBucketParameters.TokenBucketInterval == -1 {
		config.TokenBucketParameters.TokenBucketInterval = 100
	}
	if config.MemTableParameters.SkipListMaxHeight == -1 {
		config.MemTableParameters.SkipListMaxHeight = 5
	}
	if config.MemTableParameters.MaxMemTableSize == -1 {
		config.MemTableParameters.MaxMemTableSize = 5
	}
	if config.MemTableParameters.MemTableThreshold == -1 {
		config.MemTableParameters.MemTableThreshold = 60
	}

	return
}

func CreateConfigFile() {
	config := new(Config)
	config.LSMParameters.LSMMaxLevel = -1
	config.LSMParameters.LSMLevelSize = -1
	config.WalParameters.SegmentCapacity = -1
	config.HLLParameters.HLLPrecision = -1
	config.CacheParameters.CacheMaxData = -1
	config.TokenBucketParameters.TokenBucketMaxTokens = -1
	config.TokenBucketParameters.TokenBucketInterval = -1
	config.MemTableParameters.SkipListMaxHeight = -1
	config.MemTableParameters.MemTableThreshold = -1
	config.MemTableParameters.MaxMemTableSize = -1

	file, _ := json.MarshalIndent(config, "", "  ")

	_ = ioutil.WriteFile("config/config.json", file, 0644)
}
