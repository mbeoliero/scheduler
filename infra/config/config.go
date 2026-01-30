package config

import (
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	MySql     MySqlConfig     `mapstructure:"mysql"`
	Redis     RedisConfig     `mapstructure:"redis"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
}

type ServerConfig struct {
	Port   int    `mapstructure:"port"`
	NodeId string `mapstructure:"node_id"`
}

type MySqlConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	Database     string `mapstructure:"database"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type SchedulerConfig struct {
	SchedulerKeyPrefix    string        `mapstructure:"scheduler_key_prefix"`
	SchedulerLoopInterval time.Duration `mapstructure:"scheduler_loop_interval"`
	LeaderKey             string        `mapstructure:"leader_key"`
	PreReadSeconds        int           `mapstructure:"pre_read_seconds"`
	EnableTaskQueue       bool          `mapstructure:"enable_task_queue"`
	LeaderTtl             time.Duration `mapstructure:"leader_ttl"`
	LeaderRenew           time.Duration `mapstructure:"leader_renew"`
	LockerExpiry          time.Duration `mapstructure:"locker_expiry"`
	DefaultTimeout        time.Duration `mapstructure:"default_timeout"`
	BatchSize             int           `mapstructure:"batch_size"`  // 批量查询任务数量限制
	MaxWorkers            int           `mapstructure:"max_workers"` // 最大并发工作协程数
	NodeId                string        `mapstructure:"-"`           // 节点ID，仅用于测试手动指定，配置文件中无需配置
}

var globalConfig *Config

func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// 转换时间单位
	cfg.Scheduler.SchedulerLoopInterval *= time.Second
	cfg.Scheduler.LeaderTtl *= time.Second
	cfg.Scheduler.LeaderRenew *= time.Second
	cfg.Scheduler.LockerExpiry *= time.Second
	cfg.Scheduler.DefaultTimeout *= time.Second

	// 设置默认值
	if cfg.Scheduler.BatchSize <= 0 {
		cfg.Scheduler.BatchSize = 100
	}
	if cfg.Scheduler.MaxWorkers <= 0 {
		cfg.Scheduler.MaxWorkers = 50
	}

	// 自动生成 NodeID
	if cfg.Server.NodeId == "" {
		hostname, _ := os.Hostname()
		cfg.Server.NodeId = hostname + "-" + uuid.New().String()[:8]
	}

	globalConfig = &cfg
	return &cfg, nil
}

func Get() *Config {
	return globalConfig
}

// SetConfig sets the global config (used for testing)
func SetConfig(cfg *Config) {
	globalConfig = cfg
}
