package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	clickhouse2 "github.com/PavelAgarkov/service-pkg/database/clickhouse"
	"github.com/mitchellh/mapstructure"
)

const (
	envJSONLocal       = "APP_CONFIG_JSON_LOCAL"
	envJSONEl          = "APP_CONFIG_JSON_EL"
	envJSONXc          = "APP_CONFIG_JSON_XC"
	appConfigPathLocal = "APP_CONFIG_PATH_LOCAL"
	envEnv             = "APP_ENV"
)

type Config struct {
	BadgerDBMaster        BadgerDBMaster         `mapstructure:"badger_db_master" envconfig:"BADGER_DB_MASTER"`
	Application           ApplicationConfig      `mapstructure:"application"    envconfig:"APPLICATION"`
	Server                Server                 `mapstructure:"server"         envconfig:"SERVER"`
	PostgresMaster        Postgres               `mapstructure:"k8s_haproxy_pgsql_master" envconfig:"POSTGRES_MASTER"`
	PostgresAsyncReplicas Postgres               `mapstructure:"k8s_haproxy_pgsql_replicaasync" envconfig:"POSTGRES_MASTER"`
	PostgresSyncReplicas  Postgres               `mapstructure:"k8s_haproxy_pgsql_replicasync" envconfig:"POSTGRES_MASTER"`
	Clickhouse            clickhouse2.Clickhouse `mapstructure:"clickhouse"     envconfig:"CLICKHOUSE"`
	Redis                 RedisConfig            `mapstructure:"redis"          envconfig:"REDIS"`
	SimpleServer          SimpleServer           `mapstructure:"simple_server"  envconfig:"SIMPLE_SERVER"`
}

type BadgerDBMaster struct {
	InMemory             bool          `mapstructure:"in_memory"                envconfig:"IN_MEMORY"`
	RamLimitMemory       int64         `mapstructure:"ram_limit_memory"         envconfig:"RAM_LIMIT_MEMORY"`
	ReadOnly             bool          `mapstructure:"read_only"                envconfig:"READ_ONLY"`
	WithMetrics          bool          `mapstructure:"with_metrics"             envconfig:"WITH_METRICS"`
	GCInterval           time.Duration `mapstructure:"gc_interval"              envconfig:"GC_INTERVAL"`
	NumGoroutines        int           `mapstructure:"num_goroutines"           envconfig:"NUM_GOROUTINES"`
	ValueThreshold       int64         `mapstructure:"value_thresh_hold"          envconfig:"VALUE_THRESH_HOLD"`
	BaseTableSize        int64         `mapstructure:"base_table_size"         envconfig:"BASE_TABLE_SIZE"`
	NumCompactors        int           `mapstructure:"num_compactors"          envconfig:"NUM_COMPACTORS"`
	ZstdCompressionLevel int           `mapstructure:"zstd_compression_level"  envconfig:"ZSTD_COMPRESSION_LEVEL"`
	DetectConflicts      bool          `mapstructure:"detect_conflicts"        envconfig:"DETECT_CONFLICTS"`
	Encoder              string        `mapstructure:"encoder"                   envconfig:"ENCODER"`
	LoggingLevel         string        `mapstructure:"logging_level"          envconfig:"LOGGING_LEVEL"`
}

type ApplicationConfig struct {
	HeapOverflow             int           `mapstructure:"heap_overflow" envconfig:"HEAP_OVERFLOW"`
	Cores                    int           `mapstructure:"cores"                      envconfig:"CORES"`
	ClickhouseRate           int           `mapstructure:"clickhouse_out_rate"       envconfig:"CLICKHOUSE_OUT_RATE"`
	CommandBusRate           int           `mapstructure:"command_bus_rate"        envconfig:"COMMAND_BUS_RATE"`
	ExpiredNomenclatureCache time.Duration `mapstructure:"expired_nomenclature_cache" envconfig:"EXPIRED_NOMENCLATURE_CACHE"`
	Core                     Core          `mapstructure:"core"                    envconfig:"CORE"`
}

type Core struct {
	RemoveUnupdatedNomenclatureCacheDuration       time.Duration `mapstructure:"remove_unupdated_nomenclature_cache_duration" envconfig:"REMOVE_UNUPDATED_NOMENCLATURE_CACHE_DURATION"`
	RemoveUnupdatedNomenclatureCacheInterval       string        `mapstructure:"remove_unupdated_nomenclature_cache_interval"  envconfig:"REMOVE_UNUPDATED_NOMENCLATURE_CACHE_INTERVAL"`
	TTLCacheRecompilerInterval                     string        `mapstructure:"ttl_cache_recompiler_interval"                     envconfig:"TTL_CACHE_RECOMPILER_INTERVAL"`
	ScheduleNmTasksInterval                        time.Duration `mapstructure:"schedule_nm_tasks_interval"                        envconfig:"SCHEDULE_NM_TASKS_INTERVAL"`
	ScheduleCommandsBusInterval                    time.Duration `mapstructure:"schedule_commands_bus_interval"               envconfig:"SCHEDULE_COMMANDS_BUS_INTERVAL"`
	ScheduleDeleteUnusedNomenclatureFilterInterval string        `mapstructure:"schedule_delete_unused_nomenclature_filter_interval" envconfig:"SCHEDULE_DELETE_UNUSED_NOMENCLATURE_FILTER_INTERVAL"`
	AllowedTimeToClickhouseQuery                   time.Duration `mapstructure:"allowed_time_to_clickhouse_query"                  envconfig:"ALLOWED_TIME_TO_CLICKHOUSE_QUERY"`
}

type SimpleServer struct {
	Addr string `mapstructure:"addr"               envconfig:"ADDR"`
}

type Server struct {
	Addr              string        `mapstructure:"addr"               envconfig:"ADDR"`
	Network           string        `mapstructure:"network"            envconfig:"NETWORK"`
	ReflectionEnabled bool          `mapstructure:"reflection_enabled" envconfig:"REFLECTION_ENABLED"`
	OutGRPCBodySize   int           `mapstructure:"out_grpc_body_size" envconfig:"OUT_GRPC_BODY_SIZE"`
	InGRPCBodySize    int           `mapstructure:"in_grpc_body_size"  envconfig:"IN_GRPC_BODY_SIZE"`
	TimeOut           time.Duration `mapstructure:"time_out"           envconfig:"TIME_OUT"`
}

type Postgres struct {
	Host                  string        `mapstructure:"host"                    envconfig:"HOST"`
	Port                  string        `mapstructure:"port"                    envconfig:"PORT"`
	Username              string        `mapstructure:"username"                envconfig:"USERNAME"`
	Password              string        `mapstructure:"password"                envconfig:"PASSWORD"`
	Database              string        `mapstructure:"database"                envconfig:"DATABASE"`
	SSLMode               string        `mapstructure:"ssl_mode"                envconfig:"SSL_MODE"`
	MaxOpenedConnections  int           `mapstructure:"max_opened_connections"   envconfig:"MAX_OPENED_CONNECTIONS"`
	ConnectionMaxIdleTime time.Duration `mapstructure:"connection_max_idle_time" envconfig:"CONNECTION_MAX_IDLE_TIME"`
	ConnectionMaxLifeTime time.Duration `mapstructure:"connection_max_life_time" envconfig:"CONNECTION_MAX_LIFE_TIME"`
	ApplicationName       string        `mapstructure:"application_name"         envconfig:"APPLICATION_NAME"`
	HealthCheckPeriod     time.Duration `mapstructure:"health_check_period"     envconfig:"HEALTH_CHECK_PERIOD"`
	ConnectTimeout        time.Duration `mapstructure:"connect_timeout"         envconfig:"CONNECT_TIMEOUT"`
	MaxConnLifeTimeJitter time.Duration `mapstructure:"max_conn_life_time_jitter" envconfig:"MAX_CONN_LIFE_TIME_JITTER"`
}

//type Clickhouse struct {
//	Host            string        `mapstructure:"host"     envconfig:"HOST"`
//	Port            int           `mapstructure:"port"     envconfig:"PORT"`
//	Username        string        `mapstructure:"username" envconfig:"USERNAME"`
//	Password        string        `mapstructure:"password" envconfig:"PASSWORD"`
//	DialTimeout     time.Duration `mapstructure:"dial_timeout" envconfig:"DIAL_TIMEOUT"`
//	MaxOpenConn     int           `mapstructure:"max_open_conn" envconfig:"MAX_OPEN_CONN"`
//	MaxIdleConn     int           `mapstructure:"max_idle_conn" envconfig:"MAX_IDLE_CONN"`
//	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time" envconfig:"CONN_MAX_IDLE_TIME"`
//	ConnMaxLifeTime time.Duration `mapstructure:"conn_max_life_time" envconfig:"CONN_MAX_LIFE_TIME"`
//}

type RedisConfig struct {
	Address  string `mapstructure:"address"  envconfig:"ADDRESS"`
	Username string `mapstructure:"username" envconfig:"USERNAME"`
	Password string `mapstructure:"password" envconfig:"PASSWORD"`
	DB       int    `mapstructure:"db"       envconfig:"DB"`
}

type DBConfig struct {
	SchemaName   string        `json:"schemaname" mapstructure:"schemaname" envconfig:"SCHEMANAME"`
	Sources      string        `json:"sources" mapstructure:"sources" envconfig:"SOURCES"`
	MaxLifetime  time.Duration `json:"maxlifetime" mapstructure:"maxlifetime" envconfig:"MAX_LIFETIME"`
	MaxIdleConns int           `json:"maxidleconns" mapstructure:"maxidleconns" envconfig:"MAX_IDLE_CONNS"`
	MaxOpenConns int           `json:"maxopenconns" mapstructure:"maxopenconns" envconfig:"MAX_OPEN_CONNS"`
	MaxRetries   int           `json:"maxretries" mapstructure:"maxretries" envconfig:"MAX_RETRIES"`
	Duration     string        `json:"duration" mapstructure:"duration" envconfig:"DURATION"`
	EnableDelete bool          `json:"enabledelete" mapstructure:"enabledelete" envconfig:"ENABLE_DELETE"`
	Period       time.Duration `json:"period" mapstructure:"period" envconfig:"PERIOD"`
}

type CommonDownloaderConfig struct {
	Enabled              bool          `mapstructure:"enabled"                 envconfig:"ENABLED"`
	Debug                bool          `mapstructure:"debug"                   envconfig:"DEBUG"`
	SaveEmptyDownloadLog bool          `mapstructure:"save_empty_download_log" envconfig:"SAVE_EMPTY_DOWNLOAD_LOG"`
	Period               time.Duration `mapstructure:"period"                  envconfig:"PERIOD"`
	BatchSize            int64         `mapstructure:"batch_size"              envconfig:"BATCH_SIZE"`
	TTLInDays            int64         `mapstructure:"ttl_in_days"             envconfig:"TTL_IN_DAYS"`
	StartID              int64         `mapstructure:"start_id"                envconfig:"START_ID"`
	StartTime            string        `mapstructure:"start_time"              envconfig:"START_TIME"`
	OfficeIDs            []int64       `mapstructure:"office_ids"             envconfig:"OFFICE_IDS"`
	PlaceIDs             []int64       `mapstructure:"place_ids"             envconfig:"PLACE_IDS"`
	WhIDs                []int64       `mapstructure:"wh_ids"                envconfig:"WH_IDS"`
	Postgres             DBConfig      `mapstructure:"postgres"                envconfig:"POSTGRES"`
}

var dc = []string{
	envJSONEl,
	envJSONXc,
	envJSONLocal,
}

func Load() (*Config, error) {
	appEnv := os.Getenv(envEnv)

	if appEnv == "" || strings.ToLower(appEnv) == "dev" || strings.ToLower(appEnv) == "local" || strings.ToLower(appEnv) == "test" {
		if os.Getenv(envJSONLocal) == "" {
			path := os.Getenv(appConfigPathLocal)
			if path == "" {
				return nil, errors.New("APP_CONFIG_PATH must be set in dev/local/test mode")
			}

			bytes, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", path, err)
			}
			_ = os.Setenv(envJSONLocal, string(bytes))
		}
	}

	raw, ok := firstEnv(dc...)
	if !ok {
		return nil, fmt.Errorf("envs %s not found", strings.Join(dc, ", "))
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, fmt.Errorf("unmarshal JSON: %w", err)
	}

	cfg, err := decode(doc)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func firstEnv(keys ...string) (string, bool) {
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			return v, true
		}
	}
	return "", false
}

func decode(m map[string]any) (*Config, error) {
	dc := &mapstructure.DecoderConfig{
		Result:  &Config{},
		TagName: "mapstructure",
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			func(from, to reflect.Type, v interface{}) (interface{}, error) {
				if to == reflect.TypeOf(time.Duration(0)) && from.Kind() == reflect.String {
					return time.ParseDuration(v.(string))
				}
				return v, nil
			},
		),
	}

	dec, err := mapstructure.NewDecoder(dc)
	if err != nil {
		return nil, fmt.Errorf("build decoder: %w", err)
	}
	if err := dec.Decode(m); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	return dc.Result.(*Config), nil
}
