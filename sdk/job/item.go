package job

import (
	"github.com/globalsign/mgo/bson"
	"time"
)

// JobItem ...
type JobItem struct {
	ID              *bson.ObjectId `json:"id,omitempty" bson:"_id,omitempty"`
	CreatedTime     *time.Time     `json:"createdTime,omitempty" bson:"created_time,omitempty"`
	LastUpdatedTime *time.Time     `json:"lastUpdatedTime,omitempty" bson:"last_updated_time,omitempty"`
	Keys            *[]string      `json:"keys,omitempty" bson:"keys,omitempty"`

	Data            interface{} `json:"data,omitempty" bson:"data,omitempty"`
	ProcessBy       string      `json:"processBy,omitempty" bson:"process_by,omitempty"`
	ConsumerVersion string      `json:"consumerVersion,omitempty" bson:"consumer_version,omitempty"`
	LastFail        *time.Time  `json:"lastFail,omitempty" bson:"last_fail,omitempty"`

	Log           *[]string `json:"log,omitempty" bson:"log,omitempty"`
	ProcessTimeMS int       `json:"processTimeMS,omitempty" bson:"process_time_ms,omitempty"`

	SortedKey string     `json:"sortedKey,omitempty" bson:"sorted_key,omitempty"`
	SortIndex int64      `json:"sortIndex,omitempty" bson:"sort_index,omitempty"`
	Topic     string     `json:"topic,omitempty" bson:"topic,omitempty"`
	UniqueKey string     `json:"uniqueKey,omitempty" bson:"unique_key,omitempty"`
	ReadyTime *time.Time `json:"readyTime,omitempty" bson:"ready_time,omitempty"`

	RepushCount int `json:"repushCount,omitempty" bson:"repush_count,omitempty"`
	FailCount   int `json:"failCount,omitempty" bson:"fail_count,omitempty"`
}

type JobItemMetadata struct {
	UniqueKey string
	SortedKey string
	Keys      []string
	Topic     string
	ReadyTime *time.Time
}
