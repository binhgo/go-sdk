package sdk

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	mgo "github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// SortedConsumeFn ...
type SortedConsumeFn = func(*SortedQueueItem) error

// SortedLock ...
type SortedLock struct {
	SortedKey     string    `json:"sortedKey,omitempty" bson:"sorted_key,omitempty"`
	SortedIndexes *[]string `json:"sortedIndexes,omitempty" bson:"sorted_indexes,omitempty"`
	CurrentIndex  string    `json:"currentIndex,omitempty" bson:"current_index,omitempty"`
	ProcessBy     string    `json:"processBy,omitempty" bson:"process_by,omitempty"`
}

// DBSortedQueueChannel ...
type DBSortedQueueChannel struct {
	name        string
	item        chan *SortedQueueItem
	isActive    bool
	processing  bool
	consumerMap map[string]SortedConsumeFn
	queueDB     *DBModel2
	consumedDB  *DBModel2
	lock        *sync.Mutex
	config      *SortedQueueConfiguration
}

// SortedQueueItem ...
type SortedQueueItem struct {
	Data            interface{}    `json:"data,omitempty" bson:"data,omitempty"`
	ID              *bson.ObjectId `json:"id,omitempty" bson:"_id,omitempty"`
	Log             *[]string      `json:"log,omitempty" bson:"log,omitempty"`
	ProcessBy       string         `json:"processBy,omitempty" bson:"process_by,omitempty"`
	ConsumerVersion string         `json:"consumerVersion,omitempty" bson:"consumer_version,omitempty"`
	LastFail        *time.Time     `json:"lastFail,omitempty" bson:"last_fail,omitempty"`
	ProcessTimeMS   int            `json:"processTimeMS,omitempty" bson:"process_time_ms,omitempty"`
	CreatedTime     *time.Time     `json:"createdTime,omitempty" bson:"created_time,omitempty"`
	LastUpdatedTime *time.Time     `json:"lastUpdatedTime,omitempty" bson:"last_updated_time,omitempty"`
	Keys            *[]string      `json:"keys,omitempty" bson:"keys,omitempty"`
	SortedKey       string         `json:"sortedKey,omitempty" bson:"sorted_key,omitempty"`
	SortIndex       int64          `json:"sortIndex,omitempty" bson:"sort_index,omitempty"`
	Topic           string         `json:"topic,omitempty" bson:"topic,omitempty"`

	RepushCount int `json:"repushCount,omitempty" bson:"repush_count,omitempty"`
	FailCount   int `json:"failCount,omitempty" bson:"fail_count,omitempty"`
}

// SortedQueueConfiguration Configuration apply to sorted queue
type SortedQueueConfiguration struct {

	// Sleep time of selector when all queue items in DB are processed & processing. Default 500 ms, min 100ms, max 3000 ms.
	SelectorDelayMS int

	// Size of log array. Default 5, min 1, max 20.
	LogSize int

	// Time that queue channel wait to retry (recover from error) an item. Default 3 seconds, min 1 seconds, max 30 seconds.
	MaximumWaitToRetryS int

	// Number of SECONDS that queue used to clear processing item with older code version. Default 10 min, min 1 min, max 30 min.
	OldVersionTimeoutS int

	// Number of SECONDS that queue used to clear processing item with the same code version. Default 20 min, min 1 min, max 1 hour.
	CurVersionTimeoutS int

	// Number of channel that consume items. Default 50, min 1, max 100.
	ChannelCount int
}

func (c *DBSortedQueueChannel) start() {

	for true {

		// wait from the channel
		item := <-c.item

		// the more the item fail, it require wait more time
		// limit max = <size of log> second
		if item.LastFail != nil && time.Since(*item.LastFail).Seconds() < float64(c.config.MaximumWaitToRetryS) {
			if item.Log != nil {
				if len(*item.Log) < 3 {
					time.Sleep(time.Duration(200*len(*item.Log)) * time.Millisecond)
				} else {
					time.Sleep(time.Duration(len(*item.Log)) * time.Second)
				}
			} else {
				time.Sleep(time.Duration(1 * time.Second))
			}
		}

		// check if there are exist any other item with same sorted key
		validOrder := c.validateItemOrder(item)
		var err error

		if validOrder {
			start := time.Now()
			consumer := c.consumerMap[item.Topic]
			err = (error)(&Error{
				Type:    "INIT_MISSING",
				Message: "Consumer for " + item.Topic + " is not ready.",
			})

			if consumer != nil {
				err = consumer(item)
			}

			if err == nil { // if successfully
				c.queueDB.Delete(&QueueItem{
					ID: item.ID,
				})
				t := time.Since(start).Nanoseconds() / 1000000
				item.ProcessTimeMS = int(t)
				item.ID = nil
				c.consumedDB.Create(item)
			}
		}

		if !validOrder || err != nil { // if consuming function return errors
			if err == nil {
				err = (error)(&Error{
					Type:    "WRONG_ORDER",
					Message: "This item is consumed at wrong order.",
				})
			}

			errStr := c.name + " " + time.Now().Format("2006-01-02T15:04:05+0700") + " " + err.Error()
			l := 0
			if item.Log == nil {
				item.Log = &[]string{errStr}
			} else {
				l = len(*item.Log)
				if l > c.config.LogSize && l >= 5 { // crop if too long
					tmp := (*item.Log)[l-4:]
					item.Log = &tmp
				}
				log := append(*item.Log, errStr)
				item.Log = &log
			}

			if validOrder {
				item.FailCount += 1
			}

			// re-push to end of queue for too-many-fail item
			if l > c.config.LogSize {
				c.repushItem(item)
			} else {
				// update item
				now := time.Now()
				c.queueDB.UpdateOne(&SortedQueueItem{
					ID: item.ID,
				}, &SortedQueueItem{
					Log:       item.Log,
					ProcessBy: "NONE",
					LastFail:  &now,
					FailCount: item.FailCount,
				})
			}
		}
		c.processing = false
	}

}

func (c *DBSortedQueueChannel) repushItem(item *SortedQueueItem) {
	repushedItem := &SortedQueueItem{
		Data:        item.Data,
		Keys:        item.Keys,
		ProcessBy:   "NONE",
		SortedKey:   item.SortedKey,
		SortIndex:   item.SortIndex,
		Topic:       item.Topic,
		RepushCount: item.RepushCount + 1,
		Log:         item.Log,
		FailCount:   item.FailCount,
	}
	c.queueDB.Create(repushedItem)
	c.queueDB.Delete(&SortedQueueItem{
		ID: item.ID,
	})
}

func (c *DBSortedQueueChannel) putItem(item *SortedQueueItem) bool {
	c.item <- item
	return true
}

func (c *DBSortedQueueChannel) validateItemOrder(item *SortedQueueItem) bool {
	itemRs := c.queueDB.QueryS(&SortedQueueItem{
		SortedKey: item.SortedKey,
	}, 0, 1, "sort_index")

	firstItem := itemRs.Data.([]*SortedQueueItem)[0]
	if firstItem.SortIndex < item.SortIndex {
		return false
	}
	// if len(itemList) > 1 {
	//	for _, t := range itemList {
	//		if t.ID.Hex() != item.ID.Hex() && t.SortIndex < item.SortIndex {
	//			// fmt.Println("Found older " + strconv.FormatInt(t.SortIndex, 10) +
	//			// 	" " + strconv.FormatInt(item.SortIndex, 10) + " => " + t.ID.Hex() + " " + item.ID.Hex())
	//			return false
	//		}
	//	}
	// }
	return true
}

// DBSortedQueueConnector ...
type DBSortedQueueConnector struct {
	name            string
	channels        []*DBSortedQueueChannel
	queueDB         *DBModel2
	version         string
	acceptableTopic []string
	config          *SortedQueueConfiguration
}

func (dbc *DBSortedQueueConnector) pickFreeChannel(start int, limit int) int {
	var quota = limit
	var i = start
	for quota > 0 {
		if !dbc.channels[i].processing {
			dbc.channels[i].processing = true
			return i
		}
		i = (i + 1) % limit
		quota--
	}
	return -1
}

func (dbc *DBSortedQueueConnector) start() {
	var channelNum = len(dbc.channels)
	var counter = 0
	for true {

		// if OS notify sigterm -> stop processing new item
		if IsSigTerm {
			return
		}

		// pick channel
		picked := -1
		for picked < 0 {
			picked = dbc.pickFreeChannel(0, channelNum)
			if picked < 0 {
				// if all channels are busy
				time.Sleep(10 * time.Millisecond)
			}
		}

		// pick one item in queue
		var query *bson.M
		if dbc.acceptableTopic != nil && len(dbc.acceptableTopic) > 0 {
			query = &bson.M{
				"process_by": "NONE",
				"topic": &bson.M{
					"$in": dbc.acceptableTopic,
				},
			}
		} else {
			query = &bson.M{
				"process_by": "NONE",
			}
		}
		resp := dbc.queueDB.UpdateOne(query, &bson.M{
			"process_by":       dbc.name + " - " + dbc.channels[picked].name,
			"consumer_version": dbc.version,
		})

		if resp.Status == APIStatus.Ok {

			item := resp.Data.([]*SortedQueueItem)[0]
			dbc.channels[picked].putItem(item)

		} else {
			// if no item found
			dbc.channels[picked].processing = false
			time.Sleep(time.Duration(dbc.config.SelectorDelayMS) * time.Millisecond)
		}

		// check & clean old item
		counter++
		if counter > 500 {

			// clean old item with different version
			oldTime := time.Now().Add(-(time.Duration(dbc.config.OldVersionTimeoutS) * time.Second))
			dbc.queueDB.Update(&bson.M{
				"process_by": bson.M{
					"$ne": "NONE",
				},
				"consumer_version": bson.M{
					"$ne": dbc.version,
				},
				"last_updated_time": bson.M{
					"$lt": oldTime,
				},
			}, &bson.M{
				"process_by":       "NONE",
				"consumer_version": "NONE",
			})

			// all version
			oldTime = oldTime.Add(-(time.Duration(dbc.config.CurVersionTimeoutS) * time.Second))
			dbc.queueDB.Update(&bson.M{
				"process_by": bson.M{
					"$ne": "NONE",
				},
				"last_updated_time": bson.M{
					"$lt": oldTime,
				},
			}, &bson.M{
				"process_by":       "NONE",
				"consumer_version": "NONE",
			})

			counter = 0
		}

	}
}

// DBSortedQueue ...
type DBSortedQueue struct {
	ColName         string
	queueDB         *DBModel2
	consumedDB      *DBModel2
	ready           bool
	channels        []*DBSortedQueueChannel
	connector       *DBSortedQueueConnector
	hostname        string
	acceptableTopic []string
	config          *SortedQueueConfiguration

	lock          *sync.Mutex
	counter       int64
	lastCountTime int64

	consumerMap map[string]SortedConsumeFn
}

// Init ...
func (dbq *DBSortedQueue) Init(mSession *DBSession, dbName string) {

	// setup main queue
	dbq.InitWithConfig(mSession, dbName, &SortedQueueConfiguration{
		CurVersionTimeoutS:  20 * 60, // 20 min
		OldVersionTimeoutS:  10 * 60, // 10 min
		LogSize:             5,
		MaximumWaitToRetryS: 3,   // 3 secs
		SelectorDelayMS:     500, // 500ms
		ChannelCount:        50,
	})
}

// Init ...
func (dbq *DBSortedQueue) InitWithConfig(mSession *DBSession, dbName string, config *SortedQueueConfiguration) {
	if config == nil {
		panic(Error{Message: "SortedQueue require configuration when init"})
	}

	// setup default & limit value
	config.ChannelCount = NormalizeIntValue(config.ChannelCount, 1, 100)
	config.CurVersionTimeoutS = NormalizeIntValue(config.CurVersionTimeoutS, 60, 3600)
	config.OldVersionTimeoutS = NormalizeIntValue(config.OldVersionTimeoutS, 60, 1800)
	config.LogSize = NormalizeIntValue(config.LogSize, 1, 20)
	config.MaximumWaitToRetryS = NormalizeIntValue(config.MaximumWaitToRetryS, 1, 30)
	config.SelectorDelayMS = NormalizeIntValue(config.SelectorDelayMS, 100, 3000)

	dbq.config = config
	// setup main queue
	dbq.InitWithExpiredTime(mSession, dbName, time.Duration(7*24)*time.Hour)

}

// InitWithExpiredTime ...
func (dbq *DBSortedQueue) InitWithExpiredTime(mSession *DBSession, dbName string, expiredTime time.Duration) {

	// setup main queue
	dbq.queueDB = &DBModel2{
		ColName:        dbq.ColName,
		DBName:         dbName,
		TemplateObject: &SortedQueueItem{},
	}
	dbq.queueDB.Init(mSession)
	dbq.queueDB.CreateIndex(mgo.Index{
		Key:        []string{"process_by"},
		Background: true,
	})
	dbq.queueDB.CreateIndex(mgo.Index{
		Key:        []string{"keys"},
		Background: true,
	})
	dbq.queueDB.CreateIndex(mgo.Index{
		Key:        []string{"topic", "keys"},
		Background: true,
	})
	dbq.queueDB.CreateIndex(mgo.Index{
		Key:        []string{"topic", "process_by", "_id"},
		Background: true,
	})
	dbq.queueDB.CreateIndex(mgo.Index{
		Key:        []string{"sorted_key", "sort_index"},
		Background: true,
	})

	// setup consumed (for history)
	dbq.consumedDB = &DBModel2{
		ColName:        dbq.ColName + "_consumed",
		DBName:         dbName,
		TemplateObject: &SortedQueueItem{},
	}

	dbq.consumedDB.Init(mSession)
	dbq.consumedDB.CreateIndex(mgo.Index{
		Key:         []string{"last_updated_time"},
		Background:  true,
		ExpireAfter: expiredTime,
	})
	dbq.queueDB.CreateIndex(mgo.Index{
		Key:        []string{"topic", "keys"},
		Background: true,
	})

	dbq.ready = true
	dbq.lock = &sync.Mutex{}
	dbq.consumerMap = make(map[string]SortedConsumeFn)
}

func (dbq *DBSortedQueue) getCounter(time int64) int64 {
	dbq.lock.Lock()
	defer dbq.lock.Unlock()

	if time > dbq.lastCountTime {
		dbq.lastCountTime = time
		dbq.counter = 1
	} else {
		dbq.counter++
	}

	return time + dbq.counter
}

// SetConsumer ...
func (dbq *DBSortedQueue) SetConsumer(consumer SortedConsumeFn) {
	if dbq.acceptableTopic == nil {
		dbq.acceptableTopic = []string{"default"}
	} else {
		dbq.acceptableTopic = append(dbq.acceptableTopic, "default")
	}
	dbq.SetTopicConsumer("default", consumer)
}

// SetTopicConsumer ...
func (dbq *DBSortedQueue) SetTopicConsumer(topic string, consumer SortedConsumeFn) {
	if dbq.acceptableTopic == nil {
		dbq.acceptableTopic = []string{topic}
	} else {
		dbq.acceptableTopic = append(dbq.acceptableTopic, topic)
	}
	dbq.consumerMap[topic] = consumer
}

func (dbq *DBSortedQueue) StartConsume() {
	if dbq.ready == false {
		panic(Error{Type: "NOT_INITED", Message: "Require to init db before using queue."})
	}

	dbq.channels = []*DBSortedQueueChannel{}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "undefined"
	}
	dbq.hostname = hostname
	for i := 0; i < dbq.config.ChannelCount; i++ {
		c := &DBSortedQueueChannel{
			name:        hostname + "/" + strconv.Itoa(i+1),
			consumerMap: dbq.consumerMap,
			isActive:    true,
			processing:  false,
			queueDB:     dbq.queueDB,
			consumedDB:  dbq.consumedDB,
			lock:        &sync.Mutex{},
			item:        make(chan *SortedQueueItem),
			config:      dbq.config,
		}
		dbq.channels = append(dbq.channels, c)
		go c.start()
	}
	go dbq.startConnectors()

}

// startConnector start the job that query item from DB and deliver to channel
func (dbq *DBSortedQueue) startConnectors() {
	// wait some time for all channels inited
	time.Sleep(1 * time.Second)

	version := os.Getenv("version")
	if version == "" {
		version = strconv.Itoa((1000000 + rand.Int()) % 999999)
	}

	dbq.connector = &DBSortedQueueConnector{
		name:            dbq.hostname + "/connector",
		queueDB:         dbq.queueDB,
		channels:        dbq.channels,
		version:         version,
		acceptableTopic: dbq.acceptableTopic,
		config:          dbq.config,
	}

	go dbq.connector.start()
}

// Push put new item into queue
func (dbq *DBSortedQueue) Push(data interface{}, sortedKey string) error {
	return dbq.PushWithKeys(data, sortedKey, nil)
}

// PushWithKeys Mark item with key
func (dbq *DBSortedQueue) PushWithKeys(data interface{}, sortedKey string, keys *[]string) error {
	return dbq.PushWithKeysAndTopic(data, sortedKey, keys, "default")
}

// PushWithKeysAndTopic Mark item with key
func (dbq *DBSortedQueue) PushWithKeysAndTopic(data interface{}, sortedKey string, keys *[]string, topic string) error {
	if dbq.ready == false {
		panic(Error{Type: "NOT_INITED", Message: "Require to init db before using queue."})
	}

	var time = time.Now().UnixNano()
	var item = SortedQueueItem{
		Data:      data,
		Keys:      keys,
		ProcessBy: "NONE",
		SortedKey: sortedKey,
		SortIndex: dbq.getCounter(time),
		Topic:     topic,
	}

	resp := dbq.queueDB.Create(&item)
	if resp.Status == APIStatus.Ok {
		return nil
	}

	return &Error{Type: resp.ErrorCode, Message: resp.Message}
}
