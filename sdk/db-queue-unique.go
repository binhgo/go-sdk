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

// UniqueConsumeFn ...
type UniqueConsumeFn = func(*UniqueQueueItem) error

// UniqueLock ...
type UniqueLock struct {
	UniqueKey     string    `json:"sortedKey,omitempty" bson:"sorted_key,omitempty"`
	UniqueIndexes *[]string `json:"sortedIndexes,omitempty" bson:"sorted_indexes,omitempty"`
	CurrentIndex  string    `json:"currentIndex,omitempty" bson:"current_index,omitempty"`
	ProcessBy     string    `json:"processBy,omitempty" bson:"process_by,omitempty"`
}

// DBUniqueQueueChannel ...
type DBUniqueQueueChannel struct {
	name       string
	item       chan *UniqueQueueItem
	isActive   bool
	processing bool
	consumer   UniqueConsumeFn
	queueDB    *DBModel2
	consumedDB *DBModel2
	lock       *sync.Mutex
	config     *UniqueQueueConfiguration
}

// UniqueQueueItem ...
type UniqueQueueItem struct {
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
	UniqueKey       string         `json:"uniqueKey,omitempty" bson:"unique_key,omitempty"`
	Topic           string         `json:"topic,omitempty" bson:"topic,omitempty"`
	RepushCount     int            `json:"repushCount,omitempty" bson:"repush_count,omitempty"`
}

// UniqueQueueConfiguration Configuration apply to sorted queue
type UniqueQueueConfiguration struct {

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

func (c *DBUniqueQueueChannel) start() {

	for true {

		// wait from the channel
		item := <-c.item

		// the more the item fail, it require wait more time
		// limit = size of log
		if item.LastFail != nil && time.Since(*item.LastFail).Seconds() < float64(c.config.MaximumWaitToRetryS) {
			time.Sleep(time.Duration(len(*item.Log)) * time.Second)
		}

		start := time.Now()
		consumer := c.consumer
		err := (error)(&Error{
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
		} else { // if consuming function return errors
			errStr := c.name + " " + time.Now().Format("2006-01-02T15:04:05+0700") + " " + err.Error()
			l := 0
			if item.Log == nil {
				item.Log = &[]string{errStr}
			} else {
				l = len(*item.Log)
				if l > c.config.LogSize { // crop if too long
					tmp := (*item.Log)[l-4:]
					item.Log = &tmp
				}
				log := append(*item.Log, errStr)
				item.Log = &log

			}

			// re-push to end of queue for too-many-fail item
			if l > c.config.LogSize {
				c.repushItem(item)
			} else {
				// update item
				now := time.Now()
				c.queueDB.UpdateOne(&QueueItem{
					ID: item.ID,
				}, &QueueItem{
					Log:       item.Log,
					ProcessBy: "NONE",
					LastFail:  &now,
				})
			}
		}
		c.processing = false
	}
}

func (c *DBUniqueQueueChannel) repushItem(item *UniqueQueueItem) {
	repushedItem := &UniqueQueueItem{
		Data:        item.Data,
		Keys:        item.Keys,
		ProcessBy:   "NONE",
		UniqueKey:   item.UniqueKey,
		Topic:       item.Topic,
		RepushCount: item.RepushCount + 1,
		Log:         item.Log,
	}
	result := c.queueDB.Create(repushedItem)
	if result.Status == APIStatus.Ok {
		c.queueDB.Delete(&UniqueQueueItem{
			ID: item.ID,
		})
	}
}

func (c *DBUniqueQueueChannel) putItem(item *UniqueQueueItem) bool {
	c.item <- item
	return true
}

// DBUniqueQueueConnector ...
type DBUniqueQueueConnector struct {
	name            string
	channels        []*DBUniqueQueueChannel
	queueDB         *DBModel2
	version         string
	acceptableTopic []string
	config          *UniqueQueueConfiguration
}

func (dbc *DBUniqueQueueConnector) pickFreeChannel(start int, limit int) int {
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

var uniqueDefaultQuery = &UniqueQueueItem{
	ProcessBy: "NONE",
}

func (dbc *DBUniqueQueueConnector) start() {
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
		resp := dbc.queueDB.UpdateOne(uniqueDefaultQuery, &UniqueQueueItem{
			ProcessBy:       dbc.name + " - " + dbc.channels[picked].name,
			ConsumerVersion: dbc.version,
		})

		if resp.Status == APIStatus.Ok {

			item := resp.Data.([]*UniqueQueueItem)[0]
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

// DBUniqueQueue ...
type DBUniqueQueue struct {
	ColName    string
	queueDB    *DBModel2
	consumedDB *DBModel2
	ready      bool
	channels   []*DBUniqueQueueChannel
	connector  *DBUniqueQueueConnector
	hostname   string
	config     *UniqueQueueConfiguration

	lock          *sync.Mutex
	counter       int64
	lastCountTime int64

	consumer UniqueConsumeFn
}

// Init ...
func (dbq *DBUniqueQueue) Init(mSession *DBSession, dbName string) {

	// setup main queue
	dbq.InitWithConfig(mSession, dbName, &UniqueQueueConfiguration{
		CurVersionTimeoutS:  20 * 60, // 20 min
		OldVersionTimeoutS:  10 * 60, // 10 min
		LogSize:             5,
		MaximumWaitToRetryS: 3,   // 3 secs
		SelectorDelayMS:     500, // 500ms
		ChannelCount:        50,
	})
}

// Init ...
func (dbq *DBUniqueQueue) InitWithConfig(mSession *DBSession, dbName string, config *UniqueQueueConfiguration) {
	if config == nil {
		panic(Error{Message: "UniqueQueue require configuration when init"})
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
func (dbq *DBUniqueQueue) InitWithExpiredTime(mSession *DBSession, dbName string, expiredTime time.Duration) {

	// setup main queue
	dbq.queueDB = &DBModel2{
		ColName:        dbq.ColName,
		DBName:         dbName,
		TemplateObject: &UniqueQueueItem{},
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
		Key:        []string{"unique_key"},
		Background: true,
		Unique:     true,
	})

	// setup consumed (for history)
	dbq.consumedDB = &DBModel2{
		ColName:        dbq.ColName + "_consumed",
		DBName:         dbName,
		TemplateObject: &UniqueQueueItem{},
	}

	dbq.consumedDB.Init(mSession)
	dbq.consumedDB.CreateIndex(mgo.Index{
		Key:         []string{"last_updated_time"},
		Background:  true,
		ExpireAfter: expiredTime,
	})
	dbq.queueDB.CreateIndex(mgo.Index{
		Key:        []string{"keys"},
		Background: true,
	})
	dbq.queueDB.CreateIndex(mgo.Index{
		Key:        []string{"unique_key"},
		Background: true,
	})

	dbq.ready = true
	dbq.lock = &sync.Mutex{}
}

func (dbq *DBUniqueQueue) getCounter(time int64) int64 {
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
func (dbq *DBUniqueQueue) SetConsumer(consumer UniqueConsumeFn) {
	dbq.consumer = consumer
}

func (dbq *DBUniqueQueue) StartConsume() {
	if dbq.ready == false {
		panic(Error{Type: "NOT_INITED", Message: "Require to init db before using queue."})
	}

	dbq.channels = []*DBUniqueQueueChannel{}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "undefined"
	}
	dbq.hostname = hostname
	for i := 0; i < dbq.config.ChannelCount; i++ {
		c := &DBUniqueQueueChannel{
			name:       hostname + "/" + strconv.Itoa(i+1),
			consumer:   dbq.consumer,
			isActive:   true,
			processing: false,
			queueDB:    dbq.queueDB,
			consumedDB: dbq.consumedDB,
			lock:       &sync.Mutex{},
			item:       make(chan *UniqueQueueItem),
			config:     dbq.config,
		}
		dbq.channels = append(dbq.channels, c)
		go c.start()
	}
	go dbq.startConnectors()

}

// startConnector start the job that query item from DB and deliver to channel
func (dbq *DBUniqueQueue) startConnectors() {
	// wait some time for all channels inited
	time.Sleep(1 * time.Second)

	version := os.Getenv("version")
	if version == "" {
		version = strconv.Itoa((1000000 + rand.Int()) % 999999)
	}

	dbq.connector = &DBUniqueQueueConnector{
		name:     dbq.hostname + "/connector",
		queueDB:  dbq.queueDB,
		channels: dbq.channels,
		version:  version,
		config:   dbq.config,
	}

	go dbq.connector.start()
}

// Push put new item into queue
func (dbq *DBUniqueQueue) Push(data interface{}, uniqueKey string) error {
	return dbq.PushWithKeys(data, uniqueKey, nil)
}

// PushWithKeys Mark item with key
func (dbq *DBUniqueQueue) PushWithKeys(data interface{}, uniqueKey string, keys *[]string) error {
	if dbq.ready == false {
		panic(Error{Type: "NOT_INITED", Message: "Require to init db before using queue."})
	}

	var item = UniqueQueueItem{
		Data:      data,
		Keys:      keys,
		ProcessBy: "NONE",
		UniqueKey: uniqueKey,
	}

	resp := dbq.queueDB.Create(&item)
	if resp.Status == APIStatus.Ok {
		return nil
	}

	return &Error{Type: resp.ErrorCode, Message: resp.Message}
}
