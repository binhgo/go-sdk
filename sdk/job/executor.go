package job

import (
	"github.com/globalsign/mgo"
	"gitlab.ghn.vn/common-projects/go-sdk/sdk"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// ExecutionFn ...
type ExecutionFn = func(*JobItem) error

// ExecutorConfiguration Configuration apply to job executor
type ExecutorConfiguration struct {

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

	// Time to remove consumed data
	ConsumedExipredTime time.Duration

	// Apply unique mechanism
	UniqueItem bool

	// Apply sorted mechanism
	SortedItem bool

	// Apply delay mechanism
	WaitForReadyTime bool

	// Process topics parallel
	ParallelTopicProcessing bool

	// After a number of fail, item will be re-push to the end of queue
	FailThreshold int
}

// Executor ...
type Executor struct {
	ColName         string
	jobDB           *sdk.DBModel2
	consumedDB      *sdk.DBModel2
	ready           bool
	channels        []*ExecutionChannel
	connector       *ExecutionSelector
	hostname        string
	acceptableTopic []string
	config          *ExecutorConfiguration

	lock          *sync.Mutex
	counter       int64
	lastCountTime int64

	consumerMap map[string]ExecutionFn
}

// Init ...
func (je *Executor) Init(mSession *sdk.DBSession, dbName string) {

	// setup main queue
	je.InitWithConfig(mSession, dbName, &ExecutorConfiguration{
		CurVersionTimeoutS:  20 * 60, // 20 min
		OldVersionTimeoutS:  10 * 60, // 10 min
		LogSize:             5,
		MaximumWaitToRetryS: 3,   // 3 secs
		SelectorDelayMS:     500, // 500ms
		ChannelCount:        50,
		ConsumedExipredTime: time.Duration(7*24) * time.Hour,
	})
}

// Init ...
func (je *Executor) InitWithConfig(mSession *sdk.DBSession, dbName string, config *ExecutorConfiguration) {
	if config == nil {
		panic(sdk.Error{Message: "SortedQueue require configuration when init"})
	}

	// setup default & limit value
	config.ChannelCount = sdk.NormalizeIntValue(config.ChannelCount, 1, 100)
	config.CurVersionTimeoutS = sdk.NormalizeIntValue(config.CurVersionTimeoutS, 60, 3600)
	config.OldVersionTimeoutS = sdk.NormalizeIntValue(config.OldVersionTimeoutS, 60, 1800)
	config.LogSize = sdk.NormalizeIntValue(config.LogSize, 1, 20)
	config.MaximumWaitToRetryS = sdk.NormalizeIntValue(config.MaximumWaitToRetryS, 1, 30)
	config.SelectorDelayMS = sdk.NormalizeIntValue(config.SelectorDelayMS, 100, 3000)
	if config.ConsumedExipredTime == 0 {
		config.ConsumedExipredTime = time.Duration(7*24) * time.Hour
	}

	je.config = config

	// setup main queue
	je.jobDB = &sdk.DBModel2{
		ColName:        je.ColName,
		DBName:         dbName,
		TemplateObject: &JobItem{},
	}
	je.jobDB.Init(mSession)
	je.jobDB.CreateIndex(mgo.Index{
		Key:        []string{"keys", "topic"},
		Background: true,
	})
	je.jobDB.CreateIndex(mgo.Index{
		Key:        []string{"topic", "process_by", "ready_time", "sorted_key", "sort_index"},
		Background: true,
	})
	je.jobDB.CreateIndex(mgo.Index{
		Key:        []string{"topic", "process_by", "sorted_key", "sort_index"},
		Background: true,
	})
	je.jobDB.CreateIndex(mgo.Index{
		Key:        []string{"process_by", "sorted_key", "sort_index"},
		Background: true,
	})
	je.jobDB.CreateIndex(mgo.Index{
		Key:        []string{"sorted_key", "sort_index"},
		Background: true,
	})
	je.jobDB.CreateIndex(mgo.Index{
		Key:        []string{"unique_key"},
		Background: true,
		Unique:     true,
	})

	// setup consumed (for history)
	je.consumedDB = &sdk.DBModel2{
		ColName:        je.ColName + "_consumed",
		DBName:         dbName,
		TemplateObject: &JobItem{},
	}

	je.consumedDB.Init(mSession)
	je.consumedDB.CreateIndex(mgo.Index{
		Key:         []string{"last_updated_time"},
		Background:  true,
		ExpireAfter: config.ConsumedExipredTime,
	})
	je.consumedDB.CreateIndex(mgo.Index{
		Key:        []string{"topic", "keys"},
		Background: true,
	})

	je.ready = true
	je.lock = &sync.Mutex{}
	je.consumerMap = make(map[string]ExecutionFn)
}

func (je *Executor) getCounter(time int64) string {
	je.lock.Lock()
	defer je.lock.Unlock()

	if time > je.lastCountTime {
		je.lastCountTime = time
		je.counter = 1
	} else {
		je.counter++
	}

	return strconv.FormatInt(time, 10) + strconv.FormatInt(je.counter, 10)
}

// SetConsumer ...
func (je *Executor) SetConsumer(consumer ExecutionFn) {
	je.SetTopicConsumer("default", consumer)
}

// SetTopicConsumer ...
func (je *Executor) SetTopicConsumer(topic string, consumer ExecutionFn) {
	if je.acceptableTopic == nil {
		je.acceptableTopic = []string{topic}
	} else {
		je.acceptableTopic = append(je.acceptableTopic, topic)
	}
	je.consumerMap[topic] = consumer
	if je.config.ParallelTopicProcessing && topic != "default" {
		je.consumerMap["default"] = consumer
	}
}

func (je *Executor) StartConsume() {
	if je.ready == false {
		panic(sdk.Error{Type: "NOT_INITED", Message: "Require to init db before using queue."})
	}

	je.channels = []*ExecutionChannel{}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "undefined"
	}
	je.hostname = hostname
	for i := 0; i < je.config.ChannelCount; i++ {
		c := &ExecutionChannel{
			name:        hostname + "/" + strconv.Itoa(i+1),
			consumerMap: je.consumerMap,
			isActive:    true,
			processing:  false,
			queueDB:     je.jobDB,
			consumedDB:  je.consumedDB,
			lock:        &sync.Mutex{},
			config:      je.config,
		}
		je.channels = append(je.channels, c)
		go c.start()
	}
	go je.startSelector()

}

// startConnector start the job that query item from DB and deliver to channel
func (je *Executor) startSelector() {
	// wait some time for all channels inited
	time.Sleep(1 * time.Second)

	version := os.Getenv("version")
	if version == "" {
		version = strconv.Itoa(rand.Intn(1000000))
	}

	je.connector = &ExecutionSelector{
		name:            je.hostname + "/selector",
		queueDB:         je.jobDB,
		channels:        je.channels,
		version:         version,
		acceptableTopic: je.acceptableTopic,
		config:          je.config,
	}

	go je.connector.start()
}

// Push put new item into queue
func (je *Executor) Push(data interface{}, metadata *JobItemMetadata) error {
	if je.ready == false {
		panic(sdk.Error{Type: "NOT_INITED", Message: "Require to init db before using queue."})
	}

	var time = time.Now().UnixNano()
	var item = JobItem{
		Data:      data,
		ProcessBy: "NONE",
		SortedKey: metadata.SortedKey,
		Topic:     metadata.Topic,
		UniqueKey: metadata.UniqueKey,
		ReadyTime: metadata.ReadyTime,
	}

	if metadata.Keys != nil {
		item.Keys = &metadata.Keys
	}

	if je.config.SortedItem || !je.config.UniqueItem {
		if je.config.SortedItem {
			item.SortIndex = time
		}

		if !je.config.UniqueItem {
			item.UniqueKey = je.getCounter(time)
		}
	}

	resp := je.jobDB.Create(&item)
	if resp.Status == sdk.APIStatus.Ok {
		return nil
	}

	return &sdk.Error{Type: resp.ErrorCode, Message: resp.Message}
}

func (je *Executor) GetJobDB() *sdk.DBModel2 {
	return je.jobDB
}

func (je *Executor) GetConsumedJobDB() *sdk.DBModel2 {
	return je.consumedDB
}
