package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/globalsign/mgo/bson"

	"github.com/binhgo/go-sdk/sdk"
	"github.com/binhgo/go-sdk/sdk/job"
)

type TestObj struct {
	ID    *bson.ObjectId `bson:"_id,omitempty"`
	Value int            `bson:"int"`
}

func main() {

	// var consumerA = func(item *sdk.SortedQueueItem) error {
	// 	fmt.Println("Consumer A - " + strconv.FormatInt(item.SortIndex%10000000000+10000000000, 10) + " " + item.Data.(string))
	// 	return nil
	// }
	// var consumerB = func(item *sdk.SortedQueueItem) error {
	// 	fmt.Println("Consumer B - " + strconv.FormatInt(item.SortIndex%10000000000+10000000000, 10) + " " + item.Data.(string))
	// 	return nil
	// }

	var executor = &job.Executor{
		ColName: "announcement_job",
	}

	// var fixDB = sdk.DBModel2{
	//	ColName:        "announcement_job",
	//	DBName:         "internal-tools_stg_css_queue",
	//	TemplateObject: &TestObj{},
	// }

	var app = sdk.NewApp("Test App")

	// setup db client
	var db = app.SetupDBClient(sdk.DBConfiguration{
		Address:  []string{"35.247.133.237:27017"},
		Username: "admin",
		Password: "",
		AuthDB:   "admin",
	})

	db.OnConnected(func(session *sdk.DBSession) error {
		executor.InitWithConfig(session, "internal-tools_stg_css_queue", &job.ExecutorConfiguration{
			ParallelTopicProcessing: false,
			FailThreshold:           50,
			ChannelCount:            5,
			SortedItem:              false,
			LogSize:                 5,
			SelectorDelayMS:         100,
			WaitForReadyTime:        true,
			UniqueItem:              false,
			ConsumedExipredTime:     time.Duration(7) * time.Hour,
			CurVersionTimeoutS:      30,
			OldVersionTimeoutS:      30,
			MaximumWaitToRetryS:     15,
		})
		// fixDB.Init(session)
		return nil
	})

	// var pushQueue = func(topic string, data string, sk string) {
	// 	queue.PushWithKeysAndTopic(data, "X"+sk, nil, topic)
	// }

	app.OnAllDBConnected(func() {
		executor.SetTopicConsumer("announcement", func(item *job.JobItem) error {
			str, _ := json.Marshal(item)
			fmt.Println(string(str))
			return nil
		})
		// for i := 0; i < 30; i++ {
		//	executor.Push("data "+strconv.Itoa(i), &job.JobItemMetadata{
		//		Topic:     "AAA",
		//		SortedKey: strconv.Itoa(i % 2),
		//	})
		// }
		//
		// for i := 0; i < 30; i++ {
		//	executor.Push("data "+strconv.Itoa(i), &job.JobItemMetadata{
		//		Topic:     "BBB",
		//		SortedKey: strconv.Itoa(10 + i%2),
		//	})
		// }
		//
		executor.StartConsume()

	})

	var server, _ = app.SetupAPIServer("HTTP")
	server.Expose(80)

	app.Launch()
}
