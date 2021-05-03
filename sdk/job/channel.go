package job

import (
	"gitlab.ghn.vn/common-projects/go-sdk/sdk"
	"sync"
	"time"
)

// ExecutionChannel ...
type ExecutionChannel struct {
	name        string
	item        *JobItem
	isActive    bool
	processing  bool
	consumerMap map[string]ExecutionFn
	queueDB     *sdk.DBModel2
	consumedDB  *sdk.DBModel2
	lock        *sync.Mutex
	config      *ExecutorConfiguration
}

func (c *ExecutionChannel) start() {

	for true {

		// wait from the channel
		item := c.item
		for item == nil {
			time.Sleep(20 * time.Millisecond)
			item = c.item
		}

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
				time.Sleep(1 * time.Second)
			}
		}

		// check if there are exist any other item with same sorted key
		validOrder := c.validateItemOrder(item)
		var err error

		if validOrder {
			start := time.Now()
			consumer := c.consumerMap[item.Topic]
			if c.config.ParallelTopicProcessing {
				consumer = c.consumerMap["default"]
			}
			err = (error)(&sdk.Error{
				Type:    "INIT_MISSING",
				Message: "Consumer for " + item.Topic + " is not ready.",
			})

			if consumer != nil {
				err = consumer(item)
			}

			if err == nil { // if successfully
				c.queueDB.Delete(&JobItem{
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
				err = (error)(&sdk.Error{
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
				if l > c.config.LogSize { // crop if too long
					tmp := (*item.Log)[l-c.config.LogSize+1:]
					item.Log = &tmp
				}
				log := append(*item.Log, errStr)
				item.Log = &log
			}

			if validOrder {
				item.FailCount += 1
			}

			// re-push to end of queue for too-many-fail item
			if l > c.config.FailThreshold {
				c.repushItem(item)
			} else {
				// update item
				now := time.Now()
				c.queueDB.UpdateOne(&JobItem{
					ID: item.ID,
				}, &JobItem{
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

func (c *ExecutionChannel) repushItem(item *JobItem) {
	repushedItem := &JobItem{
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
	c.queueDB.Delete(&JobItem{
		ID: item.ID,
	})
}

func (c *ExecutionChannel) putItem(item *JobItem) bool {
	c.item = item
	return true
}

func (c *ExecutionChannel) validateItemOrder(item *JobItem) bool {
	itemRs := c.queueDB.QueryS(&JobItem{
		SortedKey: item.SortedKey,
	}, 0, 1, "sort_index")

	firstItem := itemRs.Data.([]*JobItem)[0]
	if firstItem.SortIndex < item.SortIndex {
		return false
	}
	return true
}
