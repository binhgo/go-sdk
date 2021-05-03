package job

import (
	"github.com/globalsign/mgo/bson"
	"gitlab.ghn.vn/common-projects/go-sdk/sdk"
	"time"
)

// ExecutionSelector ...
type ExecutionSelector struct {
	name            string
	channels        []*ExecutionChannel
	queueDB         *sdk.DBModel2
	version         string
	acceptableTopic []string
	config          *ExecutorConfiguration
}

func (es *ExecutionSelector) pickFreeChannel(start int, limit int) int {
	var quota = limit
	var i = start
	for quota > 0 {
		if !es.channels[i].processing {
			es.channels[i].processing = true
			return i
		}
		i = (i + 1) % limit
		quota--
	}
	return -1
}

func (es *ExecutionSelector) start() {

	var counter = 0
	for true {

		if es.config.ParallelTopicProcessing {
			var topics []string
			topicResult := es.queueDB.Distinct(nil, "topic", &topics)

			if topicResult.Status == sdk.APIStatus.Ok {

				if len(topics) > 0 {
					for _, topic := range topics {
						if es.config.SortedItem {
							es.scanTopic(topic)
						} else {
							es.doSelect(topic, "")
						}
					}
				} else {
					time.Sleep(time.Duration(es.config.SelectorDelayMS) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(es.config.SelectorDelayMS) * time.Millisecond)
			}
		} else {
			if es.config.SortedItem {
				es.scanTopic("")
			} else {
				es.doSelect("", "")
			}
		}

		// check & clean old item
		counter++
		if counter > 500 {

			// clean old item with different version
			oldTime := time.Now().Add(-(time.Duration(es.config.OldVersionTimeoutS) * time.Second))
			es.queueDB.Update(&bson.M{
				"process_by": bson.M{
					"$ne": "NONE",
				},
				"consumer_version": bson.M{
					"$ne": es.version,
				},
				"last_updated_time": bson.M{
					"$lt": oldTime,
				},
			}, &bson.M{
				"process_by":       "NONE",
				"consumer_version": "NONE",
			})

			// all version
			oldTime = oldTime.Add(-(time.Duration(es.config.CurVersionTimeoutS) * time.Second))
			es.queueDB.Update(&bson.M{
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

func (es *ExecutionSelector) scanTopic(topic string) {
	var query = bson.M{
		"process_by": "NONE",
	}
	if topic != "" {
		query["topic"] = topic
	}
	var keys []string
	keysResult := es.queueDB.Distinct(&query, "sorted_key", &keys)
	if keysResult.Status == sdk.APIStatus.Ok {
		if len(keys) > 0 {
			for _, key := range keys {
				es.doSelect(topic, key)
			}
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		time.Sleep(10 * time.Millisecond)
	}
}

func (es *ExecutionSelector) doSelect(topic string, sortedKey string) {
	var channelNum = len(es.channels)

	// pick channel
	picked := -1
	for picked < 0 {
		picked = es.pickFreeChannel(0, channelNum)
		if picked < 0 {
			// if all channels are busy
			time.Sleep(10 * time.Millisecond)
		}
	}

	// setup query
	var query *bson.M
	if !es.config.ParallelTopicProcessing && es.acceptableTopic != nil && len(es.acceptableTopic) > 0 {
		query = &bson.M{
			"process_by": "NONE",
			"topic": &bson.M{
				"$in": es.acceptableTopic,
			},
		}
	} else {
		if topic == "" {
			topic = "default"
		}
		query = &bson.M{
			"process_by": "NONE",
			"topic":      topic,
		}
	}

	if es.config.WaitForReadyTime {
		(*query)["ready_time"] = &bson.M{
			"$lt": time.Now(),
		}
	}

	// pick an item from DB
	var resp *sdk.APIResponse
	if es.config.SortedItem {
		(*query)["sorted_key"] = sortedKey

		resp = es.queueDB.QueryS(query, 0, 1, "sort_index")
		if resp.Status == sdk.APIStatus.Ok {
			item := resp.Data.([]*JobItem)[0]
			if item.ProcessBy == "NONE" {
				resp = es.queueDB.UpdateOneSort(query, []string{"sort_index"}, &bson.M{
					"process_by":       es.name + " - " + es.channels[picked].name,
					"consumer_version": es.version,
				})
				if resp.Status == sdk.APIStatus.Ok {
					item = resp.Data.([]*JobItem)[0]
					if !es.validateItemOrder(item) {
						resp.Status = sdk.APIStatus.Existed
						es.queueDB.UpdateOne(&JobItem{
							ID: item.ID,
						}, &JobItem{
							ProcessBy: "NONE",
						})
					}
				}
			} else {
				resp.Status = sdk.APIStatus.Existed
			}
		}
	} else {
		resp = es.queueDB.UpdateOne(query, &bson.M{
			"process_by":       es.name + " - " + es.channels[picked].name,
			"consumer_version": es.version,
		})
	}

	if resp.Status == sdk.APIStatus.Ok {
		item := resp.Data.([]*JobItem)[0]
		es.channels[picked].putItem(item)
	} else {
		// if no item found
		es.channels[picked].processing = false
		if !es.config.ParallelTopicProcessing && !es.config.SortedItem { // sleep shorter if parallel processing
			time.Sleep(time.Duration(es.config.SelectorDelayMS) * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (es *ExecutionSelector) validateItemOrder(item *JobItem) bool {
	itemRs := es.queueDB.QueryS(&JobItem{
		SortedKey: item.SortedKey,
	}, 0, 1, "sort_index")

	if itemRs.Status == sdk.APIStatus.Ok {
		firstItem := itemRs.Data.([]*JobItem)[0]
		if firstItem.SortIndex < item.SortIndex {
			return false
		}
	}
	return true
}
