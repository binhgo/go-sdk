package sdk

import (
	"strconv"
	"sync"
	"time"

	"github.com/labstack/gommon/log"

	mgo "github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// DBQueue ...
type DBQueue struct {
	ColName    string
	DBName     string
	collection *mgo.Collection
	db         *mgo.Database
	consumed   *mgo.Collection
	master     *mgo.Collection
	mSession   *DBSession
	Consumer   ConsumeFn
	Limit      int
}

// ConsumeFn ...
type ConsumeFn = func(*QueueItem) error

func (q *DBQueue) startConsume() {
	if q.Limit == 0 {
		q.Limit = 10
	}

	for true {
		if q.Consumer == nil {
			time.Sleep(1000 * time.Millisecond)
		} else {
			items := []QueueItem{}
			q.queryItem(&items)

			length := len(items)
			log.Debug("=> Get result:  " + strconv.Itoa(length))
			if length > 0 {
				// wait group
				var wg sync.WaitGroup
				wg.Add(length)
				// loop and consume each item
				for _, item := range items {
					go q.run(item, &wg)
				}
				wg.Wait()
			} else {
				time.Sleep(50 * time.Millisecond)
			}
			log.Debug("=> End result:  " + strconv.Itoa(length))
		}
	}
}

func (q *DBQueue) queryItem(items *[]QueueItem) {
	s := q.mSession.Copy()
	defer s.Close()
	col := q.collection.With(s.GetMGOSession())

	query := col.Find(bson.M{})
	query.Limit(q.Limit)
	query.All(items)
}

func (q *DBQueue) run(item QueueItem, wg *sync.WaitGroup) {

	if item.Log == nil {
		item.Log = &[]string{}
	}
	err := q.Consumer(&item)
	if err == nil {
		s := q.mSession.Copy()
		defer s.Close()
		col := q.collection.With(s.GetMGOSession())

		err := col.Remove(bson.M{"_id": item.ID})
		if err != nil {
			log.Error(err.Error())
		}
		q.addConsumed(item)
	} else {
		go q.collection.Update(bson.M{
			"_id": item.ID,
		}, bson.M{
			"$set": bson.M{
				"log": &[]string{"Consume error: " + err.Error()},
			},
		})
	}
	wg.Done()
}

// InitDB ...
func (q *DBQueue) InitDB(s *DBSession, startConsumer bool) error {
	if len(q.DBName) == 0 || len(q.ColName) == 0 {
		return &Error{Type: "INVALID_INPUT", Message: "Require valid DB name and collection name."}
	}
	q.db = s.session.DB(q.DBName)
	q.collection = q.db.C(q.ColName)
	q.consumed = q.db.C(q.ColName + "_consumed")
	q.master = q.db.C("queue_master")
	q.mSession = s
	if startConsumer {
		go q.startConsume()
	}
	return nil
}

// SetConsumer ...
func (q *DBQueue) SetConsumer(consumer ConsumeFn) {
	q.Consumer = consumer
}

// Push put new item into queue
func (q *DBQueue) Push(data interface{}) error {
	// start := time.Now()
	if q.collection == nil {
		q.collection = q.db.C(q.ColName)
	}

	var item = bson.M{
		"last_updated_time": time.Now(),
		"created_time":      time.Now(),
		"data":              data,
	}
	s := q.mSession.Copy()
	defer s.Close()
	col := q.collection.With(s.GetMGOSession())

	col.Insert(item)
	// elapsed := time.Since(start)
	// log.Printf("Run time %s", elapsed/time.Millisecond)
	return nil
}

func (q *DBQueue) addConsumed(item QueueItem) error {
	if q.consumed == nil {
		q.consumed = q.db.C(q.ColName + "_consumed")
	}
	s := q.mSession.Copy()
	defer s.Close()
	col := q.consumed.With(s.GetMGOSession())
	time := time.Now()
	item.CreatedTime = &time
	col.Insert(item)
	return nil
}
