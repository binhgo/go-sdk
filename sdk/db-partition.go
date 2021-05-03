package sdk

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	hm "github.com/cornelk/hashmap"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// PartitionType ...
type PartitionType string

type partitionData struct {
	Day   PartitionType
	Month PartitionType
	Year  PartitionType
}

// PartitionTypes ...
var PartitionTypes = partitionData{
	Day:   "Day",
	Month: "MONTH",
	Year:  "YEAR",
}

// InitCollectionHandler ...
type InitCollectionHandler = func(session *DBSession, colName string, dbName string) (DBModel, error)

// DBPartitionedModel ...
type DBPartitionedModel struct {
	ColName          string
	DBMap            *hm.HashMap // store DBModel list
	DbName           string
	PartitionType    PartitionType
	db               *mgo.Database
	session          *DBSession
	OnInitCollection InitCollectionHandler
}

// InitCollection ...
func (p *DBPartitionedModel) InitCollection(fn InitCollectionHandler) {
	p.OnInitCollection = fn
}

// SetDatabase ...
func (p *DBPartitionedModel) SetDatabase(s *DBSession, partitionType PartitionType) {

	if p.ColName == "" || p.DbName == "" || p.OnInitCollection == nil {
		panic(errors.New("Missing data Partition"))
	}
	// clear map
	p.DBMap = &hm.HashMap{}
	p.db = s.session.DB(p.DbName)
	p.session = s
	p.PartitionType = partitionType
}

// GetModelByName ...
func (p *DBPartitionedModel) GetModelByName(colName string) *DBModel {

	model, ok := p.DBMap.Get(colName)
	if ok {
		return model.(*DBModel)
	}
	modelInited, err := p.OnInitCollection(p.session, colName, p.DbName)
	if err == nil {
		p.DBMap.Set(colName, &modelInited)
		return &modelInited
	}
	return nil
}

// GetModel ...
func (p *DBPartitionedModel) GetModel(time time.Time) *DBModel {
	colName := p.GetNameFromValue(time)
	return p.GetModelByName(colName)
}

// GetNameFromValue ...
func (p *DBPartitionedModel) GetNameFromValue(time time.Time) string {

	switch p.PartitionType {
	case PartitionTypes.Day:
		return fmt.Sprintf("%s_%04d_%02d_%02d", p.ColName, time.Year(), time.Month(), time.Day())
	case PartitionTypes.Month:
		return fmt.Sprintf("%s_%04d_%02d", p.ColName, time.Year(), time.Month())
	case PartitionTypes.Year:
		return fmt.Sprintf("%s_%04d", p.ColName, time.Year())
	}
	return p.ColName
}

// ==============================CURD=======================================//

// Create : create document partition by date
func (p *DBPartitionedModel) Create(ent interface{}) error {
	obj, err := parse(ent)
	if err != nil {
		return err
	}
	now := time.Now()

	obj["created_time"] = now
	obj["last_updated_time"] = now

	dateVal := obj["date"]
	if dateVal == nil {
		obj["date"] = now
	}
	date := obj["date"].(time.Time)

	// get model from date
	dbModel := p.GetModel(date)
	// col, err := m.GetCollection()
	s := dbModel.GetFreshSession()
	defer s.Close()
	col, err := dbModel.GetColWith(s)

	if err != nil {
		return err
	}
	return col.Insert(obj)
}

// Query ...
func (p *DBPartitionedModel) Query(query bson.M, date time.Time, maxSpanCount int, offset int, limit int, reverse bool, result interface{}) (int, error) {

	// Init Slice with input Type
	object := reflect.Indirect(reflect.ValueOf(result))
	listType := object.Type()
	fRs := reflect.MakeSlice(listType, 0, 0)

	// Caculate total
	if limit == 0 {
		limit = math.MaxInt32
	}
	var total int
	dates := []time.Time{}
	count := []int{}
	dateTmp := date

	// cal total and dates
	for maxSpanCount >= 0 {
		dbModel := p.GetModel(dateTmp)
		cur, err := dbModel.Count(query)
		if err == nil {
			dates = append(dates, dateTmp)
			count = append(count, cur)
			total += cur
		} else {
			fmt.Println(err.Error())
		}
		switch p.PartitionType {
		case PartitionTypes.Year:
			dateTmp = getTimeYear(dateTmp).AddDate(-1, 0, 0)
		case PartitionTypes.Month:
			dateTmp = getTimeMonth(dateTmp).AddDate(0, -1, 0)
		case PartitionTypes.Day:
			dateTmp = getTimeDay(dateTmp).AddDate(0, 0, -1)

		}
		maxSpanCount--
	}

	length := len(dates)
	// loop partition by dates and query
	for i := 0; i < length; i++ {
		j := i
		if !reverse {
			j = length - i - 1
		}
		time := dates[j]
		cur := count[j]
		if cur == 0 {
			continue
		}

		if offset >= cur {
			offset -= cur
		} else {
			dbModel := p.GetModel(time)
			errQ := dbModel.Query(query, offset, limit, reverse, result)
			if errQ == nil && result != nil {
				// Reflect result and append
				object = reflect.Indirect(reflect.ValueOf(result))
				for i := 0; i < object.Len(); i++ {
					fRs = reflect.Append(fRs, object.Index(i))
				}
				if object.Len() >= limit {
					break
				}
				limit -= object.Len()
				offset = 0
			} else {
				fmt.Println(errQ.Error())
			}
		}
	}

	v := reflect.Indirect(reflect.ValueOf(result))
	v.Set(fRs)
	return total, nil
}

// Q ...
func (p *DBPartitionedModel) Q(query interface{}, date time.Time, maxSpanCount int, offset int, limit int, reverse bool, result interface{}) (int, error) {

	// Init Slice with input Type
	object := reflect.Indirect(reflect.ValueOf(result))
	listType := object.Type()
	fRs := reflect.MakeSlice(listType, 0, 0)

	// Caculate total
	if limit == 0 {
		limit = math.MaxInt32
	}
	var total int
	dates := []time.Time{}
	count := []int{}
	dateTmp := date

	// cal total and dates
	for maxSpanCount >= 0 {
		dbModel := p.GetModel(dateTmp)
		cur, err := dbModel.Count(query)
		if err == nil {
			dates = append(dates, dateTmp)
			count = append(count, cur)
			total += cur
		} else {
			fmt.Println(err.Error())
		}
		switch p.PartitionType {
		case PartitionTypes.Year:
			dateTmp = getTimeYear(dateTmp).AddDate(-1, 0, 0)
		case PartitionTypes.Month:
			dateTmp = getTimeMonth(dateTmp).AddDate(0, -1, 0)
		case PartitionTypes.Day:
			dateTmp = getTimeDay(dateTmp).AddDate(0, 0, -1)

		}
		maxSpanCount--
	}

	length := len(dates)
	// loop partition by dates and query
	for i := 0; i < length; i++ {
		j := i
		if !reverse {
			j = length - i - 1
		}
		time := dates[j]
		cur := count[j]
		if cur == 0 {
			continue
		}

		if offset >= cur {
			offset -= cur
		} else {
			dbModel := p.GetModel(time)
			qRs, errQ := dbModel.Q(query, offset, limit, reverse)
			if errQ == nil && qRs != nil {
				qRs.All(result)
				// Reflect result and append
				object = reflect.Indirect(reflect.ValueOf(result))
				for i := 0; i < object.Len(); i++ {
					fRs = reflect.Append(fRs, object.Index(i))
				}
				if object.Len() >= limit {
					break
				}
				limit -= object.Len()
				offset = 0
			} else {
				fmt.Println(errQ.Error())
			}
		}
	}

	v := reflect.Indirect(reflect.ValueOf(result))
	v.Set(fRs)
	return total, nil
}

func getTimeYear(t time.Time) time.Time {
	return time.Date(t.Year(), 6, t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
}

func getTimeMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 15, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
}

func getTimeDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 12, t.Minute(), t.Second(), t.Nanosecond(), t.Location())
}

func convertSlice(list interface{}) []interface{} {

	object := reflect.Indirect(reflect.ValueOf(list))
	var items []interface{}

	for i := 0; i < object.Len(); i++ {
		items = append(items, object.Index(i).Interface())
	}

	return items
}

func parse(ent interface{}) (map[string]interface{}, error) {
	sel, err := bson.Marshal(ent)
	if err != nil {
		return nil, err
	}

	obj := map[string]interface{}{}
	bson.Unmarshal(sel, &obj)
	return obj, nil
}

// C Count mongo ...
func (p *DBPartitionedModel) C(query interface{}, date time.Time, maxSpanCount int) (int, error) {

	// Caculate total
	var total int
	dateTmp := date
	// cal total
	for maxSpanCount >= 0 {
		dbModel := p.GetModel(dateTmp)
		cur, err := dbModel.Count(query)
		if err == nil {
			total += cur
		} else {
			return total, err
		}
		switch p.PartitionType {
		case PartitionTypes.Year:
			dateTmp = getTimeYear(dateTmp).AddDate(-1, 0, 0)
		case PartitionTypes.Month:
			dateTmp = getTimeMonth(dateTmp).AddDate(0, -1, 0)
		case PartitionTypes.Day:
			dateTmp = getTimeDay(dateTmp).AddDate(0, 0, -1)

		}
		maxSpanCount--
	}

	return total, nil
}

// Count ...
func (p *DBPartitionedModel) Count(query bson.M, date time.Time, maxSpanCount int) (int, error) {

	var total int
	dateTmp := date

	// cal total and dates
	for maxSpanCount >= 0 {
		dbModel := p.GetModel(dateTmp)
		cur, err := dbModel.Count(query)
		if err == nil {
			total += cur
		} else {
			return total, err
		}
		switch p.PartitionType {
		case PartitionTypes.Year:
			dateTmp = getTimeYear(dateTmp).AddDate(-1, 0, 0)
		case PartitionTypes.Month:
			dateTmp = getTimeMonth(dateTmp).AddDate(0, -1, 0)
		case PartitionTypes.Day:
			dateTmp = getTimeDay(dateTmp).AddDate(0, 0, -1)

		}
		maxSpanCount--
	}
	return total, nil
}

// R convert to APIResponse
func (p *DBPartitionedModel) R(result interface{}, err error) *APIResponse {
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB error: " + err.Error(),
		}
	}

	s := reflect.ValueOf(result)
	if s.Len() == 0 {
		return &APIResponse{
			Status:  APIStatus.NotFound,
			Message: "DB error: No " + p.ColName + " was found.",
		}
	}

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Query " + p.ColName + " successfully.",
		Data:    result,
	}
}
