package sdk

import (
	"reflect"
	"time"

	mgo "github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// DBModel2 ...
type DBModel2 struct {
	ColName        string
	DBName         string
	TemplateObject interface{}
	collection     *mgo.Collection
	db             *mgo.Database
	mSession       *DBSession
}

// convertToObject convert bson to object
func (m *DBModel2) convertToObject(b bson.M) (interface{}, error) {
	obj := m.NewObject()

	if b == nil {
		return obj, nil
	}

	bytes, err := bson.Marshal(b)
	if err != nil {
		return nil, err
	}

	bson.Unmarshal(bytes, obj)
	return obj, nil
}

// convertToBson Go object to map (to get / query)
func (m *DBModel2) convertToBson(ent interface{}) (bson.M, error) {
	if ent == nil {
		return bson.M{}, nil
	}

	sel, err := bson.Marshal(ent)
	if err != nil {
		return nil, err
	}

	obj := bson.M{}
	bson.Unmarshal(sel, &obj)

	return obj, nil
}

// Init ...
func (m *DBModel2) Init(s *DBSession) error {

	if len(m.DBName) == 0 || len(m.ColName) == 0 {
		return &Error{Type: "INVALID_INPUT", Message: "Require valid DB name and collection name."}
	}
	dbName := s.database
	if s.database == "" || s.database == "admin" {
		dbName = m.DBName
	}
	m.db = s.session.DB(dbName)
	m.collection = m.db.C(m.ColName)
	m.mSession = s
	return nil
}

// NewObject return new object with same type of TemplateObject
func (m *DBModel2) NewObject() interface{} {
	t := reflect.TypeOf(m.TemplateObject)
	// fmt.Println(t)
	v := reflect.New(t)
	return v.Interface()
}

// NewList return new object with same type of TemplateObject
func (m *DBModel2) NewList(limit int) interface{} {
	t := reflect.TypeOf(m.TemplateObject)
	return reflect.MakeSlice(reflect.SliceOf(t), 0, limit).Interface()
}

// GetFreshSession ...
func (m *DBModel2) GetFreshSession() *DBSession {
	return m.mSession.Copy()
}

// GetColWith ...
func (m *DBModel2) GetColWith(s *DBSession) (*mgo.Collection, error) {
	if m.collection == nil {
		m.collection = m.db.C(m.ColName)
	}
	return m.collection.With(s.GetMGOSession()), nil
}

// Create insert one object into DB
func (m *DBModel2) Create(entity interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB error: " + err.Error(),
		}
	}

	// convert to bson
	obj, err := m.convertToBson(entity)
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "MAP_OBJECT_FAILED",
		}
	}

	// init time
	if obj["created_time"] == nil {
		obj["created_time"] = time.Now()
		obj["last_updated_time"] = obj["created_time"]
	} else {
		obj["last_updated_time"] = time.Now()
	}

	// insert
	err = col.Insert(obj)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}

	entity, _ = m.convertToObject(obj)

	list := m.NewList(1)
	listValue := reflect.Append(reflect.ValueOf(list),
		reflect.Indirect(reflect.ValueOf(entity)))

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Create " + m.ColName + " successfully.",
		Data:    listValue.Interface(),
	}

}

// CreateMany insert many object into db
func (m *DBModel2) CreateMany(entityList ...interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}
	objs := []bson.M{}
	ints := []interface{}{}

	if len(entityList) == 1 {
		rt := reflect.TypeOf(entityList[0])
		switch rt.Kind() {
		case reflect.Slice:
			entityList = entityList[0].([]interface{})
		case reflect.Array:
			entityList = entityList[0].([]interface{})
		}
	}
	for _, ent := range entityList {
		obj, err := m.convertToBson(ent)
		if err != nil {
			return &APIResponse{
				Status:    APIStatus.Error,
				Message:   "DB Error: " + err.Error(),
				ErrorCode: "MAP_OBJECT_FAILED",
			}
		}
		if obj["created_time"] == nil {
			obj["created_time"] = time.Now()
			obj["last_updated_time"] = obj["created_time"]
		} else {
			obj["last_updated_time"] = time.Now()
		}
		objs = append(objs, obj)
		ints = append(ints, obj)
	}

	err = col.Insert(ints...)
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "CREATE_FAILED",
		}
	}
	list := m.NewList(len(entityList))
	listValue := reflect.ValueOf(list)
	for _, obj := range objs {
		entity, _ := m.convertToObject(obj)
		listValue = reflect.Append(listValue, reflect.Indirect(reflect.ValueOf(entity)))
	}

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Create " + m.ColName + "(s) successfully.",
		Data:    listValue.Interface(),
	}
}

// Query Get all object in DB
func (m *DBModel2) Query(query interface{}, offset int, limit int, reverse bool) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	q := col.Find(query)
	if limit == 0 {
		limit = 1000
	}
	if limit > 0 {
		q.Limit(limit)
	}
	if offset > 0 {
		q.Skip(offset)
	}
	if reverse {
		q.Sort("-_id")
	}

	list := m.NewList(limit)
	err = q.All(&list)

	if err != nil || reflect.ValueOf(list).Len() == 0 {
		return &APIResponse{
			Status:    APIStatus.NotFound,
			Message:   "Not found any matched " + m.ColName + ".",
			ErrorCode: "NOT_FOUND",
		}
	}
	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Query " + m.ColName + " successfully.",
		Data:    list,
	}
}

// QueryS Get all object in DB with orderby clause
func (m *DBModel2) QueryS(query interface{}, offset int, limit int, sortFields ...string) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	q := col.Find(query)
	if limit == 0 {
		limit = 1000
	}
	if limit > 0 {
		q.Limit(limit)
	}
	if offset > 0 {
		q.Skip(offset)
	}
	if len(sortFields) > 0 {
		sortFields = deleteEmpty(sortFields)
		q.Sort(sortFields...)
	}

	list := m.NewList(limit)
	err = q.All(&list)

	if err != nil || reflect.ValueOf(list).Len() == 0 {
		return &APIResponse{
			Status:    APIStatus.NotFound,
			Message:   "Not found any matched " + m.ColName + ".",
			ErrorCode: "NOT_FOUND",
		}
	}
	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Query " + m.ColName + " successfully.",
		Data:    list,
	}
}

// Update Update all matched item
func (m *DBModel2) Update(query interface{}, updater interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	obj, err := m.convertToBson(updater)
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "MAP_OBJECT_FAILED",
		}
	}
	obj["last_updated_time"] = time.Now()

	info, err := col.UpdateAll(query, bson.M{
		"$set": obj,
	})
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "Update error: " + err.Error(),
			ErrorCode: "UPDATE_FAILED",
		}
	}

	if info.Matched == 0 {
		return &APIResponse{
			Status:  APIStatus.Ok,
			Message: "Not found any " + m.ColName + ".",
		}
	}

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Update " + m.ColName + " successfully.",
	}
}

// QueryOne ...
func (m *DBModel2) QueryOne(query interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	q := col.Find(query)
	q.Limit(1)

	list := m.NewList(1)
	err = q.All(&list)
	if err != nil || reflect.ValueOf(list).Len() == 0 {
		return &APIResponse{
			Status:    APIStatus.NotFound,
			Message:   "Not found any matched " + m.ColName + ".",
			ErrorCode: "NOT_FOUND",
		}
	}
	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Query " + m.ColName + " successfully.",
		Data:    list,
	}
}

// UpdateOne Update one matched object.
func (m *DBModel2) UpdateOne(query interface{}, updater interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	bUpdater, err := m.convertToBson(updater)
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "MAP_OBJECT_FAILED",
		}
	}
	bUpdater["last_updated_time"] = time.Now()

	change := mgo.Change{
		Update:    bson.M{"$set": bUpdater},
		ReturnNew: true,
	}
	tmp := bson.M{}
	q := col.Find(query)
	q.Limit(1)
	return m.applyUpdateOne(q, &change, &tmp)
}

func (m *DBModel2) UpdateOneSort(query interface{}, sortFields []string, updater interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	bUpdater, err := m.convertToBson(updater)
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "MAP_OBJECT_FAILED",
		}
	}
	bUpdater["last_updated_time"] = time.Now()

	change := mgo.Change{
		Update:    bson.M{"$set": bUpdater},
		ReturnNew: true,
	}
	tmp := bson.M{}
	q := col.Find(query)
	q.Limit(1).Sort(sortFields...)
	return m.applyUpdateOne(q, &change, &tmp)
}

func (m *DBModel2) applyUpdateOne(q *mgo.Query, change *mgo.Change, newResult interface{}) *APIResponse {
	obj := m.NewObject()
	info, err := q.Apply(*change, newResult)
	if (err != nil && err.Error() == "not found") || (info != nil && info.Matched == 0) {
		return &APIResponse{
			Status:  APIStatus.NotFound,
			Message: "Not found any " + m.ColName + ".",
		}
	}

	if err == nil {
		bytes, err := bson.Marshal(newResult)
		if err == nil {
			bson.Unmarshal(bytes, obj)
			list := m.NewList(1)
			listValue := reflect.Append(reflect.ValueOf(list),
				reflect.Indirect(reflect.ValueOf(obj)))
			return &APIResponse{
				Status:  APIStatus.Ok,
				Message: "Update one " + m.ColName + " successfully.",
				Data:    listValue.Interface(),
			}
		}

	}

	return &APIResponse{
		Status:    APIStatus.Error,
		Message:   "Update error: " + err.Error(),
		ErrorCode: "UPDATE_FAILED",
	}
}

// UpsertOne Update one matched object, if notfound, create new document
func (m *DBModel2) UpsertOne(query interface{}, updater interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	bUpdater, err := m.convertToBson(updater)
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "MAP_OBJECT_FAILED",
		}
	}
	now := time.Now()
	bUpdater["last_updated_time"] = now

	change := mgo.Change{
		Update: bson.M{
			"$set": bUpdater,
			"$setOnInsert": bson.M{
				"created_time": now,
			},
		},
		ReturnNew: true,
		Upsert:    true,
	}

	obj := m.NewObject()
	tmp := bson.M{}
	_, err = col.Find(query).Limit(1).Apply(change, &tmp)
	if err == nil {
		bytes, err := bson.Marshal(tmp)
		if err == nil {
			bson.Unmarshal(bytes, obj)
			list := m.NewList(1)
			listValue := reflect.Append(reflect.ValueOf(list),
				reflect.Indirect(reflect.ValueOf(obj)))
			return &APIResponse{
				Status:  APIStatus.Ok,
				Message: "Upsert one " + m.ColName + " successfully.",
				Data:    listValue.Interface(),
			}
		}

	}

	if err.Error() == "not found" {
		return &APIResponse{
			Status:  APIStatus.NotFound,
			Message: "Not found any " + m.ColName + ".",
		}
	}
	return &APIResponse{
		Status:    APIStatus.Error,
		Message:   "DB Error: " + err.Error(),
		ErrorCode: "UPSERT_ONE_ERROR",
	}

}

// Delete Delete all object which matched with selector
func (m *DBModel2) Delete(selector interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	err = col.Remove(selector)
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "Delete error: " + err.Error(),
			ErrorCode: "DELETE_FAILED",
		}
	}
	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Delete " + m.ColName + " successfully.",
	}
}

// Count Count object which matched with query.
func (m *DBModel2) Count(query interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	count, err := col.Find(query).Count()
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "Count error: " + err.Error(),
			ErrorCode: "COUNT_FAILED",
		}
	}

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Count query executed successfully.",
		Total:   int64(count),
	}

}

// IncreOne Increase one field of the document & return new value
func (m *DBModel2) IncreOne(query interface{}, fieldName string, value int) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "GET_COLLECTION_FAILED",
		}
	}

	updater := bson.M{}
	updater[fieldName] = value
	change := mgo.Change{
		Update:    bson.M{"$inc": updater},
		ReturnNew: true,
		Upsert:    true,
	}

	obj := m.NewObject()
	_, err = col.Find(query).Limit(1).Apply(change, obj)
	list := m.NewList(1)
	listValue := reflect.Append(reflect.ValueOf(list),
		reflect.Indirect(reflect.ValueOf(obj)))
	if err != nil {
		if err.Error() == "not found" {
			return &APIResponse{
				Status:  APIStatus.NotFound,
				Message: "Not found any " + m.ColName + ".",
			}
		}
		return &APIResponse{
			Status:    APIStatus.Error,
			Message:   "DB Error: " + err.Error(),
			ErrorCode: "INCREMENT_ERROR",
		}
	}
	return &APIResponse{
		Status:  APIStatus.Ok,
		Data:    listValue.Interface(),
		Message: "Increase " + fieldName + " of " + m.ColName + " successfully.",
	}
}

// CreateIndex ...
func (m *DBModel2) CreateIndex(index mgo.Index) error {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err == nil {
		crErr := col.EnsureIndex(index)
		return crErr
	}
	return err
}

// Aggregate ...
func (m *DBModel2) Aggregate(pipeline interface{}, result interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: err.Error(),
		}
	}
	q := col.Pipe(pipeline)
	err = q.All(result)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: err.Error(),
		}
	}

	return &APIResponse{
		Status: APIStatus.Ok,
	}
}

// Aggregate ...
func (m *DBModel2) Distinct(filter interface{}, field string, values interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: err.Error(),
		}
	}
	err = col.Find(filter).Distinct(field, values)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: err.Error(),
		}
	}
	return &APIResponse{
		Status: APIStatus.Ok,
		Data:   values,
	}
}
