package sdk

import (
	"crypto/tls"
	"errors"
	"net"
	"reflect"
	"time"

	mgo "github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// DBConfiguration ...
type DBConfiguration struct {
	Address            []string
	Ssl                bool
	Username           string
	Password           string
	AuthDB             string
	ReplicaSetName     string
	SecondaryPreferred bool
}

// DBClient ..
type DBClient struct {
	Name        string
	Config      DBConfiguration
	onConnected OnConnectedHandler
}

// DBSession ..
type DBSession struct {
	session  *mgo.Session
	database string
}

// GetMGOSession : get mgo session
func (s *DBSession) GetMGOSession() *mgo.Session {
	return s.session
}

// Copy : copy dbsession
func (s *DBSession) Copy() *DBSession {
	return &DBSession{
		session: s.session.Copy(),
	}
}

// Valid : check if session is active
func (s *DBSession) Valid() bool {
	return s.session.Ping() == nil
}

// Clone : clone dbsession
func (s *DBSession) Clone() *DBSession {
	return &DBSession{
		session: s.session.Clone(),
	}
}

// Close : close db session
func (s *DBSession) Close() {
	s.session.Close()
}

// OnConnectedHandler ...
type OnConnectedHandler = func(session *DBSession) error

// OnConnected ...
func (client *DBClient) OnConnected(fn OnConnectedHandler) {
	client.onConnected = fn
}

// Connect ...
func (client *DBClient) Connect() error {
	dialInfo := mgo.DialInfo{
		Addrs:          client.Config.Address,
		Username:       client.Config.Username,
		Password:       client.Config.Password,
		ReplicaSetName: client.Config.ReplicaSetName,
		PoolLimit:      200,
		MinPoolSize:    3,
		MaxIdleTimeMS:  600000,
	}

	if client.Config.AuthDB != "" {
		dialInfo.Database = client.Config.AuthDB
	}

	if client.Config.Ssl {
		tlsConfig := &tls.Config{}
		tlsConfig.InsecureSkipVerify = true
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			return conn, err
		}
	}
	session, err := mgo.DialWithInfo(&dialInfo)
	if client.Config.SecondaryPreferred {
		session.SetMode(mgo.SecondaryPreferred, true)
	}

	if err != nil {
		return err
	}

	err = client.onConnected(&DBSession{session: session, database: client.Config.AuthDB})
	return err
}

// DBModel ...
type DBModel struct {
	ColName    string
	DBName     string
	collection *mgo.Collection
	db         *mgo.Database
	mSession   *DBSession
}

// parse Go object to map (to insert/update DB)
func (m *DBModel) parse(ent interface{}) (map[string]interface{}, error) {
	if ent == nil {
		return map[string]interface{}{}, nil
	}

	sel, err := bson.Marshal(ent)
	if err != nil {
		return nil, err
	}

	obj := map[string]interface{}{}
	bson.Unmarshal(sel, &obj)
	return obj, nil
}

// parseToQuery Go object to map (to get / query)
func (m *DBModel) parseToQuery(ent interface{}) (bson.M, error) {
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
func (m *DBModel) Init(s *DBSession) error {
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

// GetFreshSession ...
func (m *DBModel) GetFreshSession() *DBSession {
	return m.mSession.Copy()
}

// GetCollection ...
// Deprecated: Use GetColWith instead.
// func (m *DBModel) GetCollection() (*mgo.Collection, error) {
// 	if m.collection == nil {
// 		m.collection = m.db.C(m.ColName)
// 	}
// 	return m.collection, nil
// }

// GetColWith ...
func (m *DBModel) GetColWith(s *DBSession) (*mgo.Collection, error) {
	if m.collection == nil {
		m.collection = m.db.C(m.ColName)
	}
	return m.collection.With(s.GetMGOSession()), nil
}

// DBQuery ...
type DBQuery struct {
	query    *mgo.Query
	mSession *DBSession
}

// All ...
func (q *DBQuery) All(result interface{}) error {
	if q.query != nil && q.mSession != nil {
		defer q.mSession.Close()
		return q.query.All(result)
	}
	return errors.New("Query must be initialized")
}

// One ...
func (q *DBQuery) One(result interface{}) error {
	if q.query != nil && q.mSession != nil {
		defer q.mSession.Close()
		return q.query.One(result)
	}
	return errors.New("Query must be initialized")
}

// Count ...
func (q *DBQuery) Count() (n int, err error) {
	if q.query != nil && q.mSession != nil {
		defer q.mSession.Close()
		return q.query.Count()
	}
	return 0, errors.New("Query must be initialized")
}

// Distinct ...
func (q *DBQuery) Distinct(key string, result interface{}) error {
	if q.query != nil && q.mSession != nil {
		defer q.mSession.Close()

		return q.query.Distinct(key, result)
	}
	return errors.New("Query must be initialized")
}

// Execute Get all object in DB
func (m *DBModel) Execute(query *DBQuery, result interface{}) error {
	err := query.All(result)
	return err
}

// Q Get all object in DB
func (m *DBModel) Q(obj interface{}, offset int, limit int, reverse bool) (*DBQuery, error) {
	query, err := m.parseToQuery(obj)
	if err != nil {
		return nil, err
	}

	s := m.GetFreshSession()
	col, err := m.GetColWith(s)
	if err != nil {
		return nil, err
	}
	q := col.Find(query)
	if limit > 0 {
		q.Limit(limit)
	}
	if offset > 0 {
		q.Skip(offset)
	}
	if reverse {
		q.Sort("-_id")
	}

	return &DBQuery{
		query:    q,
		mSession: s,
	}, nil
}

// QS Get all object in DB with sort logic
func (m *DBModel) QS(obj interface{}, offset int, limit int, sortLogic string) (*DBQuery, error) {

	query, err := m.parseToQuery(obj)
	if err != nil {
		return nil, err
	}

	s := m.GetFreshSession()
	col, err := m.GetColWith(s)

	if err != nil {
		return nil, err
	}

	q := col.Find(query)
	if limit > 0 {
		q.Limit(limit)
	}
	if offset > 0 {
		q.Skip(offset)
	}
	if sortLogic != "" {
		q.Sort(sortLogic)
	}

	return &DBQuery{
		query:    q,
		mSession: s,
	}, nil
}

// R convert to APIResponse
func (m *DBModel) R(result interface{}, err error) *APIResponse {
	if err != nil {
		if err.Error() != "not found" {
			return &APIResponse{
				Status:  APIStatus.Error,
				Message: "DB error: " + err.Error(),
			}
		}
		return &APIResponse{
			Status:  APIStatus.NotFound,
			Message: "Not found any matched " + m.ColName,
		}

	}

	rt := reflect.TypeOf(result)
	switch rt.Kind() {
	case reflect.Slice:
	case reflect.Array:
		s := reflect.ValueOf(result)
		if s.Len() == 0 {
			return &APIResponse{
				Status:  APIStatus.NotFound,
				Message: "Not found any matched " + m.ColName,
			}
		}
	}

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Query " + m.ColName + " successfully.",
		Data:    result,
	}
}

// G Get all object in DB
func (m *DBModel) G(id string, result interface{}) *APIResponse {

	if !bson.IsObjectIdHex(id) {
		return &APIResponse{
			Status:  APIStatus.Invalid,
			Message: "Invalid id.",
		}
	}

	// col, err := m.GetCollection()
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB error: " + err.Error(),
		}
	}
	err = col.Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(result)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.NotFound,
			Message: "DB error: " + err.Error(),
		}
	}

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Get " + m.ColName + " successfully.",
		Data:    []interface{}{result},
	}
}

// Query Get all object in DB
func (m *DBModel) Query(query bson.M, offset int, limit int, reverse bool, result interface{}) error {
	// col, err := m.GetCollection()

	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return err
	}
	q := col.Find(query)
	if limit > 0 {
		q.Limit(limit)
	}
	if offset > 0 {
		q.Skip(offset)
	}
	if reverse {
		q.Sort("-_id")
	}

	err = q.All(result)
	return err
}

// QueryOne Get all object in DB
func (m *DBModel) QueryOne(query bson.M, result interface{}) (interface{}, error) {
	// col, err := m.GetCollection()
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return nil, err
	}
	err = col.Find(query).Limit(1).One(result)
	return result, err
}

// Create Insert new entity
func (m *DBModel) Create(ent interface{}) error {
	// col, err := m.GetCollection()
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return err
	}

	obj, err := m.parse(ent)
	if err != nil {
		return err
	}

	obj["created_time"] = time.Now()
	obj["last_updated_time"] = time.Now()
	return col.Insert(obj)
}

// I Insert new entity
func (m *DBModel) I(ent interface{}) *APIResponse {
	// col, err := m.GetCollection()
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}

	obj, err := m.parse(ent)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}

	obj["created_time"] = time.Now()
	obj["last_updated_time"] = time.Now()
	err = col.Insert(obj)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Create " + m.ColName + " successfully.",
	}
}

// Update Update exist entity
func (m *DBModel) Update(id string, updater interface{}) error {
	// col, err := m.GetCollection()

	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return err
	}

	obj, err := m.parse(updater)
	if err != nil {
		return err
	}

	obj["last_updated_time"] = time.Now()
	return col.Update(bson.M{"_id": bson.ObjectIdHex(id)}, bson.M{"$set": obj})
}

// U Update exist entity
func (m *DBModel) U(id string, updater interface{}) *APIResponse {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	// col, err := m.GetCollection()
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}

	obj, err := m.parse(updater)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}

	obj["last_updated_time"] = time.Now()
	err = col.Update(bson.M{"_id": bson.ObjectIdHex(id)}, bson.M{"$set": obj})
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}

	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Update " + m.ColName + " successfully.",
	}
}

// Delete Delete exist entity
func (m *DBModel) Delete(id string) error {
	// col, err := m.GetCollection()
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return err
	}
	return col.Remove(bson.M{"_id": bson.ObjectIdHex(id)})
}

// D Delete exist entity
func (m *DBModel) D(id string) *APIResponse {
	// col, err := m.GetCollection()
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}
	err = col.Remove(bson.M{"_id": bson.ObjectIdHex(id)})
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}
	return &APIResponse{
		Status:  APIStatus.Ok,
		Message: "Delete " + m.ColName + " successfully.",
	}
}

// Incre Delete exist entity
func (m *DBModel) IncreOne(query interface{}, fieldName string, value int, result interface{}) *APIResponse {
	// col, err := m.GetCollection()
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}
	updater := bson.M{}
	updater[fieldName] = value
	change := mgo.Change{
		Update:    bson.M{"$inc": updater},
		ReturnNew: true,
		Upsert:    true,
	}
	_, err = col.Find(query).Limit(1).Apply(change, result)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}
	return &APIResponse{
		Status:  APIStatus.Ok,
		Data:    []interface{}{result},
		Message: "Increase " + fieldName + " successfully.",
	}
}

// FindAndUpdate Delete exist entity
func (m *DBModel) FindAndUpdate(query interface{}, updater interface{}, limit int, result interface{}) *APIResponse {
	// col, err := m.GetCollection()
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}
	change := mgo.Change{
		Update:    bson.M{"$set": updater},
		ReturnNew: true,
	}
	_, err = col.Find(query).Limit(limit).Apply(change, result)
	if err != nil {
		return &APIResponse{
			Status:  APIStatus.Error,
			Message: "DB Error: " + err.Error(),
		}
	}
	return &APIResponse{
		Status:  APIStatus.Ok,
		Data:    []interface{}{result},
		Message: "Find and update successfully.",
	}
}

// Count Count by condition
func (m *DBModel) Count(obj interface{}) (int, error) {
	query, err := m.parseToQuery(obj)
	if err != nil {
		return 0, err
	}

	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return 0, err
	}
	return col.Find(query).Count()
}

// CountAll Count all exist entity of collection
func (m *DBModel) CountAll() (int, error) {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err != nil {
		return 0, err
	}
	return col.Count()
}

// CreateIndex ...
func (m *DBModel) CreateIndex(index mgo.Index) error {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)
	if err == nil {
		crErr := col.EnsureIndex(index)
		return crErr
	}
	return err
}

// CreateMany Insert many entity
func (m *DBModel) CreateMany(ents ...interface{}) error {
	s := m.GetFreshSession()
	defer s.Close()
	col, err := m.GetColWith(s)

	if err != nil {
		return err
	}
	objs := []interface{}{}

	for _, ent := range ents {
		obj, err := m.parse(ent)
		if err != nil {
			return err
		}
		obj["created_time"] = time.Now()
		obj["last_updated_time"] = time.Now()
		objs = append(objs, obj)
	}

	return col.Insert(objs...)
}
