package websocket

import (
	"fmt"
	"strings"
	"sync"
)

type OnWSConnectedHandler = func(conn *Connection)
type OnWSMessageHandler = func(conn *Connection, message string)
type OnWSCloseHandler = func(conn *Connection, err error)

type wsRoute struct {
	OnConnected OnWSConnectedHandler
	OnMessage   OnWSMessageHandler
	OnClose     OnWSCloseHandler

	conMap          map[int]*Connection
	payloadSize     int
	countConnection bool
	instanceId      string

	Lock *sync.RWMutex
}

// default construction
func newWSRoute() *wsRoute {
	return &wsRoute{
		Lock:        &sync.RWMutex{},
		conMap:      map[int]*Connection{},
		payloadSize: 512, // default 512
	}
}

// transferring keep chatting with client
func (wsr *wsRoute) transferring(con *Connection) {

	for {

		if con.rootCon.MaxPayloadBytes != wsr.payloadSize {
			con.rootCon.MaxPayloadBytes = wsr.payloadSize
		}

		payload, err := con.Read()

		// golang wss implement 2 type of EOF
		// 1. EOF end of stream -> loop :again
		// 2. EOF return by FrameReader -> Conn is closed
		// so no need to check for err=io.EOF
		if err != nil {

			wsr.removeCon(con, err.Error())
			if wsr.OnClose != nil {
				wsr.OnClose(con, err)
			}

			// if err = "use of closed network connection"
			// no need to call close, just remove the conn from the map[][]
			if !strings.Contains(err.Error(), "use of closed network connection") {
				con.Close()
			}

			return
		}

		// if has payload
		if len(payload) > 0 {
			if wsr.OnMessage != nil {
				wsr.OnMessage(con, payload)
			}
		}

	}
}

func (wsr *wsRoute) addCon(con *Connection) {

	// write lock
	wsr.Lock.Lock()
	defer wsr.Lock.Unlock()
	wsr.conMap[con.Id] = con

	if wsr.countConnection {
		fmtStr := fmt.Sprintf("host_id: (%s) conn_id: (%d) total: (%d) t_out: (%d)", wsr.instanceId, con.Id, len(wsr.conMap), con.timeout)
		fmt.Println(fmtStr)
	}
}

func (wsr *wsRoute) removeCon(con *Connection, reason string) {

	// write lock
	wsr.Lock.Lock()
	defer wsr.Lock.Unlock()

	delete(wsr.conMap, con.Id)

	if wsr.countConnection {
		fmtStr := fmt.Sprintf("removed_id: (%d) total: (%d) reason: (%s)", con.Id, len(wsr.conMap), reason)
		fmt.Println(fmtStr)
	}
}

func (wsr *wsRoute) GetConnectionMap() map[int]*Connection {

	// write lock
	wsr.Lock.RLock()
	defer wsr.Lock.RUnlock()

	return wsr.conMap
}

func (wsr *wsRoute) GetConnection(id int) *Connection {
	wsr.Lock.RLock()
	defer wsr.Lock.RUnlock()

	return wsr.conMap[id]
}

func (wsr *wsRoute) SetPayloadSize(size int) {
	wsr.payloadSize = size
}

func (wsr *wsRoute) SetCountConnection(isActive bool, instanceId string) {
	wsr.countConnection = isActive
	wsr.instanceId = instanceId
}
