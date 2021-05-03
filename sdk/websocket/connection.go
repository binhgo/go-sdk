package websocket

import (
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

// Connection represent a ws connection
type Connection struct {
	Id       int
	Attached map[string]interface{}
	Key      string

	rootCon  *websocket.Conn
	isActive bool
	lock     *sync.Mutex

	timeout      int
	sentMsgCount int
	readMsgCount int
	lastWrite    *time.Time
	lastRead     *time.Time
}

func newConnection(id int, conn *websocket.Conn, timeout int) *Connection {
	con := &Connection{
		Id:       id,
		rootCon:  conn,
		Attached: make(map[string]interface{}),
		isActive: true,
		lock:     &sync.Mutex{},
		timeout:  timeout,
	}
	return con
}

// getRooCon return root
func (con *Connection) getRootCon() *websocket.Conn {
	return con.rootCon
}

// Send push message to client side
func (con *Connection) Send(message string) error {

	// no need, hijacked already take the time from http serve
	// if con.timeout > 0 {
	// 	t := time.Now().Add(time.Duration(con.timeout) * time.Second)
	// 	con.rootCon.SetReadDeadline(t)
	// 	con.rootCon.SetWriteDeadline(t)
	// }
	// end

	n, err := con.rootCon.Write([]byte(message))
	if err == nil && n > 0 {
		con.lock.Lock()
		defer con.lock.Unlock()

		con.sentMsgCount++
	}

	return err
}

// Read push message to client side
func (con *Connection) Read() (string, error) {

	var msg = make([]byte, con.rootCon.MaxPayloadBytes)

	n, err := con.rootCon.Read(msg)
	if err == nil && n > 0 {
		con.lock.Lock()
		defer con.lock.Unlock()

		con.readMsgCount++
	}
	return string(msg[:n]), err
}

// IsActive check status of connection
func (con *Connection) IsActive() bool {
	return con.isActive
}

// CLose close the connection
func (con *Connection) Close() {
	con.isActive = false
	con.rootCon.Close()
}

// Deactive deactive status of connection
func (con *Connection) Deactive() {
	con.isActive = false
}

func (con *Connection) GetIP() string {
	adr := con.rootCon.Request().RemoteAddr
	n := strings.LastIndexAny(adr, ":")
	if n > 0 {
		return adr[:n]
	}

	return adr
}

func (con *Connection) GetUserAgent() string {
	hdr := con.rootCon.Request().Header
	if hdr != nil {
		return hdr.Get("user-agent")
	}
	return ""
}
