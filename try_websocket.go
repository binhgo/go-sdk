package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/binhgo/go-sdk/sdk/websocket"
)

func main4() {

	wss := websocket.NewWebSocketServer("1")
	coord := wss.NewRoute("/")
	coord.OnConnected = func(conn *websocket.Connection) {
		fmt.Println("/ Connected #" + strconv.Itoa(conn.Id) + " => " + conn.GetIP() + " " + conn.GetUserAgent())
	}
	coord.OnMessage = func(conn *websocket.Connection, message string) {
		fmt.Println("/ Receive message from #" + strconv.Itoa(conn.Id) + " [" + message + "]")
	}
	coord.OnClose = func(conn *websocket.Connection, err error) {
		fmt.Println("/ Closed #" + strconv.Itoa(conn.Id) + " " + err.Error())
	}

	coord = wss.NewRoute("/hello")
	coord.OnConnected = func(conn *websocket.Connection) {
		fmt.Println("/hello Connected #" + strconv.Itoa(conn.Id))
		go (func() {
			time.Sleep(5 * time.Second)
			conn.Close()
		})()
	}
	coord.OnMessage = func(conn *websocket.Connection, message string) {
		fmt.Println("/hello Receive message from #" + strconv.Itoa(conn.Id) + " [" + message + "]")
	}
	coord.OnClose = func(conn *websocket.Connection, err error) {
		fmt.Println("/hello Closed #" + strconv.Itoa(conn.Id) + " " + err.Error())
	}

	go (func() {
		for _ = range time.Tick(5 * time.Second) {
			route := wss.GetRoute("/")
			if route != nil {
				conMap := route.GetConnectionMap()
				if conMap != nil {
					for id, con := range conMap {
						con.Send("Ping " + strconv.Itoa(id))
					}
				}
			}
		}

	})()

	wss.Expose(8080)
	wss.Start()
}
