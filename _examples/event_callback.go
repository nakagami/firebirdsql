package main

import (
	"github.com/arteev/firebirdsql"
	"log"
	"math/rand"
)

func main() {
	dsn := "sysdba:masterkey@127.0.0.1/bar.fdb"
	events := []string{"my_event", "order_created"}
	fbEvent, err := firebirdsql.NewFBEvent(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer fbEvent.Close()

	sbr, err := fbEvent.Subscribe(events, func(event firebirdsql.Event) {
		log.Printf("event: %s, count: %d, id: %d, remote id:%d \n",
			event.Name, event.Count, event.ID, event.RemoteID)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sbr.Unsubscribe()

	go func() {
		for i := 0; i < 100; i++ {
			fbEvent.PostEvent(events[ rand.Intn(2)])
		}
	}()

	wait := make(chan struct{})
	<-wait
}
