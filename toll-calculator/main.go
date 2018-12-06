package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/go-nats"
	"log"
	"os"
	"time"

	"github.com/micro/go-micro"
)

var (
	natsURI            = os.Getenv("NATS_URI")
	subscribeQueueName = "GoMicro_PollutionMatcher"
	// publishQueueName   = "--- UNDEFINED ---"
	globalNatsConn *nats.Conn
)

//Coordinates Struct to unite a Latitude and Longitude to one location
type Coordinates struct {
	Lat float32
	Lon float32
}

//Segment is a polluted area and defined by a polygon between segment sections
type Segment struct {
	SegmentID       int
	PollutionLevel  int
	SegmentSections []Coordinates
}

//PollutionMatcherMessage Data the pollution matcher is sending after processing
type PollutionMatcherMessage struct {
	MessageID int
	CarID     int
	Timestamp string
	Segments  []Segment
}

func (m PollutionMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

func main() {

	service := micro.NewService(
		micro.Name("go.micro.tollcalculator"),
		micro.RegisterTTL(time.Second*30),
		micro.RegisterInterval(time.Second*10),
	)

	// optionally setup command line usage
	service.Init()

	nc, err := nats.Connect(natsURI)
	if err != nil {
		log.Fatal(err)
	}
	globalNatsConn = nc

	nc.Subscribe(subscribeQueueName, func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
		var msg PollutionMatcherMessage
		rawJSONMsg := json.RawMessage(m.Data)
		bytes, err := rawJSONMsg.MarshalJSON()
		if err != nil {
			panic(err)
		}
		err2 := json.Unmarshal(bytes, &msg)
		if err2 != nil {
			fmt.Println("error:", err)
		}

		processMessage(msg)

	})

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func processMessage(msg PollutionMatcherMessage) {
	// Calculate the fee based on the given PollutionMatcherMessage
	fmt.Print(msg.CarID)
	fmt.Printf("--- The Toll fee for the Car with the ID %v is 5.49 Euro ---\n", msg.CarID)
	fmt.Println(msg.toString())
	// publishMapMatcherMessage(msgData)
}

// func publishMapMatcherMessage(msg PollutionMatcherMessage) {
// 	msgDataJSON, err := json.Marshal(msg)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	messageToSend := broker.Message{
// 		Header: map[string]string{},
// 		Body:   msgDataJSON,
// 	}

// 	fmt.Printf("--- Data to Publish in Body---\n" + msg.toString())

// 	globalBroker.Publish(
// 		publishQueueName,
// 		&messageToSend,
// 	)

// 	fmt.Printf("--- Publishing process completed ---")
// }
