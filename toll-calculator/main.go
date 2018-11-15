package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/micro/go-micro"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-plugins/broker/nats"
)

var (
	natsURI            = "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	subscribeQueueName = "GoMicro_PollutionMatcher"
	// publishQueueName   = "--- UNDEFINED ---"
	globalBroker broker.Broker
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
	natsBroker := nats.NewBroker(broker.Addrs(natsURI))

	service := micro.NewService(
		micro.Name("go.micro.tollcalculator"),
		micro.RegisterTTL(time.Second*30),
		micro.RegisterInterval(time.Second*10),
	)

	// optionally setup command line usage
	service.Init()

	//Connect to Nats
	natsBroker.Connect()
	natsBroker.Subscribe(
		subscribeQueueName,
		broker.Handler(func(p broker.Publication) error {
			var msgBody = p.Message().Body
			var msg PollutionMatcherMessage
			rawJSONMsg := json.RawMessage(msgBody)
			bytes, err := rawJSONMsg.MarshalJSON()
			if err != nil {
				panic(err)
			}
			err2 := json.Unmarshal(bytes, &msg)
			if err2 != nil {
				fmt.Println("error:", err)
			}
			fmt.Println("--- RECEIVED ---\n" + msg.toString())
			processMessage(msg)
			return nil
		}),
	)

	globalBroker = natsBroker

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
