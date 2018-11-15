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
	subscribeQueueName = "GoMicro_MapMatcher"
	publishQueueName   = "GoMicro_PollutionMatcher"
	globalBroker       broker.Broker
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

//MapMatcherMessage Data the map matcher is sending after processing
type MapMatcherMessage struct {
	MessageID int
	CarID     int
	Timestamp string
	Route     []Coordinates
}

func (m MapMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
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
		micro.Name("go.micro.pollutionmatcher"),
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
			var msg MapMatcherMessage
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

func processMessage(msg MapMatcherMessage) {
	//Get the Segments from some API/Service/Whereever
	msgData := PollutionMatcherMessage{
		MessageID: msg.MessageID,
		CarID:     msg.CarID,
		Timestamp: msg.Timestamp,
		Segments: []Segment{
			Segment{
				SegmentID: 0,
				SegmentSections: []Coordinates{
					Coordinates{
						Lat: 2.35,
						Lon: 3.45,
					},
					Coordinates{
						Lat: 3.56,
						Lon: 3.89,
					},
				},
			},
		},
	}
	fmt.Printf("--- Output of Processing ---\n" + msgData.toString())
	publishMapMatcherMessage(msgData)
}

func publishMapMatcherMessage(msg PollutionMatcherMessage) {
	msgDataJSON, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	messageToSend := broker.Message{
		Header: map[string]string{},
		Body:   msgDataJSON,
	}

	fmt.Printf("--- Data to Publish in Body---\n" + msg.toString())

	globalBroker.Publish(
		publishQueueName,
		&messageToSend,
	)

	fmt.Printf("--- Publishing process completed ---")
}
