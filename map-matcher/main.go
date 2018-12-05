package main

import (
	"encoding/json"
	"fmt"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-plugins/broker/nats"
	"log"
	"time"
)

var (
	natsURI            = "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	subscribeQueueName = "GoMicro_SimulatorData"
	publishQueueName   = "GoMicro_MapMatcher"
	globalBroker       broker.Broker
)

//Coordinates Struct to unite a Latitude and Longitude to one location
type Coordinates struct {
	Lat float32
	Lon float32
}

//SimulatorDataMessage Data received by the Simulator
type SimulatorDataMessage struct {
	MessageID int
	CarID     int
	Timestamp string
	Accuracy  int
	Lat       float32
	Lon       float32
}

func (s SimulatorDataMessage) toString() string {
	return fmt.Sprintf("%+v\n", s)
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

func main() {
	natsBroker := nats.NewBroker(broker.Addrs(natsURI))

	service := micro.NewService(
		micro.Name("mapmatcher"),
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
			var msg SimulatorDataMessage
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

func processMessage(msg SimulatorDataMessage) {
	msgData := MapMatcherMessage{
		MessageID: msg.MessageID,
		CarID:     msg.CarID,
		Timestamp: msg.Timestamp,
		Route: []Coordinates{
			Coordinates{
				Lat: msg.Lat,
				Lon: msg.Lon,
			},
		},
	}
	fmt.Printf("--- Output of Processing ---\n" + msgData.toString())
	publishMapMatcherMessage(msgData)
}

func publishMapMatcherMessage(msg MapMatcherMessage) {
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
