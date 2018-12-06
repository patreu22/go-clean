package main

import (
	"encoding/json"
	"fmt"
	"github.com/micro/go-micro"
	"github.com/nats-io/go-nats"
	"log"
	"os"
	"time"
)

var (
	natsURI            = os.Getenv("NATS_URI")
	subscribeQueueName = "GoMicro_MapMatcher"
	publishQueueName   = "GoMicro_PollutionMatcher"
	globalNatsConn     *nats.Conn
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

	service := micro.NewService(
		micro.Name("go.micro.pollutionmatcher"),
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
		var msg MapMatcherMessage
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

	globalNatsConn.Publish(publishQueueName, msgDataJSON)

	fmt.Printf("--- Publishing process completed ---")
}
