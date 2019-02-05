package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	micro "github.com/micro/go-micro"
	nats "github.com/nats-io/go-nats"
)

var (
	natsURI            = os.Getenv("NATS_URI")
	subscribeQueueName = "location.matched"
	publishQueueName   = "pollution.matched"
	globalNatsConn     *nats.Conn
	logQueueName       = "logs"
)

//Coordinates Struct to unite a Latitude and Longitude to one location
type Coordinates struct {
	Lat float32 `json:"lat"`
	Lon float32 `json:"lon"`
}

//Segment is a polluted area and defined by a polygon between segment sections
type Segment struct {
	SegmentID       int           `json:"segmentId"`
	PollutionLevel  int           `json:"pollutionLevel"`
	SegmentSections []Coordinates `json:"segmentSections"`
}

//MapMatcherMessage the message the mapmatcher processes via NATs
type MapMatcherMessage struct {
	Sender string                `json:"sender"`
	Topic  string                `json:"topic"`
	Data   MapMatcherMessageData `json:"data"`
}

//MapMatcherMessageData the map matcher data
type MapMatcherMessageData struct {
	MessageID int           `json:"messageId"`
	CarID     int           `json:"carId"`
	Timestamp string        `json:"timestamp"`
	Route     []Coordinates `json:"route"`
}

//LogMessage to be put into the log queue
type LogMessage struct {
	Data LogMessageData `json:"data"`
}

//LogMessageData which is part of the LogMessage
type LogMessageData struct {
	MessageID int    `json:"messageId"`
	Sender    string `json:"sender"`
	Framework string `json:"framework"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
}

func (m MapMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

func (m MapMatcherMessageData) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

//PollutionMatcherMessage Data the pollution matcher is sending after processing
type PollutionMatcherMessage struct {
	MessageID int       `json:"messageId"`
	CarID     int       `json:"carId"`
	Timestamp string    `json:"timestamp"`
	Segments  []Segment `json:"segments"`
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
		fmt.Println("Marshalled message")
		fmt.Println("\n\n\n")
		fmt.Println(msg.Data.toString() + "\n\n\n")
		logMessage(msg.Data.MessageID, "received")
		processMessage(msg.Data)

	})

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func logMessage(messageID int, msgType string) {
	msg := LogMessage{
		Data: LogMessageData{
			MessageID: messageID,
			Sender:    "pollution-matcher",
			Framework: "gomicro",
			Type:      msgType,
			Timestamp: time.Now().Local().Format(time.RFC3339),
		},
	}

	logOutput, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn.Publish(logQueueName, logOutput)
	fmt.Printf("--- Message logged in queue %s ---:\n\n", logQueueName)
	fmt.Println("\n\n")
	fmt.Println(string(logOutput))
	fmt.Println("\n\n")
	fmt.Println("--- Message logged in queue ---")
}

func processMessage(msg MapMatcherMessageData) {
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
	publishPollutionMatcherMessage(msgData)
}

func publishPollutionMatcherMessage(msg PollutionMatcherMessage) {
	msgDataJSON, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn.Publish(publishQueueName, msgDataJSON)
	fmt.Println("Freeze...")
	time.Sleep(2 * time.Second)
	fmt.Println("Continue...")
	logMessage(msg.MessageID, "sent")
	fmt.Printf("--- Publishing process completed ---")
}
