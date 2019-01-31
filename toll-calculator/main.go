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
	subscribeQueueName = "pollution.matched"
	logQueueName       = "logs"
	publishQueueName   = "toll.calculated"
	globalNatsConn     *nats.Conn
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

//LogMessage to be put into the log queue
type LogMessage struct {
	Data LogMessageData `json:"data"`
}

//LogMessageData which is part of the LogMessage
type LogMessageData struct {
	MessageId int    `json:"messageId"`
	Sender    string `json:"sender"`
	Framework string `json:"framework"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
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
		logMessage(msg.MessageID, "received")
		processMessage(msg)

	})

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func logMessage(messageID int, msgType string) {
	msg := LogMessage{
		Data: LogMessageData{
			MessageId: messageID,
			Sender:    "toll-calculator",
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

func processMessage(msg PollutionMatcherMessage) {
	// Calculate the fee based on the given PollutionMatcherMessage
	fmt.Print(msg.CarID)
	fmt.Printf("--- The Toll fee for the Car with the ID %v is 5.49 Euro ---\n", msg.CarID)
	fmt.Println(msg.toString())
	// publishMapMatcherMessage(msgData)
	time.Sleep(2000 * time.Millisecond)
	logMessage(msg.MessageID, "sent")
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
