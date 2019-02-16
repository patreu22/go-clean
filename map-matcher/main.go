package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	micro "github.com/micro/go-micro"
	nats "github.com/nats-io/go-nats"
)

var (
	natsURI            = os.Getenv("NATS_URI")
	osrmURI            = os.Getenv("OSRM_URI")
	subscribeQueueName = "location.update"
	publishQueueName   = "location.matched"
	logQueueName       = "logs"
	globalNatsConn     *nats.Conn
	messageQueue       = make(map[string][]SimulatorMessageData) // car id to locations dict; example id:locations:[.., .., .., ]
	messageQueueLength = 2
)

//SimulatorMessageData received by the Simulator
type SimulatorMessageData struct {
	MessageID int     `json:"messageId"`
	CarID     string  `json:"carId"`
	Timestamp string  `json:"timestamp"`
	Accuracy  float64 `json:"accuracy"`
	Lat       float64 `json:"lat,float64"`
	Lon       float64 `json:"lon,float64"`
}

func (s SimulatorMessageData) toString() string {
	return fmt.Sprintf("%+v\n", s)
}

//SimulatorMessage received from the simulator queue
type SimulatorMessage struct {
	Event string               `json:"event"`
	Data  SimulatorMessageData `json:"data"`
}

func (s SimulatorMessage) toString() string {
	return fmt.Sprintf("%+v\n", s)
}

//LogMessage to be put into the nats log queue
type LogMessage struct {
	Data LogMessageData `json:"data"`
}

func (l LogMessage) toString() string {
	return fmt.Sprintf("%+v\n", l)
}

//LogMessageData to be put into the LogMessage
type LogMessageData struct {
	MessageID int    `json:"messageId"`
	Sender    string `json:"sender"`
	Framework string `json:"framework"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
}

// MapMatcherOutput message struct
type MapMatcherOutput struct {
	Data MapMatcherMessage `json:"data"`
}

func (m MapMatcherOutput) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

// MapMatcherMessage struct
type MapMatcherMessage struct {
	MessageID int           `json:"messageId"`
	CarID     string        `json:"carId"`
	Timestamp string        `json:"timestamp"`
	Route     []Coordinates `json:"route"`
	Sender    string        `json:"sender"`
	Topic     string        `json:"topic"`
}

func (m MapMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

//Coordinates Struct to unite a Latitude and Longitude to one location
type Coordinates struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// OSRMResponse from the OSRM server
type OSRMResponse struct {
	Code        string
	Tracepoints []Tracepoints
}

// Tracepoints <meaningful comment>
type Tracepoints struct {
	AternativesCount int
	Location         []float64
	Distance         float64
	Hint             string
	Name             string
	MatchinIndex     int
	WaypointIndex    int
}

func (r OSRMResponse) toString() string {
	return fmt.Sprintf("%+v\n", r)
}

func pushToMessageQueue(ms SimulatorMessageData) {

	messageQueue[ms.CarID] = append(messageQueue[ms.CarID], ms)

	if len(messageQueue[ms.CarID]) >= messageQueueLength {
		msg1 := messageQueue[ms.CarID][len(messageQueue[ms.CarID])-1]
		msg2 := messageQueue[ms.CarID][len(messageQueue[ms.CarID])-2]
		messageQueue[ms.CarID] = messageQueue[ms.CarID][:len(messageQueue[ms.CarID])-1]
		messageQueue[ms.CarID] = messageQueue[ms.CarID][:len(messageQueue[ms.CarID])-1]
		processMessage(msg1, msg2)
	}

}

func main() {
	service := micro.NewService(
		micro.Name("mapmatcher"),
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
		fmt.Println("---Received a message:---\n", string(m.Data))
		var msg SimulatorMessage
		rawJSONMsg := json.RawMessage(m.Data)
		bytes, err := rawJSONMsg.MarshalJSON()
		if err != nil {
			fmt.Println(err)
		}
		err2 := json.Unmarshal(bytes, &msg)
		if err2 != nil {
			fmt.Println("error:", err)
		}
		logMessage(msg.Data.MessageID, "received")
		pushToMessageQueue(msg.Data)

	})

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func logMessage(MessageID int, msgType string) {
	msg := LogMessage{
		Data: LogMessageData{
			MessageID: MessageID,
			Sender:    "map-matcher",
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
	fmt.Printf("--- Message logged in queue %s ---\n", logQueueName)
	fmt.Println(string(logOutput))
}

func processMessage(msg1 SimulatorMessageData, msg2 SimulatorMessageData) {
	fmt.Println("----MESSAGE1----")
	fmt.Println(msg1.toString())
	fmt.Println("----MESSAGE2----")
	fmt.Println(msg2.toString())
	fmt.Println("---sending Data to osrm---")
	resp, err := http.Get("http://" + osrmURI + "/match/v1/car/" + strconv.FormatFloat(msg1.Lon, 'f', -1, 64) + "," + strconv.FormatFloat(msg1.Lat, 'f', -1, 64) + ";" + strconv.FormatFloat(msg2.Lon, 'f', -1, 64) + "," + strconv.FormatFloat(msg2.Lat, 'f', -1, 64) + "?radiuses=100.0;100.0" /*strconv.FormatFloat(msg1.Accuracy, 'f', -1, 64)*/ /* + strconv.FormatFloat(msg2.Accuracy, 'f', -1, 64)*/)
	if err != nil {
		fmt.Printf("--- OSRM error!----\n")
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	var osrmRes OSRMResponse
	rawJSON := json.RawMessage(body)
	bytes, err := rawJSON.MarshalJSON()
	if err != nil {
		fmt.Println("---error:---\n", err)
		return
	}
	err2 := json.Unmarshal(bytes, &osrmRes)
	if err2 != nil {
		fmt.Println("---error:---\n", err)
		return
	}

	fmt.Printf("--- OSRM output----\n")
	fmt.Println(osrmRes.toString())

	msgData := MapMatcherMessage{
		Sender:    "GoMicro-MapMatcher",
		Topic:     "location.matched",
		MessageID: msg1.MessageID,
		CarID:     msg1.CarID,
		Timestamp: time.Now().Local().Format(time.RFC3339),
		Route: []Coordinates{
			Coordinates{
				Lat: osrmRes.Tracepoints[0].Location[1],
				Lon: osrmRes.Tracepoints[0].Location[0],
			},
			Coordinates{
				Lat: osrmRes.Tracepoints[1].Location[1],
				Lon: osrmRes.Tracepoints[1].Location[0],
			},
		},
	}

	mmOutput := MapMatcherOutput{
		Data: msgData,
	}

	publishMapMatcherMessage(mmOutput)
}

func publishMapMatcherMessage(msg MapMatcherOutput) {
	mmOutput, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn.Publish(publishQueueName, mmOutput)
	logMessage(msg.Data.MessageID, "sent")
	fmt.Println("---published message---\n" + msg.toString())
	fmt.Println("--- Publishing process completed --- \n")
}
