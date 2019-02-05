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
	natsURI = os.Getenv("NATS_URI")
	osrmURI = os.Getenv("OSRM_URI")
	// "nats://nats:IoslProjec2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	// subscribeQueueName = "GoMicro_SimulatorData"
	subscribeQueueName = "location.update"
	// publishQueueName   = "LOL"
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

//SimulatorMessage received from the simulator queue
type SimulatorMessage struct {
	Event string               `json:"event"`
	Data  SimulatorMessageData `json:"data"`
}

//LogMessage to be put into the nats log queue
type LogMessage struct {
	Data LogMessageData `json:"data"`
}

//LogMessageData to be put into the LogMessage
type LogMessageData struct {
	MessageID int    `json:"messageId"`
	Sender    string `json:"sender"`
	Framework string `json:"framework"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
}

func (l LogMessage) toString() string {
	return fmt.Sprintf("%+v\n", l)
}

func (s SimulatorMessage) toString() string {
	return fmt.Sprintf("%+v\n", s)
}

func (s SimulatorMessageData) toString() string {
	return fmt.Sprintf("%+v\n", s)
}

// MapMatcherOutput message struct
type MapMatcherOutput struct {
	Sender string            `json:"sender"`
	Topic  string            `json:"topic"`
	Data   MapMatcherMessage `json:"data"`
}

// MapMatcherMessage struct
type MapMatcherMessage struct {
	MessageID int           `json:"messageId"`
	CarID     string        `json:"carId"`
	Timestamp string        `json:"timestamp"`
	Route     []Coordinates `json:"route"`
}

//Coordinates Struct to unite a Latitude and Longitude to one location
type Coordinates struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

func (m MapMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
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
		fmt.Printf("Received a message: %s\n", string(m.Data))
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
		fmt.Println(msg.toString())
		pushToMessageQueue(msg.Data)

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
	fmt.Printf("--- Message logged in queue %s ---", logQueueName)
	fmt.Printf(string(logOutput))
}

func processMessage(msg1 SimulatorMessageData, msg2 SimulatorMessageData) {
	fmt.Printf("sending data to osrm")
	fmt.Printf("----MESSAGE1----")
	fmt.Printf(msg1.toString())
	fmt.Printf("----MESSAGE2----")
	fmt.Printf(msg2.toString())
	fmt.Printf("LOL")
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
		fmt.Println("error:", err)
	}
	err2 := json.Unmarshal(bytes, &osrmRes)
	if err2 != nil {
		fmt.Println("error:", err)
	}

	fmt.Printf("--- OSRM output----\n")
	fmt.Println(osrmRes.toString())

	msgData := MapMatcherMessage{
		MessageID: msg1.MessageID,
		CarID:     msg1.CarID,
		Timestamp: time.Now().Local().Format(time.RFC3339),
		Route: []Coordinates{
			Coordinates{
				Lat: osrmRes.Tracepoints[0].Location[0],
				Lon: osrmRes.Tracepoints[0].Location[1],
			},
		},
	}

	mmOutput := MapMatcherOutput{
		Sender: "GoMicro-MapMatcher",
		Topic:  "location-matched",
		Data:   msgData,
	}

	// fmt.Printf("--- Output of Processing ---\n" + msgData.toString())
	publishMapMatcherMessage(mmOutput)
}

func publishMapMatcherMessage(msg MapMatcherOutput) {
	mmOutput, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn.Publish(publishQueueName, mmOutput)
	logMessage(msg.Data.MessageID, "sent")

	fmt.Printf("--- Publishing process completed ---")
}
