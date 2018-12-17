package main

import (
	"encoding/json"
	"fmt"
	"github.com/micro/go-micro"
	"github.com/nats-io/go-nats"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

var (
	natsURI = os.Getenv("NATS_URI")
	osrmURI = os.Getenv("OSRM_URI")
	// "nats://nats:IoslProjec2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	// subscribeQueueName = "GoMicro_SimulatorData"
	subscribeQueueName = "MOL-iosl2018.EVENTB.toll-simulator.location.update"
	publishQueueName   = "GoMicro_MapMatcher"
	globalNatsConn     *nats.Conn
	messageQueue       = make(map[string][]SimulatorMessageData) // car id to locations dict; example id:locations:[.., .., .., ]
	messageQueueLength = 2
)

//SimulatorMessage Data received by the Simulator
type SimulatorMessageData struct {
	MessageId int
	CarId     string
	Timestamp string
	Accuracy  float64
	Lat       float64 `json:",float64"`
	Long      float64 `json:",float64"`
}

type SimulatorMessage struct {
	Event string
	Data  SimulatorMessageData
}

func (s SimulatorMessage) toString() string {
	return fmt.Sprintf("%+v\n", s)
}

//MapMatcherMessage Data the map matcher is sending after processing

type MapMatcherOutput struct {
	Sender string
	Topic  string
	Data   MapMatcherMessage
}
type MapMatcherMessage struct {
	MessageID int
	CarID     string
	Timestamp string
	Route     []Coordinates
}

//Coordinates Struct to unite a Latitude and Longitude to one location
type Coordinates struct {
	Lat  float64
	Long float64
}

func (m MapMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

type OSRMResponse struct {
	Code        string
	Tracepoints []Tracepoints
}

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

	messageQueue[ms.CarId] = append(messageQueue[ms.CarId], ms)

	if len(messageQueue[ms.CarId]) >= messageQueueLength {
		msg1 := messageQueue[ms.CarId][len(messageQueue[ms.CarId])-1]
		msg2 := messageQueue[ms.CarId][len(messageQueue[ms.CarId])-2]
		messageQueue[ms.CarId] = messageQueue[ms.CarId][:len(messageQueue[ms.CarId])-1]
		messageQueue[ms.CarId] = messageQueue[ms.CarId][:len(messageQueue[ms.CarId])-1]
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
			panic(err)
		}
		err2 := json.Unmarshal(bytes, &msg)
		if err2 != nil {
			fmt.Println("error:", err)
		}
		fmt.Println(msg.toString())

		pushToMessageQueue(msg.Data)

	})

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func processMessage(msg1 SimulatorMessageData, msg2 SimulatorMessageData) {
	fmt.Printf("sending data to osrm")

	resp, err := http.Get("http://" + osrmURI + "/match/v1/car/" + strconv.FormatFloat(msg1.Long, 'f', -1, 64) + "," + strconv.FormatFloat(msg1.Lat, 'f', -1, 64) + ";" + strconv.FormatFloat(msg2.Long, 'f', -1, 64) + "," + strconv.FormatFloat(msg2.Lat, 'f', -1, 64) + "?radiuses=100.0;100.0" /*strconv.FormatFloat(msg1.Accuracy, 'f', -1, 64)*/ /* + strconv.FormatFloat(msg2.Accuracy, 'f', -1, 64)*/)
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
		panic(err)
	}
	err2 := json.Unmarshal(bytes, &osrmRes)
	if err2 != nil {
		fmt.Println("error:", err)
	}

	fmt.Printf("--- OSRM output----\n")
	fmt.Println(osrmRes.toString())

	msgData := MapMatcherMessage{
		MessageID: msg1.MessageId,
		CarID:     msg1.CarId,
		Timestamp: time.Now().Local().Format("2000-01-02 07:55:31"),
		Route: []Coordinates{
			Coordinates{
				Lat:  osrmRes.Tracepoints[0].Location[0],
				Long: osrmRes.Tracepoints[0].Location[1],
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

	fmt.Printf("--- Publishing process completed ---")
}
