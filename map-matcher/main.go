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
	"time"
)

var (
	natsURI = os.Getenv("NATS_URI")
	// "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	subscribeQueueName = "GoMicro_SimulatorData"
	publishQueueName   = "GoMicro_MapMatcher"
	messageQueue       = make(map[int][]SimulatorDataMessage) // car id to locations dict; example id:locations:[.., .., .., ]
	globalNatsConn     *nats.Conn
)

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

//Coordinates Struct to unite a Latitude and Longitude to one location
type Coordinates struct {
	Lat float32
	Lon float32
}
type CoordinatesOSRM struct {
	Lon float32
	Lat float32
}

func (m MapMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

type OSRMResponse struct {
	Code        string
	Tracepoints []Tracepoints
}

type Tracepoints struct {
	alternativesCount int
	location          []CoordinatesOSRM // OSRM returns reversed coordinates
	hint              string
	name              string
	matchinIndex      int
	waypointIndex     int
}

func (r OSRMResponse) toString() string {
	return fmt.Sprintf("%+v\n", r)
}

func pushToMessageQueue(ms SimulatorDataMessage) {

	messageQueue[ms.CarID] = append(messageQueue[ms.CarID], ms)

	fmt.Println(messageQueue[ms.CarID])

	if len(messageQueue[ms.CarID]) >= 2 {
		msg1 := messageQueue[ms.CarID][len(messageQueue[ms.CarID])-1]
		msg2 := messageQueue[ms.CarID][len(messageQueue[ms.CarID])-2]
		messageQueue[ms.CarID] = messageQueue[ms.CarID][:len(messageQueue[ms.CarID])-1]
		messageQueue[ms.CarID] = messageQueue[ms.CarID][:len(messageQueue[ms.CarID])-2]
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
		var msg SimulatorDataMessage
		rawJSONMsg := json.RawMessage(m.Data)
		bytes, err := rawJSONMsg.MarshalJSON()
		if err != nil {
			panic(err)
		}
		err2 := json.Unmarshal(bytes, &msg)
		if err2 != nil {
			fmt.Println("error:", err)
		}

		pushToMessageQueue(msg)

	})

	// globalNatsConn.Publish(subscribeQueueName, []byte("{stuff: stuff}"))

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func processMessage(msg1 SimulatorDataMessage, msg2 SimulatorDataMessage) {
	// msgData := MapMatcherMessage{
	// 	MessageID: msg.MessageID,
	// 	CarID:     msg.CarID,
	// 	Timestamp: msg.Timestamp,
	// 	Route: []Coordinates{
	// 		Coordinates{
	// 			Lat: msg.Lat,
	// 			Lon: msg.Lon,
	// 		},
	// 	},
	// }

	resp, err := http.Get("http://localhost:5000/match/v1/car/" + fmt.Sprintf("%f", msg1.Lat) + "," + fmt.Sprintf("%f", msg1.Lon) + ";" + fmt.Sprintf("%f", msg2.Lat) + "," + fmt.Sprintf("%f", msg2.Lon))
	if err != nil {
		fmt.Printf("--- OSRM error----\n")
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

	fmt.Println(osrmRes.toString())
	// msgData := MapMatcherMessage{
	// 	MessageID: msg1.MessageID,
	// 	CarID:     msg1.CarID,
	// 	Timestamp: msg1.Timestamp,
	// 	Route: []Coordinates{
	// 		Coordinates{
	// 			Lat: msg1.Lat,
	// 			Lon: msg1.Lon,
	// 		},
	// 	},
	// }
	fmt.Printf("--- OSRM output----\n")

	// fmt.Printf("--- Output of Processing ---\n" + msgData.toString())
	// publishMapMatcherMessage(msgData)
}

func publishMapMatcherMessage(msg MapMatcherMessage) {
	msgDataJSON, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn.Publish(publishQueueName, msgDataJSON)

	fmt.Printf("--- Data to Publish in Body---\n" + msg.toString())

	fmt.Printf("--- Publishing process completed ---")
}
