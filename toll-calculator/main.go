package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
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
	priceList          = map[int]int{
		1: 1,
		2: 2,
		3: 3,
		4: 4,
		5: 5,
		6: 6,
		7: 7,
		8: 8,
		9: 9,
	}
	pricesPerCar = make(map[int]float64)
)

//Coordinates Struct to unite a Latitude and Longitude to one location
type Coordinates struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

//Segment is a polluted area and defined by a polygon between segment sections
type Segment struct {
	SegmentID       int           `json:"segmentId"`
	PollutionLevel  int           `json:"pollutionLevel"`
	SegmentSections []Coordinates `json:"segmentSections"`
}

//PollutionMatcherMessage Data the pollution matcher is sending after processing
type PollutionMatcherMessage struct {
	MessageId int       `json:"MessageId"`
	CarId     int       `json:"CarId"`
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
	MessageId int    `json:"MessageId"`
	Sender    string `json:"sender"`
	Framework string `json:"framework"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
}

type TollcalculatorMessage struct {
	MessageId int
	CarId     int
	Timestamp string
	Toll      float64
	Sender    string `json:"sender"`
	Topic     string `json:"topic"`
}

func (m TollcalculatorMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

type TollcalculatorOutput struct {
	Data TollcalculatorMessage `json:"data"`
}

func (m TollcalculatorOutput) toString() string {
	return fmt.Sprintf("%+v\n", m)
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
		fmt.Printf("---Received a message:---\n%s\n", string(m.Data))
		var msg PollutionMatcherMessage
		rawJSONMsg := json.RawMessage(m.Data)
		bytes, err := rawJSONMsg.MarshalJSON()
		if err != nil {
			panic(err)
		}
		err2 := json.Unmarshal(bytes, &msg)
		if err2 != nil {
			fmt.Println("---error:---\n", err)
		}
		logMessage(msg.MessageId, "received")
		processMessage(msg)

	})

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func logMessage(MessageId int, msgType string) {
	msg := LogMessage{
		Data: LogMessageData{
			MessageId: MessageId,
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
	fmt.Printf("--- Message logged in queue %s ---\n", logQueueName)
	fmt.Println(string(logOutput))
}

func processMessage(msg PollutionMatcherMessage) {
	// Calculate the fee based on the given PollutionMatcherMessage
	fmt.Print(msg.CarId)
	fmt.Println(msg.toString())
	time.Sleep(2000 * time.Millisecond)
	logMessage(msg.MessageId, "sent")

	priceListSum := 0.0
	for _, seg := range msg.Segments {
		var sections = seg.SegmentSections
		distance := Distance(sections[0].Lat, sections[0].Lon, sections[1].Lat, sections[1].Lon)
		priceListSum += distance * float64(priceList[seg.PollutionLevel])

	}

	pricesPerCar[msg.CarId] += priceListSum

	msgData := TollcalculatorMessage{
		Sender:    "GoMicro-TollCalculator",
		Topic:     "toll.calculated",
		MessageId: msg.MessageId,
		CarId:     msg.CarId,
		Timestamp: time.Now().Local().Format(time.RFC3339),
		Toll:      pricesPerCar[msg.CarId],
	}

	publishTollCalculatorMessage(msgData)

}

func publishTollCalculatorMessage(msg TollcalculatorMessage) {
	outputMsg := TollcalculatorOutput{
		Data: msg,
	}

	msgDataJSON, err := json.Marshal(outputMsg)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn.Publish(publishQueueName, msgDataJSON)
	logMessage(msg.MessageId, " message sent")
	fmt.Println("---published message---\n" + msg.toString())
	fmt.Println("--- Publishing process completed ---")
}

// returns haversine distance in meters
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * r * math.Asin(math.Sqrt(h))
}

//this is called by *** distance(float64, float64, float64, float64) float64 *** do no call yourself, only works on rad
func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}
