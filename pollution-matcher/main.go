package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"time"

	micro "github.com/micro/go-micro"
	nats "github.com/nats-io/go-nats"
)

var (
	natsURI            = os.Getenv("NATS_URI")
	connStr            = os.Getenv("PG_URI")
	subscribeQueueName = "location.matched"
	publishQueueName   = "pollution.matched"
	globalNatsConn     *nats.Conn
	logQueueName       = "logs"
	globalDbConn       *sql.DB
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

//MapMatcherMessage the message the mapmatcher processes via NATs
type MapMatcherMessage struct {
	Sender string                `json:"sender"`
	Topic  string                `json:"topic"`
	Data   MapMatcherMessageData `json:"data"`
}

func (m MapMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

//MapMatcherMessageData the map matcher data
type MapMatcherMessageData struct {
	MessageID int           `json:"messageId"`
	CarID     int           `json:"carId"`
	Timestamp string        `json:"timestamp"`
	Route     []Coordinates `json:"route"`
}

func (m MapMatcherMessageData) toString() string {
	return fmt.Sprintf("%+v\n", m)
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

//PollutionMatcherMessage Data the pollution matcher is sending after processing
type PollutionMatcherMessage struct {
	MessageID int       `json:"messageId"`
	CarID     int       `json:"carId"`
	Timestamp string    `json:"timestamp"`
	Segments  []Segment `json:"segments"`
	Sender    string    `json:"sender"`
	Topic     string    `json:"topic"`
}

func (m PollutionMatcherMessage) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

type PollutionMatcherOutput struct {
	Data PollutionMatcherMessage `json:"data"`
}

func (m PollutionMatcherOutput) toString() string {
	return fmt.Sprintf("%+v\n", m)
}

//GEOJSON Data from postgis

type PGData struct {
	pollution   int
	coordinates []string
}

func main() {

	service := micro.NewService(
		micro.Name("go-micro-pollutionmatcher"),
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

	time.Sleep(20 * time.Second)

	db, err := sql.Open("postgres", connStr)
	checkErr(err)
	globalDbConn = db

	nc.Subscribe(subscribeQueueName, func(m *nats.Msg) {
		fmt.Printf("---Received a message:---\n%s\n", string(m.Data))
		var msg MapMatcherMessage
		rawJSONMsg := json.RawMessage(m.Data)
		bytes, err := rawJSONMsg.MarshalJSON()
		if err != nil {
			fmt.Println(err)
		}
		err2 := json.Unmarshal(bytes, &msg)
		if err2 != nil {
			fmt.Println("---error:---\n", err)
		}
		fmt.Println("---Marshalled message---")
		fmt.Println(msg.Data.toString())
		logMessage(msg.Data.MessageID, "received")
		processMessage(msg.Data)

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
	fmt.Printf("--- Message logged in queue %s ---:\n", logQueueName)
	fmt.Println(string(logOutput))
	fmt.Println("\n")
}

func processMessage(msg MapMatcherMessageData) {
	//Get the Segments from some API/Service/Whereever

	rows, err := globalDbConn.Query("select  ST_AsGeoJson(ST_intersection (a.outline, ST_GeomFromText('LINESTRING(" + strconv.FormatFloat(msg.Route[0].Lon, 'f', -1, 64) + " " + strconv.FormatFloat(msg.Route[0].Lat, 'f', -1, 64) + ", " + strconv.FormatFloat(msg.Route[1].Lon, 'f', -1, 64) + " " + strconv.FormatFloat(msg.Route[1].Lat, 'f', -1, 64) + ")', 4326))) as geometry, a.pollution FROM berlin_polygons a WHERE not ST_IsEmpty(ST_AsText(ST_intersection (a.outline, ST_GeomFromText('LINESTRING(" + strconv.FormatFloat(msg.Route[0].Lon, 'f', -1, 64) + " " + strconv.FormatFloat(msg.Route[0].Lat, 'f', -1, 64) + ", " + strconv.FormatFloat(msg.Route[1].Lon, 'f', -1, 64) + " " + strconv.FormatFloat(msg.Route[1].Lat, 'f', -1, 64) + ")',4326))));")
	if err != nil {
		fmt.Println("----db query error----")
		fmt.Println(err)
	}

	var pgDataPoints []PGData

	for rows.Next() { //evaluate|extract db result
		var pol int
		var str string
		err = rows.Scan(&str, &pol)
		if err != nil {
			fmt.Println("---row scan error---")
			fmt.Println(err)
		}

		re := regexp.MustCompile("[0-9]+.[0-9]+")
		res := re.FindAllString(str, -1)

		pgData := PGData{
			pollution:   pol,
			coordinates: res,
		}

		pgDataPoints = append(pgDataPoints, pgData)
	}
	var segments []Segment
	for _, point := range pgDataPoints {
		if len(point.coordinates) < 1 {
			return
		}
		latOne, err := strconv.ParseFloat(point.coordinates[0], 64)
		lonOne, err2 := strconv.ParseFloat(point.coordinates[1], 64)
		latTwo, err3 := strconv.ParseFloat(point.coordinates[2], 64)
		lonTwo, err4 := strconv.ParseFloat(point.coordinates[3], 64)

		if err != nil || err2 != nil || err3 != nil || err4 != nil {
			fmt.Println("---float parsing error in segment generation---")
		}
		segment := Segment{

			SegmentID:      rand.Intn(10000000000),
			PollutionLevel: point.pollution,
			SegmentSections: []Coordinates{
				Coordinates{
					Lat: latOne,
					Lon: lonOne,
				},
				Coordinates{
					Lat: latTwo,
					Lon: lonTwo,
				},
			},
		}
		segments = append(segments, segment)
	}

	msgData := PollutionMatcherMessage{
		Topic:     "pollution.matched",
		Sender:    "GoMicro-PollutionMatcher",
		MessageID: msg.MessageID,
		CarID:     msg.CarID,
		Timestamp: time.Now().Local().Format(time.RFC3339),
		Segments:  segments,
	}
	publishPollutionMatcherMessage(msgData)
}

func publishPollutionMatcherMessage(msg PollutionMatcherMessage) {

	outputMsg := PollutionMatcherOutput{
		Data: msg,
	}

	msgDataJSON, err := json.Marshal(outputMsg)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn.Publish(publishQueueName, msgDataJSON)
	logMessage(msg.MessageID, "sent")
	fmt.Println("---published message---\n" + msg.toString())
	fmt.Println("--- Publishing process completed ---")
}

func checkErr(err error) {
	if err != nil {
		fmt.Print(err)
	}
}
