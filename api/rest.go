package main

import (
	"encoding/json"
	"fmt"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-plugins/broker/nats"
	"github.com/micro/go-web"
	"log"
)

var (
	natsServerAddress   = "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	natsRawSimDataQueue = "GoMicro_SimulatorData"
	globalBroker        broker.Broker
)

//SimulatorAPI : Used to PushData
type SimulatorAPI struct{}

//SimulatorDataMessage comment
type SimulatorDataMessage struct {
	MessageID int
	CarID     int
	Timestamp string
	Accuracy  int
	Lat       float32
	Lon       float32
}

//PushData : post mockup simulation data via rest API
func (s *SimulatorAPI) PushData(req *restful.Request, rsp *restful.Response) {
	log.Print("Received SimulatorApi.PushData API request")
	msgData := SimulatorDataMessage{
		MessageID: 1,
		CarID:     2,
		Timestamp: "yyyy-mm-dd hh:MM:ss",
		Accuracy:  3,
		Lat:       1.23,
		Lon:       2.34,
	}
	msgDataJSON, err := json.Marshal(msgData)
	if err != nil {
		log.Fatal(err)
	}

	msg := broker.Message{
		Header: map[string]string{},
		Body:   msgDataJSON,
	}

	globalBroker.Publish(natsRawSimDataQueue, &msg)
	fmt.Printf("Published")

	rsp.WriteEntity(map[string]string{
		"message": "Pushed mockup dimulation data to NATS queue",
	})
}

func main() {
	natsBroker := nats.NewBroker(broker.Addrs(natsServerAddress))

	// Create service
	service := web.NewService(
		web.Name("apimockup"),
	)

	service.Init()
	natsBroker.Connect()
	globalBroker = natsBroker

	// Create RESTful handler
	simAPI := new(SimulatorAPI)
	ws := new(restful.WebService)
	wc := restful.NewContainer()
	ws.Consumes(restful.MIME_XML, restful.MIME_JSON)
	ws.Produces(restful.MIME_JSON, restful.MIME_XML)
	ws.Path("/simulator")
	ws.Route(ws.POST("/").To(simAPI.PushData))
	wc.Add(ws)

	// Register Handler
	service.Handle("/", wc)

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}
