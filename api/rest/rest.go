package main

import (
	"log"
	// "context"
	"encoding/json"
	"fmt"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-plugins/broker/nats"
	"github.com/micro/go-web"
	// "github.com/nats-io/go-nats"
)

var (
	// natsConn            *nats.Conn
	natsServerAddress   string = "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	natsRawSimDataQueue string = "GoMicro_SimulatorData"
)

type SimulatorApi struct{}
type MockupData struct {
	MessageId int
	CarId     int
	Timestamp string
	Accuracy  int
	Lat       float32
	Lon       float32
}

//post mockup simulation data via rest API
func (s *SimulatorApi) PushData(req *restful.Request, rsp *restful.Response) {
	log.Print("Received SimulatorApi.PushData API request")

	// natsConn.Subscribe(natsRawSimDataQueue, func(m *nats.Msg) {
	// 	fmt.Printf("Received a message: %s\n", string(m.Data))
	// })

	// mockUp := MockupData{
	// 	MessageId: 1,
	// 	CarId:     1,
	// 	Timestamp: "xx-xx-xxx",
	// 	Accuracy:  1,
	// 	Lat:       1.23,
	// 	Lon:       2.34,
	// }
	// mockUpJson, err := json.Marshal(mockUp)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// push data to NATS Queue
	// natsConn.Publish(natsRawSimDataQueue, mockUpJson)
	// brokershared.Publish(natsRawSimDataQueue, mockUpJson)

	rsp.WriteEntity(map[string]string{
		"message": "pushed mockup dimulation data to NATS queue",
	})
}

func main() {
	natsBroker := nats.NewBroker(broker.Addrs(natsServerAddress))

	// Create service
	service := web.NewService(
		web.Name("go.micro.api.mockup"),
	)

	service.Init()
	natsBroker.Connect()
	mockUp := MockupData{
		MessageId: 1,
		CarId:     1,
		Timestamp: "xx-xx-xxx",
		Accuracy:  1,
		Lat:       1.23,
		Lon:       2.34,
	}
	mockUpJson, err := json.Marshal(mockUp)
	if err != nil {
		log.Fatal(err)
	}

	msg := broker.Message{
		Header: map[string]string{},
		Body:   mockUpJson,
	}

	natsBroker.Publish(natsRawSimDataQueue, &msg)
	fmt.Printf("published")
	// nc, err := nats.Connect(natsServerAddress)
	// if err != nil {
	// 	log.Fatal(err)
	// } else {
	// 	natsConn = nc
	// }

	// Create RESTful handler
	simAPI := new(SimulatorApi)
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
