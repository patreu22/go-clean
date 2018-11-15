package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/micro/go-micro"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-plugins/broker/nats"
)

var (
	natsURI            = "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	subscribeQueueName = "GoMicro_SimulatorData"
	publishQueueName   = "GoMicro_MapMatcher"
)

// MockedReceivingMessage comment
type MockedReceivingMessage struct {
	MessageID int
	CarID     int
	Timestamp string
	Accuracy  int
	Lat       float32
	Lon       float32
}

func main() {
	natsBroker := nats.NewBroker(broker.Addrs(natsURI))

	service := micro.NewService(
		micro.Name("go.micro.mapmatcher"),
		micro.RegisterTTL(time.Second*30),
		micro.RegisterInterval(time.Second*10),
		micro.Broker(natsBroker),
	)

	// optionally setup command line usage
	service.Init()

	//Connect to Nats
	natsBroker.Connect()

	natsBroker.Subscribe(
		subscribeQueueName,
		broker.Handler(func(p broker.Publication) error {
			fmt.Printf("HELLOOO!!!")
			var msgBody = p.Message().Body
			fmt.Printf("HELLO!")
			fmt.Printf(string(msgBody))
			var msg MockedReceivingMessage
			err := json.Unmarshal(msgBody, &msg)
			if err != nil {
				fmt.Println("error:", err)
			}
			processMessage(MockedReceivingMessage(msg))
			return nil
		}),
	)

	// var msg = broker.Message{
	// 	map[string]string{},
	// 	[]byte("Hello NATS!"),
	// }

	// natsBroker.Publish(
	// 	publishQueueName,
	// 	&msg,
	// )

	// Register Handlers
	//hello.RegisterSayHandler(service.Server(), new(Say))

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}

func processMessage(msg MockedReceivingMessage) {
	fmt.Printf("---------------\n")
}
