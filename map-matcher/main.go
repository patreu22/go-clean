package main

import (
	"fmt"
	"log"
	"time"

	"github.com/micro/go-micro"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-plugins/broker/nats"
)

var (
	natsUri   = "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	queueName = "GoMicro_SimulatorData"
)

func main() {
	natsBroker := nats.NewBroker(broker.Addrs(natsUri))

	service := micro.NewService(
		micro.Name("go.micro.mapmatcher"),
		micro.RegisterTTL(time.Second*30),
		micro.RegisterInterval(time.Second*10),
		micro.Broker(natsBroker),
	)

	// optionally setup command line usage
	service.Init()

	//Connect to Nats
	var connect = natsBroker.Connect()
	fmt.Printf("Connected? %s\n", connect)

	//broker.Subscribe("GoMicro_SimulatorData", func(Publication) {

	natsBroker.Subscribe(
		queueName,
		broker.Handler(func(p broker.Publication) error {
			fmt.Printf("Whoop, whoop, subscription received!\n")
			fmt.Printf(string(p.Message().Body) + "\n")
			return nil
		}),
	)

	var msg = broker.Message{
		map[string]string{
			"header": "Hi, this is the Header",
		},
		[]byte("Hello NATS!"),
	}

	natsBroker.Publish(
		queueName,
		&msg,
	)

	fmt.Printf("Probably published!\n")
	// Register Handlers
	//hello.RegisterSayHandler(service.Server(), new(Say))

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}
