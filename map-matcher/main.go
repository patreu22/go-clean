package main

import (
	"fmt"
	"log"
	"time"

	"github.com/micro/go-micro"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-plugins/broker/nats"
)

type Say struct{}

// func (s *Say) Hello(ctx context.Context, req, rsp) error {
// 	log.Print("Received Say.Hello request")
// 	rsp.Msg = "Hello " + req.Name
// 	return nil
// }

func main() {
	natsBroker := nats.NewBroker(broker.Addrs("nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"))

	// Addrs     []string
	// Secure    bool
	// Codec     codec.Codec
	// TLSConfig *tls.Config
	// // Other options for implementations of the interface
	// // can be stored in a context
	// Context context.Context

	service := micro.NewService(
		micro.Name("go.micro.mapmatcher"),
		micro.RegisterTTL(time.Second*30),
		micro.RegisterInterval(time.Second*10),
		micro.Broker(natsBroker),
	)

	// optionally setup command line usage
	service.Init()

	//Connect to Nats
	broker.Connect()
	fmt.Printf("Connected?\n")
	//broker.Subscribe("GoMicro_SimulatorData", func(Publication) {

	natsBroker.Subscribe(
		"GoMicro_SimulatorData",
		broker.Handler(func(p broker.Publication) error {
			fmt.Printf("Whoop, whoop, subscription received!")
			fmt.Printf(p.Topic())
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
		"GoMicro_SimulatorData",
		&msg,
	)

	// Register Handlers
	//hello.RegisterSayHandler(service.Server(), new(Say))

	// Run server
	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}
