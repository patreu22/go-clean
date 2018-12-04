package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/go-nats"
	"log"
	"net/http"
)

var (
	natsServerAddress   = "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	natsRawSimDataQueue = "GoMicro_SimulatorData"
	nc                  *nats.Conn
	port                = 80
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
func (s *SimulatorAPI) PushData(w http.ResponseWriter, r *http.Request) {
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

	nc.Publish(natsRawSimDataQueue, msgDataJSON)

	responseMsg := map[string]string{
		"message": "Pushed mockup dimulation data to NATS queue",
		"data":    string(msgDataJSON),
	}

	responseMsgJSON, err := json.Marshal(responseMsg)
	if err != nil {
		log.Fatal(err)
	}

	w.Write(responseMsgJSON)
	fmt.Printf("Published following message:\n")
	fmt.Printf(string(responseMsgJSON))
}

func main() {
	nc, _ := nats.Connect(natsServerAddress)
	nc.Subscribe(natsRawSimDataQueue, func(m *nats.Msg) {
		fmt.Printf("-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})

	http.HandleFunc("/simulator", new(SimulatorAPI).PushData)
	http.ListenAndServe(":80", nil)
}
