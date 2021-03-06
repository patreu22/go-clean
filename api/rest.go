package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/go-nats"
	"log"
	"net/http"
	"os"
)

var (
	natsServerAddress = os.Getenv("NATS_URI")
	// natsServerAddress = "nats://nats:IoslProject2018@iosl2018hxqma76gup7si-vm0.westeurope.cloudapp.azure.com:4222"
	natsRawSimDataQueue = "GoMicro_SimulatorData"
	globalNatsConn      *nats.Conn
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
		Lat:       13.296343,
		Lon:       52.528917,
	}

	msgDataJSON, err := json.Marshal(msgData)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn.Publish(natsRawSimDataQueue, msgDataJSON)

	responseMsg := map[string]string{
		"message": "Pushed mockup dimulation data to NATS queue",
		"data":    string(msgDataJSON),
	}

	responseMsgJSON, err := json.Marshal(responseMsg)
	if err != nil {
		log.Fatal(err)
	}

	w.Write(responseMsgJSON)
	log.Print("Published following message:")
	log.Print(string(responseMsgJSON))
}

func main() {
	fmt.Println("Retrieved Nats URI from Environment: " + natsServerAddress)
	nc, err := nats.Connect(natsServerAddress)
	if err != nil {
		log.Fatal(err)
	}

	globalNatsConn = nc

	http.HandleFunc("/simulator", new(SimulatorAPI).PushData)
	http.ListenAndServe(":80", nil)
}
