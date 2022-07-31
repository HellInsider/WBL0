package main

import (
	"Client/model"
	"fmt"
	"log"
	"net/http"
)

func main() {

	mem := model.New()
	err := GetDataFromDB(mem)
	if err != nil {
		log.Println(fmt.Errorf("init cache error %s", err))
		return
	}
	sc, err := ConnectNatsStream()
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()
	err = MsgProcessing(sc, mem)
	if err != nil {
		log.Fatal(err)
	}
	Start(mem)
	err = http.ListenAndServe(":2345", nil)
	if err != nil {
		fmt.Errorf(err.Error())
	}

}
