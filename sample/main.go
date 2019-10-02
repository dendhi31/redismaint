package main

import (
	"log"
	"os"
	"syscall"
	"time"

	"os/signal"

	"github.com/syariatifaris/redismaint"
)

func main() {
	config := redismaint.Configuration{
		RedisURL:   "localhost:6379",
		ContexName: "product_maintenance",
		Debug:      true,
		OnMaintenanceStarted: func(evt *redismaint.Event) error {
			log.Println("schedule start for", evt.ID)
			return nil
		},
		OnMaintenanceFinished: func(evt *redismaint.Event) error {
			log.Println("schedule end for", evt.ID)
			return nil
		},
		SleepDuration: time.Second * 2,
	}
	rmaint, err := redismaint.New(config)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	go func() {
		log.Println("starting the maintenance scheduler")
		rmaint.Run()
	}()
	//send sample schedule
	go func() {
		err = rmaint.Schedule("product_a", redismaint.CreateSchedules(
			redismaint.Schedule{Day: "wednesday", StartFmt: "15:20:00", EndFmt: "15:21:00"},
		))
		if err != nil {
			log.Fatalln(err.Error())
			return
		}
		err = rmaint.Schedule("product_b", redismaint.CreateSchedules(
			redismaint.Schedule{Day: "wednesday", StartFmt: "15:20:00", EndFmt: "15:21:00"},
		))
		if err != nil {
			log.Fatalln(err.Error())
			return
		}
	}()
	select {
	case <-rmaint.Err():
		log.Println("err", err)
	case <-term:
		rmaint.Stop()
		log.Println("signal terminated detected")
	}
}
