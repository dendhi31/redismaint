package redismaint

import (
	"fmt"
	"log"
	"strings"
	"time"

	"encoding/json"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

const tfmt = "15:04:05"

//EventHandler closure for event flag
type EventHandler func(event *Event) error

//Event as a maintenance event object
type Event struct {
	ID        string              `json:"id"`
	Schedules map[string]Schedule `json:"schedules"` //Keys Monday, Tuesday, ..., Sunday
}

//EventMessage as a message
type EventMessage struct {
	ID string `json:"id"`
}

//Schedule structure
type Schedule struct {
	Day      string `json:"day"`
	StartFmt string `json:"start_fmt"`
	EndFmt   string `json:"end_fmt"`
}

//MaintenanceScheduler structure
type MaintenanceScheduler struct {
	rclt  *redisc
	hkey  string
	echan chan error
	schan chan bool
	debug bool

	onStart  EventHandler
	onFinish EventHandler

	sleepDuration time.Duration
}

//Configuration as maintenance scheduler preferences
type Configuration struct {
	RedisURL      string
	ContexName    string
	Debug         bool
	SleepDuration time.Duration

	OnMaintenanceStarted, OnMaintenanceFinished EventHandler
}

//New creates new redis maintenance
func New(config Configuration) (*MaintenanceScheduler, error) {
	rclt, err := dial(config.RedisURL)
	if err != nil {
		return nil, err
	}
	return &MaintenanceScheduler{
		rclt:          rclt,
		hkey:          config.ContexName,
		echan:         make(chan error, 1),
		schan:         make(chan bool, 1),
		debug:         config.Debug,
		onStart:       config.OnMaintenanceStarted,
		onFinish:      config.OnMaintenanceFinished,
		sleepDuration: config.SleepDuration,
	}, nil
}

//CreateSchedules creates the schedule
func CreateSchedules(schedules ...Schedule) map[string]Schedule {
	sm := make(map[string]Schedule, 0)
	for _, s := range schedules {
		sm[s.Day] = s
	}
	return sm
}

//Schedule schedules a maintenance in eternity
func (m *MaintenanceScheduler) Schedule(id string,
	schedules map[string]Schedule) error {
	bytes, err := json.Marshal(schedules)
	if err != nil {
		return errors.Wrap(err, "schedules marshall error")
	}
	mbytes, err := json.Marshal(EventMessage{ID: id})
	if err != nil {
		return errors.Wrap(err, "schedules marshall error")
	}
	c := m.rclt.gconn()
	defer c.Close()
	_, err = c.Do("HSET", fmt.Sprintf("%s_flags", m.hkey), id, false)
	if err != nil {
		return errors.Wrap(err, "fail to set new flag data")
	}
	_, err = c.Do("HSET", fmt.Sprintf("%s_schedules", m.hkey), id, string(bytes))
	if err != nil {
		return errors.Wrap(err, "fail to set new schedule data")
	}
	key := fmt.Sprintf("%s_channel", m.hkey)
	_, err = c.Do("PUBLISH", key, string(mbytes))
	if err != nil {
		return errors.Wrap(err, "fail to set new schedule data")
	}
	return nil
}

//Run runs the maintainance scheduler
func (m *MaintenanceScheduler) Run() {
	rc := m.rclt.gconn()
	psc := redis.PubSubConn{
		Conn: rc,
	}
	key := fmt.Sprintf("%s_channel", m.hkey)
	if err := psc.PSubscribe(key); err != nil {
		m.echan <- err
		return
	}
	for {
		select {
		case <-m.schan:
			rc.Close()
			psc.Close()
			return
		default:
			switch msg := psc.Receive().(type) {
			case redis.Message:
				go m.process(msg.Data)
			}
		}
	}
}

//Err returns error channel
func (m *MaintenanceScheduler) Err() <-chan error {
	return m.echan
}

//Stop set stop flag
func (m *MaintenanceScheduler) Stop() {
	m.schan <- true
}

func (m *MaintenanceScheduler) process(bytes []byte) {
	var emsg EventMessage
	err := json.Unmarshal(bytes, &emsg)
	if err != nil {
		m.debugln("unable to cast data to event message, content=", string(bytes))
		return
	}
	rc := m.rclt.gconn()
	if rc == nil {
		m.debugln("unable to get redis connection pool, content=", string(bytes))
		return
	}
	defer func() {
		_, err = rc.Do("PUBLISH", fmt.Sprintf("%s_channel", m.hkey), string(bytes))
		if err != nil {
			m.debugln("unable to get republish message for id", emsg.ID, err.Error())
			return
		}
	}()
	defer time.Sleep(m.sleepDuration)
	skey := fmt.Sprintf("%s_schedules", m.hkey)
	str, err := redis.String(rc.Do("HGET", skey, emsg.ID))
	if err != nil { //including err = redis.ErrNil
		m.debugln("unable to get the schedule data for", emsg.ID, err.Error())
		return
	}
	if str == "" {
		m.debugln("unable to cast schedule data, empty message for", emsg.ID)
		return
	}
	var schedules map[string]Schedule
	err = json.Unmarshal([]byte(str), &schedules)
	if err != nil {
		m.debugln("unable to unsmarshall schedule data for", emsg.ID, err.Error())
		return
	}
	event := Event{ID: emsg.ID, Schedules: schedules}
	maint, err := m.between(&event)
	if err != nil {
		m.debugln("fail on checking schedules", emsg.ID, err.Error())
		return
	}
	fkey := fmt.Sprintf("%s_flags", m.hkey)
	flag, err := redis.Bool(rc.Do("HGET", fkey, emsg.ID))
	if err != nil { //including err = redis.ErrNil
		m.debugln("unable to get the schedule item flag data for", emsg.ID, err.Error())
		return
	}
	if maint {
		if !flag { //maintenance mode, but flag not set = indicate the maintenance has just started
			_, err = rc.Do("HSET", fmt.Sprintf("%s_flags", m.hkey), emsg.ID, true)
			if err != nil {
				m.debugln("fail to set", emsg.ID, "flag to true", err.Error())
				return
			}
			err := m.onStart(&event)
			if err != nil {
				m.debugln("on maintenance start callback error", emsg.ID, err.Error())
				return
			}
		}
		return
	}
	if flag { //outside maintenance mode, but the flag is still on, means it has just ended
		_, err = rc.Do("HSET", fmt.Sprintf("%s_flags", m.hkey), emsg.ID, false)
		if err != nil {
			m.debugln("fail to set", emsg.ID, "flag to false (off)", err.Error())
			return
		}
		err := m.onFinish(&event)
		if err != nil {
			m.debugln("on maintenance end callback error", emsg.ID, err.Error())
			return
		}
	}
}

func (m *MaintenanceScheduler) debugln(args ...interface{}) {
	if m.debug {
		log.Println(args...)
	}
}

func leadZero(n int)string{
	if n < 10{
		return fmt.Sprintf("0%d", n)
	}
	return fmt.Sprint(n)
}

//between checks if it is in maintenance time
func (m *MaintenanceScheduler) between(event *Event) (bool, error) {
	now := time.Now()
	hh, mm, ss := now.Clock()
	tnow, err := time.Parse(tfmt, fmt.Sprintf("%s:%s:%s", leadZero(hh), leadZero(mm), leadZero(ss)))
	if err != nil {
		return false, errors.Wrap(err, "unable to parse current time")
	}
	day := strings.ToLower(now.Weekday().String())
	schedule, ok := event.Schedules[day]
	if !ok {
		return false, nil
	}
	stime, err := time.Parse(tfmt, schedule.StartFmt)
	if err != nil {
		return false, errors.Wrap(err, "unable to parse start time")
	}
	etime, err := time.Parse(tfmt, schedule.EndFmt)
	if err != nil {
		return false, errors.Wrap(err, "unable to parse end time")
	}
	if !(tnow.After(stime) && tnow.Before(etime)) {
		return false, nil
	}
	return true, nil
}
