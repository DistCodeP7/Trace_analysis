package types

import (
	"fmt"
)

type EventType int

const (
	EventSend EventType = iota
	EventReceive
)

type Event struct {
	Type      EventType
	Process   string
	VClock    VectorClock
	MessageID int
}

func (et EventType) String() string {
	switch et {
	case EventSend:
		return "SEND"
	case EventReceive:
		return "RECV"
	default:
		return "UNKNOWN"
	}
}

type Trace []Event

func (t Trace) String() string {
	var result string
	for i, e := range t {
		result += fmt.Sprintf("e-%-2d: Msg-%d %-4s on %s, VClock: %s\n",
			i, e.MessageID, e.Type.String(), e.Process, e.VClock.String())
	}
	return result

}

type DAG struct {
	Nodes map[string][]string
	Edges map[string][]string
}
