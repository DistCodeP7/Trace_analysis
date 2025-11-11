package messages

import (
	"math/rand"
	"time"

	t "github.com/traces/types"
)

func GenerateAsyncTrace(processes []string, numEvents int) t.Trace {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	trace := make(t.Trace, 0, numEvents)
	processClocks := make(map[string]t.VectorClock)
	pendingMessages := make(map[string][]t.Event)
	pendingEvents := []t.Event{} // global pool for asynchronous delivery

	for _, p := range processes {
		processClocks[p] = t.NewVectorClock(processes)
		pendingMessages[p] = []t.Event{}
	}

	messageCounter := 0

	for len(trace) < numEvents {
		// 50% chance to deliver a pending event first
		if len(pendingEvents) > 0 && r.Intn(2) == 0 {
			idx := r.Intn(len(pendingEvents))
			event := pendingEvents[idx]
			pendingEvents = append(pendingEvents[:idx], pendingEvents[idx+1:]...)
			trace = append(trace, event)
			continue
		}

		// Otherwise, generate a new event
		senderProcess, action := getRandomProcessAction(processes, r, pendingMessages)

		switch action {
		case t.EventSend:
			receiverName := getRandomOtherProcess(r, processes, senderProcess)

			// increment sender clock
			senderClock := processClocks[senderProcess]
			senderClock[senderProcess]++

			sendEvent := t.Event{
				Type:      t.EventSend,
				Process:   senderProcess,
				VClock:    t.DeepCopy(senderClock),
				MessageID: messageCounter,
			}

			// instead of appending immediately, queue for asynchronous delivery
			pendingMessages[receiverName] = append(pendingMessages[receiverName], sendEvent)
			pendingEvents = append(pendingEvents, sendEvent)
			messageCounter++

		case t.EventReceive:
			if len(pendingMessages[senderProcess]) == 0 {
				continue // cannot receive yet
			}

			// pick a random pending message for this process
			msgIdx := r.Intn(len(pendingMessages[senderProcess]))
			msgToReceive := pendingMessages[senderProcess][msgIdx]
			pendingMessages[senderProcess] = append(
				pendingMessages[senderProcess][:msgIdx],
				pendingMessages[senderProcess][msgIdx+1:]...,
			)

			receiverClock := processClocks[senderProcess]
			receiverClock[senderProcess]++

			// merge vector clocks
			for _, p := range processes {
				receiverClock[p] = max(receiverClock[p], msgToReceive.VClock[p])
			}

			recvEvent := t.Event{
				Type:      t.EventReceive,
				Process:   senderProcess,
				VClock:    t.DeepCopy(receiverClock),
				MessageID: msgToReceive.MessageID,
			}

			pendingEvents = append(pendingEvents, recvEvent)
		}
	}

	return trace
}

// getRandomProcessAction selects a random process and determines whether it will send or receive a message.
// If the selected process has pending messages, it has a 50% chance to receive; otherwise, it will send.
func getRandomProcessAction(processes []string, r *rand.Rand, pendingMessages map[string][]t.Event) (string, t.EventType) {
	processName := processes[r.Intn(len(processes))]
	canReceive := len(pendingMessages[processName]) > 0
	action := t.EventSend
	if canReceive && r.Intn(2) == 0 {
		action = t.EventReceive
	}
	return processName, action
}

func getRandomOtherProcess(r *rand.Rand, processes []string, exclude string) string {
	for {
		p := processes[r.Intn(len(processes))]
		if p != exclude {
			return p
		}
	}
}
