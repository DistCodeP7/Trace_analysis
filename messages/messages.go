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
	// Maps a receiver's name to a list of SEND events waiting for it
	pendingMessages := make(map[string][]t.Event)

	for _, p := range processes {
		processClocks[p] = t.NewVectorClock(processes)
		pendingMessages[p] = []t.Event{}
	}

	messageCounter := 0

	for len(trace) < numEvents {
		process, action := getRandomProcessAction(processes, r, pendingMessages)

		switch action {
		case t.EventSend:
			receiverName := getRandomOtherProcess(r, processes, process)

			// Increment sender's clock
			senderClock := processClocks[process]
			senderClock[process]++

			sendEvent := t.Event{
				Type:      t.EventSend,
				Process:   process,
				VClock:    t.DeepCopy(senderClock),
				MessageID: messageCounter,
			}

			// The send event happens now, so add it to the trace
			trace = append(trace, sendEvent)
			// Queue up the message for the receiver
			pendingMessages[receiverName] = append(pendingMessages[receiverName], sendEvent)
			messageCounter++

		case t.EventReceive:
			// This process was selected to receive a message
			// Dequeue a random message that was sent to it
			msgIdx := r.Intn(len(pendingMessages[process]))
			msgToReceive := pendingMessages[process][msgIdx]
			pendingMessages[process] = append(
				pendingMessages[process][:msgIdx],
				pendingMessages[process][msgIdx+1:]...,
			)

			receiverClock := processClocks[process]
			// 1. Increment receiver's local clock
			receiverClock[process]++
			// 2. Merge clocks (element-wise maximum)
			for _, p := range processes {
				receiverClock[p] = max(receiverClock[p], msgToReceive.VClock[p])
			}

			recvEvent := t.Event{
				Type:      t.EventReceive,
				Process:   process,
				VClock:    t.DeepCopy(receiverClock),
				MessageID: msgToReceive.MessageID,
			}

			// The receive event happens now, add it to the trace
			trace = append(trace, recvEvent)
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}