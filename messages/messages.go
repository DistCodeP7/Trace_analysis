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

	for _, p := range processes {
		processClocks[p] = t.NewVectorClock(processes)
		pendingMessages[p] = make([]t.Event, 0)
	}

	messageCounter := 0
	for range numEvents {

		senderProcess, action := getRandomProcessAction(processes, r, pendingMessages)

		switch action {
		case t.EventSend:

			receiverName := getRandomOtherProcess(r, processes, senderProcess)

			senderClock := processClocks[senderProcess]
			senderClock[senderProcess]++

			sendEvent := t.Event{
				Type:      t.EventSend,
				Process:   senderProcess,
				VClock:    t.DeepCopy(senderClock),
				MessageID: messageCounter,
			}
			trace = append(trace, sendEvent)

			pendingMessages[receiverName] = append(pendingMessages[receiverName], sendEvent)
			messageCounter++

		case t.EventReceive:

			msgToReceive := pendingMessages[senderProcess][0]
			pendingMessages[senderProcess] = pendingMessages[senderProcess][1:]

			receiverClock := processClocks[senderProcess]
			receiverClock[senderProcess]++

			msgVClock := msgToReceive.VClock
			for _, p := range processes {
				receiverClock[p] = max(receiverClock[p], msgVClock[p])
			}

			recvEvent := t.Event{
				Type:      t.EventReceive,
				Process:   senderProcess,
				VClock:    t.DeepCopy(receiverClock),
				MessageID: msgToReceive.MessageID,
			}
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
	if canReceive {
		chooseReceive := r.Intn(2) == 0
		if chooseReceive {
			action = t.EventReceive
		}
	}
	return processName, action
}

// getRandomOtherProcess selects a random process from the list, ensuring it is not the excluded process.
func getRandomOtherProcess(r *rand.Rand, processes []string, exclude string) string {
	for {
		p := processes[r.Intn(len(processes))]
		if p != exclude {
			return p
		}
	}
}
