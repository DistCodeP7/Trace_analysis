package main

import (
	"fmt"

	"github.com/traces/messages"
)

func main() {
	processes := []string{"A", "B", "C"}
	trace := messages.GenerateAsyncTrace(processes, 30)
	for i, event := range trace {
		fmt.Printf("e-%-2d: Msg-%d %-4s on %s, VClock: %s\n",
			i, event.MessageID, event.Type.String(), event.Process, event.VClock.String())
	}
}
