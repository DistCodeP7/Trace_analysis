package main

import (
	"fmt"

	"github.com/traces/dag"
	"github.com/traces/messages"
)

func main() {
	processes := []string{"A", "B", "C"}
	trace := messages.GenerateAsyncTrace(processes, 10)

	fmt.Println("Trace:")
	fmt.Println(trace.String())

	lort := dag.BuildDAG(trace)
	fmt.Print(lort.ToGraphviz())

}
