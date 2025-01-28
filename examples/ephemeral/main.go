package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dstreet/interstate"
	"github.com/dstreet/interstate/memory"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Create a new in-memory datastore
	store := memory.NewDatastore()

	// Create a new Interstate instance where the in-memory datastore is used for
	// both the leader node as well as the follower nodes.
	//
	// The "./" argument is the location of a directory where the
	// domain socket will be created.
	state := interstate.NewState("./", store, store)

	// Open the state, which will start the node
	if err := state.Open(); err != nil {
		log.Println("Failed to open state: %w", err)
		return
	}
	defer state.Close()

	// Get the current state version
	version, err := state.Current()
	if err != nil {
		log.Println("Failed to get initial version: %w", err)
		return
	}

	printVersion(version, "Initial version:")

	userInput := make(chan string)
	go func() {
		for {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				userInput <- scanner.Text()
			}
		}
	}()

	// Watch for updates to the state
	updates := state.Watch()

	for {
		select {
		case <-ctx.Done():
			return

		case version = <-updates:
			printVersion(version, "Received new version:")

		case data := <-userInput:
			if err := writeVersion(version, data); err != nil {
				log.Println("Failed to write version: %w", err)
			}

			version, err = state.Current()
			if err != nil {
				log.Println("Failed to get updated version: %w", err)
				return
			}
		}
	}
}

func printVersion(v *interstate.Version, header string) {
	if header != "" {
		fmt.Println(header)
	}
	fmt.Printf(" | Version: %d\n", v.Version())
	fmt.Printf(" | Data: %s\n", parseVersion(v))
}

func parseVersion(v *interstate.Version) string {
	return string(v.Bytes())
}

func writeVersion(v *interstate.Version, data string) error {
	return v.Update([]byte(data))
}
