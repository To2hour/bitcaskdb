package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	bitcaskdb "bitcaskdb"
)

func runREPL(db *bitcaskdb.DB) {
	fmt.Printf("Connected to bitcask at %q. Type 'exit' to quit.\n", dirPath)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("\nBye")
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("bitcask> ")
		if !scanner.Scan() {
			// EOF (e.g. piped input finished)
			fmt.Println("\nBye")
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "EXIT", "QUIT":
			fmt.Println("Bye")
			return

		case "PUT":
			if len(parts) != 3 {
				fmt.Fprintln(os.Stderr, "usage: PUT <key> <value>")
				continue
			}
			if err := db.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Fprintln(os.Stderr, "ERR", err)
				continue
			}
			fmt.Println("OK")

		case "GET":
			if len(parts) != 2 {
				fmt.Fprintln(os.Stderr, "usage: GET <key>")
				continue
			}
			val, err := db.Get([]byte(parts[1]))
			if err != nil {
				if errors.Is(err, bitcaskdb.ErrKeyNotFound) {
					fmt.Println("(nil)")
				} else {
					fmt.Fprintln(os.Stderr, "ERR", err)
				}
				continue
			}
			fmt.Println(string(val))

		case "DEL":
			if len(parts) != 2 {
				fmt.Fprintln(os.Stderr, "usage: DEL <key>")
				continue
			}
			if err := db.Delete([]byte(parts[1])); err != nil {
				fmt.Fprintln(os.Stderr, "ERR", err)
				continue
			}
			fmt.Println("OK")

		default:
			fmt.Fprintf(os.Stderr, "unknown command %q, supported: PUT GET DEL EXIT\n", parts[0])
		}
	}
}
