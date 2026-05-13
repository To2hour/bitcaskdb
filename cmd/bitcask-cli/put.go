package main

import (
	"fmt"

	bitcaskdb "bitcaskdb"

	"github.com/spf13/cobra"
)

var putCmd = &cobra.Command{
	Use:   "put <key> <value>",
	Short: "Set a key-value pair",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := bitcaskdb.Open(&bitcaskdb.Options{
			DirPath:     dirPath,
			SegmentSize: bitcaskdb.GB,
		})
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
		defer db.Close()

		if err := db.Put([]byte(args[0]), []byte(args[1])); err != nil {
			return fmt.Errorf("put: %w", err)
		}
		fmt.Printf("OK\n")
		return nil
	},
}
