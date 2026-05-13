package main

import (
	"fmt"

	bitcaskdb "bitcaskdb"

	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Get the value of a key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := bitcaskdb.Open(&bitcaskdb.Options{
			DirPath:     dirPath,
			SegmentSize: bitcaskdb.GB,
		})
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
		defer db.Close()

		value, err := db.Get([]byte(args[0]))
		if err != nil {
			return fmt.Errorf("get: %w", err)
		}
		fmt.Printf("%s\n", value)
		return nil
	},
}
