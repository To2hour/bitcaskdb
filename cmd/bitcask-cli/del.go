package main

import (
	"fmt"

	bitcaskdb "bitcaskdb"

	"github.com/spf13/cobra"
)

var delCmd = &cobra.Command{
	Use:   "del <key>",
	Short: "Delete a key",
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

		if err := db.Delete([]byte(args[0])); err != nil {
			return fmt.Errorf("del: %w", err)
		}
		fmt.Printf("OK\n")
		return nil
	},
}
