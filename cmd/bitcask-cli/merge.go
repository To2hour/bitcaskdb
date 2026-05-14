package main

import (
	"fmt"

	bitcaskdb "bitcaskdb"

	"github.com/spf13/cobra"
)

var mergeCmd = &cobra.Command{
	Use:   "merge",
	Short: "Compact data files and reclaim disk space",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := bitcaskdb.Open(&bitcaskdb.Options{
			DirPath:     dirPath,
			SegmentSize: bitcaskdb.GB,
		})
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
		defer db.Close()

		fmt.Println("Merging...")
		if err := db.Merge(); err != nil {
			return fmt.Errorf("merge: %w", err)
		}
		fmt.Println("OK")
		return nil
	},
}
