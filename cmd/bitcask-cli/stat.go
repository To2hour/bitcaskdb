package main

import (
	"fmt"
	"os"

	bitcaskdb "bitcaskdb"

	"github.com/spf13/cobra"
)

var statCmd = &cobra.Command{
	Use:   "stat",
	Short: "Show database statistics",
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

		printStat(db)
		return nil
	},
}

func printStat(db *bitcaskdb.DB) {
	stat, err := db.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERR %v\n", err)
		return
	}
	fmt.Printf("Keys            : %d\n", stat.KeyCount)
	fmt.Printf("Data files      : %d\n", stat.DataFileCount)
	fmt.Printf("Total size      : %s\n", formatBytes(stat.TotalSize))
	fmt.Printf("Reclaimable     : %s\n", formatBytes(stat.ReclaimableSize))
	fmt.Printf("Disk utilization: %.1f%%\n", stat.DiskUtilization)
}

func formatBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
