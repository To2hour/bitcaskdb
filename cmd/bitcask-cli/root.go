package main

import (
	"fmt"
	"os"

	bitcaskdb "bitcaskdb"

	"github.com/spf13/cobra"
)

var dirPath string

var rootCmd = &cobra.Command{
	Use:           "bitcask-cli",
	Short:         "A CLI for bitcask key-value database",
	Long:          "bitcask-cli is a command-line tool to interact with a bitcask key-value database.",
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := bitcaskdb.Open(&bitcaskdb.Options{
			DirPath:     dirPath,
			SegmentSize: bitcaskdb.GB,
		})
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
		defer db.Close()
		runREPL(db)
		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&dirPath, "dir", "./data", "database directory path")
	rootCmd.AddCommand(putCmd, getCmd, delCmd, statCmd, mergeCmd)
}
