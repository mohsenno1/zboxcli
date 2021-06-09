package cmd

import (
	"fmt"
	"os"
	"sync"

	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/spf13/cobra"
)

var cpCmd = &cobra.Command{
	Use:   "cp",
	Short: "Copies a local file or S3 object to another location locally or in S3.",
	Long:  `Copies a local file or S3 object to another location locally or in S3.`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {

		allocationID := os.Getenv("ALLOC")
		allocationObj, err := sdk.GetAllocation(allocationID)
		if err != nil {
			fmt.Println("Error fetching the allocation", err)
			return
		}
		remotepath := args[0]
		destpath := args[1]
		commit := true

		statsMap, err := allocationObj.GetFileStats(remotepath)
		if err != nil {
			PrintError("Error in getting information about the object." + err.Error())
			os.Exit(1)
		}
		isFile := false
		for _, v := range statsMap {
			if v != nil {
				isFile = true
				break
			}
		}

		var fileMeta *sdk.ConsolidatedFileMeta
		if isFile && commit {
			fileMeta, err = allocationObj.GetFileMeta(remotepath)
			if err != nil {
				PrintError("Failed to fetch metadata for the given file", err.Error())
				os.Exit(1)
			}
		}

		err = allocationObj.CopyObject(remotepath, destpath)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Println(remotepath + " copied")
		if commit {
			fmt.Println("Commiting changes to blockchain ...")
			if isFile {
				wg := &sync.WaitGroup{}
				statusBar := &StatusBar{wg: wg}
				wg.Add(1)
				commitMetaTxn(remotepath, "Copy", "", "", allocationObj, fileMeta, statusBar)
				wg.Wait()
			} else {
				commitFolderTxn("Copy", remotepath, destpath, allocationObj)
			}
		}
		return

	},
}

func init() {
	rootCmd.AddCommand(cpCmd)
}
