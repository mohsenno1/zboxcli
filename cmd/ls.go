package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/0chain/gosdk/zboxcore/fileref"
	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/0chain/zboxcli/util"
	"github.com/spf13/cobra"
)

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
} // Convert int size to Human readable size

func listRecursive(allocationObj *sdk.Allocation, remotepath string, data [][]string, totalSize int, numObjects int, isHumanReadable bool) ([][]string, int, int) {
	currRef, _ := allocationObj.ListDir(remotepath)
	for _, child := range currRef.Children {
		if child.Type == fileref.DIRECTORY {
			data, totalSize, numObjects = listRecursive(allocationObj, child.Path, data, totalSize, numObjects, isHumanReadable)
		}
	}

	for _, child := range currRef.Children {
		size := strconv.FormatInt(child.Size, 10)
		if child.Type == fileref.DIRECTORY {
			size = ""
		}
		if size != "" {
			fileSize, _ := strconv.Atoi(size)
			totalSize += fileSize
			if isHumanReadable {
				size = ByteCountSI(int64(fileSize))
			}
		}
		isEncrypted := ""
		if child.Type == fileref.FILE {
			if len(child.EncryptionKey) > 0 {
				isEncrypted = "YES"
			} else {
				isEncrypted = "NO"
			}
		}
		data = append(data, []string{
			child.Type,
			child.Name,
			child.Path,
			size,
			strconv.FormatInt(child.NumBlocks, 10),
			child.LookupHash,
			isEncrypted,
			child.Attributes.WhoPaysForReads.String(),
		})
		numObjects += 1
	}
	return data, totalSize, numObjects
}

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List all buckets (first layer folders in an allocation)",
	Args:  cobra.MaximumNArgs(1),
	Long:  `AWS s3 compatible version of the listallocations command. It lists out all the buckets (or first layer folders in an allocations.)`,
	Run: func(cmd *cobra.Command, args []string) {

		if len(args) == 0 {
			cmd.Flag("remotepath").Value.Set("/")
		} else if len(args) == 1 {
			var path = "/"
			path += args[0]
			cmd.Flag("remotepath").Value.Set(path)
		}

		cmd.Flag("allocation").Value.Set(os.Getenv("ALLOC"))

		allocationID := cmd.Flag("allocation").Value.String()
		allocationObj, err := sdk.GetAllocation(allocationID)
		if err != nil {
			PrintError("Error fetching the allocation", err)
			os.Exit(1)
		}
		remotepath := cmd.Flag("remotepath").Value.String()
		ref, err := allocationObj.ListDir(remotepath)
		if err != nil {
			PrintError(err.Error())
			os.Exit(1)
		}

		header := []string{"Type", "Name", "Path", "Size", "Num Blocks", "Lookup Hash", "Is Encrypted", "Downloads payer"}

		isRecurive, _ := cmd.Flags().GetBool("recursive")
		isHumanReadable, _ := cmd.Flags().GetBool("human-readable")
		totalSize := 0  // For --summarize
		numObjects := 0 // For --summarize

		if !isRecurive {
			data := make([][]string, len(ref.Children))
			for idx, child := range ref.Children {
				size := strconv.FormatInt(child.Size, 10)
				if child.Type == fileref.DIRECTORY {
					size = ""
				}
				if size != "" {
					fileSize, _ := strconv.Atoi(size)
					totalSize += fileSize
					if isHumanReadable {
						size = ByteCountSI(int64(fileSize))
					}
				}
				isEncrypted := ""
				if child.Type == fileref.FILE {
					if len(child.EncryptionKey) > 0 {
						isEncrypted = "YES"
					} else {
						isEncrypted = "NO"
					}
				}
				data[idx] = []string{
					child.Type,
					child.Name,
					child.Path,
					size,
					strconv.FormatInt(child.NumBlocks, 10),
					child.LookupHash,
					isEncrypted,
					child.Attributes.WhoPaysForReads.String(),
				}
				numObjects += 1
			}
			util.WriteTable(os.Stdout, header, []string{}, data)
		} else {
			data := make([][]string, 10000) // Can list at most 10,000 entries
			data, totalSize, numObjects = listRecursive(allocationObj, remotepath, data, totalSize, numObjects, isHumanReadable)
			util.WriteTable(os.Stdout, header, []string{}, data)
		}
		isSummarized, _ := cmd.Flags().GetBool("summarize")
		if isSummarized {
			fmt.Println("Total Objects: ", numObjects)
			fmt.Println("Total Size: ", ByteCountSI(int64(totalSize)))
		}
	},
}

func init() {
	rootCmd.AddCommand(lsCmd)
	lsCmd.PersistentFlags().String("allocation", "", "Allocation ID (will get automatically if it is stored as an env variable.)")
	lsCmd.PersistentFlags().String("remotepath", "", "Remote path to list from.")
	lsCmd.MarkFlagRequired("allocation")
	lsCmd.Flags().Bool("recursive", false, "List all items in remotepath recursively")
	lsCmd.Flags().Bool("summarize", false, "Show summary (number of files, total size.)")
	lsCmd.Flags().Bool("human-readable", false, "Show file sizes in human readbale format.")
}
