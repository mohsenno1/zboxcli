package cmd

import (
	"fmt"
	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/0chain/zboxcli/config"
	"github.com/0chain/zboxcli/model"
	"github.com/0chain/zboxcli/service"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var cfgPath, region string

const (
	LocalDirectory = "/tmp/migration/s3"
)

// s3Cmd is the migrateFromS3 sub command to start the api server
var s3Cmd = &cobra.Command{
	Use:   "migrate-s3",
	Short: "migrate user data from S3 to dStorage",
	RunE:  migrateFromS3,
}

func init() {
	rootCmd.AddCommand(s3Cmd)
	s3Cmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", ".env", "config file path")
	s3Cmd.PersistentFlags().StringVarP(&region, "region", "r", "", "s3 region")
	s3Cmd.PersistentFlags().StringSliceP("bucket", "b", nil, "bucket in use")
	s3Cmd.PersistentFlags().String("allocation", "", "Allocation ID")
	//s3Cmd.PersistentFlags().String("thumbnailpath", "", "Local thumbnail path of file to upload")
	s3Cmd.Flags().Bool("encrypt", false, "pass this option to encrypt and upload the file")
	s3Cmd.Flags().Bool("commit", false, "pass this option to commit the metadata transaction")
	s3Cmd.MarkFlagRequired("allocation")
}

func migrateFromS3(cmd *cobra.Command, args []string) error {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("starting migration from s3")
	err := godotenv.Load(cfgPath)
	if err != nil {
		log.Println("No config path defined")
		log.Println("assuming the necessary env variables are defined")
	}

	// get migration configurations
	migrationConfig, err := config.GetMigrationConfig(cmd)
	if err != nil {
		log.Fatalln(err)
	}

	// use a region specific s3 session to fetch files from s3
	sess := config.GetS3Session(migrationConfig.Region)

	// list all the buckets
	if len(migrationConfig.Buckets) == 0 {
		buckets, err := ListS3Buckets(sess)
		if err != nil {
			log.Println(err)
			return err
		}

		//log.Println("buckets ,", buckets)
		migrationConfig.Buckets = buckets
	}

	log.Println("MigrateFromS3BucketsUsingLocalStorage")
	err = MigrateFromS3BucketsUsingLocalStorage(sess, migrationConfig)
	if err != nil {
		log.Println(err)
	}

	log.Println("migration done")
	return nil

}

func ListS3Buckets(sess *session.Session) ([]string, error) {
	// Create S3 service client
	svc := s3.New(sess)
	result, err := svc.ListBuckets(nil)
	if err != nil {
		log.Println("Unable to list buckets, %v" + err.Error())
		return nil, err
	}

	buckets := make([]string, 0)
	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))

		buckets = append(buckets, aws.StringValue(b.Name))
	}

	return buckets, nil
}

// new functions

//MigrateFromS3BucketsUsingLocalStorage takes all the data from specified buckets,
// downloads them to local storage and then uploads them to dStorage
func MigrateFromS3BucketsUsingLocalStorage(sess *session.Session, upConf *model.MigrationConfig) error {
	// declare a s3 download manager
	downloadManager := s3manager.NewDownloader(sess)
	// Create S3 service client
	svc := s3.New(sess)
	//pageCount := 1

	// use existing file list to exclude files that already exists in remote directory from being processed
	existingFIleList, err := getExistingFileList(upConf)
	if err != nil {
		log.Println(err)
		return err
	}

	// process files from each individual bucket
	for _, thisBucket := range upConf.Buckets {
		// paginate contents of a bucket
		err = svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{Bucket: aws.String(thisBucket)}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			//fmt.Println("pageCount:", pageCount)
			//pageCount++

			//process all files in a page
			for _, item := range page.Contents {
				fmt.Println("")
				fmt.Println("Processing Bucket:")
				fmt.Println("Name:         ", *item.Key)
				fmt.Println("Size:         ", *item.Size)

				// check if this specific file exists in existing file list, skip processing if it does
				remoteFilePath := fmt.Sprintf("/%s/%s", thisBucket, *item.Key)
				if val, ok := existingFIleList[remoteFilePath]; ok {
					log.Println("file already exits in remote")
					log.Println("dStorage size:", val)
					log.Println("s3 size:", *item.Size)
					log.Println("skipping download")
					continue
				}

				// for new files, download to local and then upload to remote storage
				localTempFilePath, err := saveToLocal(downloadManager, fmt.Sprintf("%s/tmp", LocalDirectory), thisBucket, *item.Key)
				if err != nil {
					log.Println("error:", err)
					continue
				}
				upConf.LocalTempFilePath = localTempFilePath
				upConf.RemoteFilePath = remoteFilePath

				uploadService := service.NewUploadService(upConf)
				err = uploadService.UploadToDStorage()
				if err != nil {
					log.Println("upload error:", err)
				}

				err = cleanupLocalFile(localTempFilePath)
				if err != nil {
					log.Println("file delete error:", err)
					continue
				}
			}
			return true
		})
		if err != nil {
			log.Printf("Unable to list items in bucket %q, %v", thisBucket, err)
			return err
		}
	}

	cleanupLocalAll(LocalDirectory)

	return nil
}

func getExistingFileList(uploadConfig *model.MigrationConfig) (map[string]int64, error) {
	allocationObj, err := sdk.GetAllocation(uploadConfig.AllocationID)
	if err != nil {
		PrintError("Error fetching the allocation", err)
		return nil, err
	}
	// Create filter
	filter := []string{".DS_Store", ".git"}
	exclMap := make(map[string]int)
	for idx, path := range filter {
		exclMap[strings.TrimRight(path, "/")] = idx
	}

	remoteFiles, err := allocationObj.GetRemoteFileMap(exclMap)
	if err != nil {
		PrintError("Error getting remote files.", err)
		return nil, err
	}

	fileList := map[string]int64{}

	for remoteFileName, remoteFileValue := range remoteFiles {
		fileList[remoteFileName] = remoteFileValue.Size
	}

	return fileList, nil
}

func saveToLocal(downloader *s3manager.Downloader, localDirectory, bucket, key string) (string, error) {
	// Create the directories in the path
	filePath := filepath.Join(localDirectory, key)
	if err := os.MkdirAll(filepath.Dir(filePath), 0775); err != nil {
		return "", err
	}

	// Set up the local filePath
	fd, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer fd.Close()

	// Download the filePath using the AWS SDK for Go
	fmt.Printf("Downloading s3://%s/%s to %s...\n", bucket, key, filePath)
	downloadOption := func(d *s3manager.Downloader) {
		d.PartSize = 5 * 100 * 1024 // 5MB per part
		d.Concurrency = 4
	}
	_, err = downloader.Download(fd, &s3.GetObjectInput{Bucket: &bucket, Key: &key}, downloadOption)
	if err != nil {
		return "", fmt.Errorf("download err : %s", err.Error())
	}

	return filePath, nil
}

func cleanupLocalFile(filePath string) error {
	log.Println("delete local file")
	// delete local file
	err := os.Remove(filePath)
	if err != nil {
		return err
	}

	return nil
}

func cleanupLocalAll(path string) error {
	log.Println("delete local path")
	// delete local file
	err := os.RemoveAll(path)
	if err != nil {
		return err
	}

	return nil
}

// todo: use data stream to upload

// MigrateFromS3UsingStream steams all the data in the specified buckets to dStorage
func MigrateFromS3UsingStream(sess *session.Session, upConf *model.MigrationConfig) error {
	// Create S3 service client
	svc := s3.New(sess)
	pageCount := 1

	for _, singleBucket := range upConf.Buckets {
		err := svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{Bucket: aws.String(singleBucket)}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			fmt.Println("pageCount:", pageCount)
			pageCount++
			for _, item := range page.Contents {
				fmt.Println("Name:         ", *item.Key)
				fmt.Println("Storage class:", *item.StorageClass)
				fmt.Println("")
				err := SendToStorageDirectlyFromS3Bucket(svc, upConf, singleBucket, *item.Key)
				if err != nil {
					log.Println("save to reader error:", err)
					break
				}
			}
			return true
		})
		if err != nil {
			log.Printf("Unable to list items in bucket %q, %v", singleBucket, err)
			return err
		}
	}
	cleanupLocalFile(LocalDirectory)

	return nil
}

//SendToStorageDirectlyFromS3Bucket takes a single file from the bucket and upload it as a stream to dStorage
func SendToStorageDirectlyFromS3Bucket(svc *s3.S3, upConf *model.MigrationConfig, bucket, key string) error {
	log.Println("SendToStorageDirectlyFromS3Bucket : start to save file in writer")
	t := time.Now()
	// Download the filePath using the AWS SDK for Go
	fmt.Printf("Migrating s3://%s/%s..\n", bucket, key)
	out, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Println(err)
		return err
	}
	fmt.Printf("GetObject : execution time: %s \n", time.Now().Sub(t).String())
	log.Println(out.GoString())
	fmt.Printf("out.GoString() : execution time: %s \n", time.Now().Sub(t).String())

	//resBytes :=  make([]byte,0)

	//totalSize := 0
	reader := out.Body
	upConf.SourceFileReader = reader
	upConf.RemoteFilePath = fmt.Sprintf("/%s/%s", bucket, key)

	uploadService := service.NewUploadService(upConf)
	err = uploadService.UploadToDStorage()
	if err != nil {
		log.Println("upload error:", err)
	}

	//for {
	//	data := make([]byte,1024)
	//	n, err := reader.Read(data)
	//	if err != nil {
	//		if err.Error() == "EOF"{
	//			log.Println("eof, read complete")
	//			break
	//		}
	//		log.Println(err)
	//		break
	//	}
	//	totalSize = totalSize + n
	//
	//	resBytes = append(resBytes, data...)
	//	log.Println("added",n,"bytes. total size in bytes : ", totalSize)
	//
	//}
	//f, err := os.Create("this.pdf")
	//if err != nil {
	//	log.Println(err)
	//}
	//
	//io.Copy(f, reader)
	//
	//log.Println("end download: ")
	//fmt.Printf("read All : execution time: %s \n", time.Now().Sub(t).String())
	//
	//log.Println("piped file ", key, "in", bucket, " : meta:", out.Metadata, "")
	// todo: use reader to upload to dStorage using go sdk
	fmt.Printf("SendToStorageDirectlyFromS3Bucket : execution time: %s \n", time.Now().Sub(t).String())

	return nil
}
