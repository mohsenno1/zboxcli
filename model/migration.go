package model

import "io"

type UploadConfig struct {
	Region            string
	Buckets           []string
	AllocationID      string
	Commit            bool
	Encrypt           bool
	WhoPays           string
	LocalTempFilePath string
	SourceFileReader  io.Reader
	RemoteFilePath    string
}
