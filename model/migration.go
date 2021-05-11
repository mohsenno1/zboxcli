package model

import "io"

// MigrationConfig contains configurations to upload files from S3
type MigrationConfig struct {
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
