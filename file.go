package s3fs

import (
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3File struct {
	fileOutput *s3.GetObjectOutput
	name       string
}

func (f *S3File) Read(b []byte) (int, error) {
	return f.fileOutput.Body.Read(b)
}

func (f *S3File) Close() error {
	return f.fileOutput.Body.Close()
}

func (f *S3File) Stat() (fs.FileInfo, error) {
	return f, nil
}

func (f *S3File) Name() string {
	return f.name
}

func (f *S3File) Size() int64 {
	return f.fileOutput.ContentLength
}

func (f *S3File) Mode() fs.FileMode {
	return fs.FileMode(0644)
}

func (f *S3File) ModTime() time.Time {
	return *f.fileOutput.LastModified
}

func (f *S3File) IsDir() bool {
	return false
}

func (f *S3File) Sys() interface{} {
	return nil
}
