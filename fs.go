package s3fs

import (
	"context"
	"errors"
	"io/fs"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Implements fs.FS
type S3Fs struct {
	bucketId string
	client   *s3.Client
}

func New(bucketId string, client *s3.Client) (*S3Fs, error) {
	_, err := client.HeadBucket(context.TODO(), &s3.HeadBucketInput{Bucket: aws.String(bucketId)})
	if err != nil {
		return nil, err
	}
	return &S3Fs{bucketId: bucketId, client: client}, nil
}

func (fs *S3Fs) Open(name string) (fs.File, error) {
	if strings.HasSuffix(name, "/") {
		return nil, errors.New("s3fs: cannot open a directory")
	}
	o, err := fs.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(fs.bucketId),
		Key:    aws.String(name),
	})
	if err != nil {
		return nil, err
	}

	return &S3File{name: name, fileOutput: o}, nil
}
