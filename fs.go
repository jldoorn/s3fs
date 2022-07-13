package s3fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func (fs *S3Fs) Remove(name string) error {
	if strings.HasSuffix(name, "/") {
		return errors.New("s3fs: cannot remove a directory")
	}

	_, err := fs.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucketId),
		Key:    aws.String(name),
	})
	if err != nil {
		return err
	}

	return nil
}

func (fs *S3Fs) CreateFrom(name string, r io.Reader) error {
	if strings.HasSuffix(name, "/") {
		return errors.New("s3fs: cannot create a directory")
	}

	_, err := fs.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(fs.bucketId),
		Key:    aws.String(name),
		Body:   r,
	})
	if err != nil {
		return err
	}
	return nil
}

func (fs *S3Fs) RemoveAll(name string) error {

	o, err := fs.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(fs.bucketId),
		Prefix: aws.String(name),
	})
	if err != nil {
		return err
	}
	toDelete := make([]types.ObjectIdentifier, len(o.Contents))
	for i, c := range o.Contents {
		toDelete[i].Key = c.Key
	}
	_, err = fs.client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(fs.bucketId),
		Delete: &types.Delete{Objects: toDelete},
	})
	return err
}
