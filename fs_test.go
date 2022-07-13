package s3fs

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3Fs(t *testing.T) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile("Administrator"))
	if err != nil {
		t.Error(err)
	}
	client := s3.NewFromConfig(cfg)

	s3fs, err := New("instantshare", client)
	if err != nil {
		t.Error(err)
	}

	f, err := s3fs.Open("newfolder/test.txt")
	if err != nil {
		t.Error(err)
	}

	txt, err := io.ReadAll(f)
	if err != nil {
		t.Error(err)
	}
	s := string(txt)
	if s != "hello world\n" {
		t.Error(errors.New("s3fs test: document does not match"))
	}
}
