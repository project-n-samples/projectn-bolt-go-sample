package main

import (
	"bolts3"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {

	sess, err := session.NewSession()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	boltSvc := bolts3.New(sess, aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody))

	bucket := "solaw-demo"

	resp, err := boltSvc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for _, item := range resp.Contents {
		fmt.Println(*item.Key)
	}
}

