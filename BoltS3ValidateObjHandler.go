package main

import (
	"compress/gzip"
	"context"
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gitlab.com/projectn-oss/projectn-bolt-go-sample/bolts3opsclient"
	"gitlab.com/projectn-oss/projectn-bolt-go/bolts3"
	"io/ioutil"
	"strings"
)

// HandleDataValidationRequest is the handler function that is invoked by AWS Lambda to process an incoming event for
// performing data validation tests.
// handleRequest accepts the following input parameters as part of the event:
// 1) bucket - bucket name
// 2) key - key name
// handleRequest retrieves the object from Bolt and S3 (if BucketClean is OFF), computes and returns their
// corresponding MD5 hash. If the object is gzip encoded, object is decompressed before computing its MD5.
func HandleDataValidationRequest(ctx context.Context, event bolts3opsclient.BoltEvent) (map[string]interface{}, error) {

	bucket := event.Bucket
	key := event.Key

	var bucketClean string
	if len(event.BucketClean) > 0 {
		bucketClean = strings.ToUpper(event.BucketClean)
	} else {
		bucketClean = "OFF"
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	s3Svc := s3.New(sess)
	boltSvc := bolts3.New(sess)

	// Get Object from Bolt.
	req, boltOutput := boltSvc.GetObjectRequest(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	req.HTTPRequest.Header.Set("Accept-Encoding", "gzip")
	if err = req.Send(); err != nil {
		return nil, err
	}

	// Get Object from S3 if bucket clean is off.
	var s3Output *s3.GetObjectOutput
	if bucketClean == "OFF" {
		req, s3Output = s3Svc.GetObjectRequest(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		req.HTTPRequest.Header.Set("Accept-Encoding", "gzip")
		if err := req.Send(); err != nil {
			return nil, err
		}
	}

	defer boltOutput.Body.Close()
	defer s3Output.Body.Close()

	// If Object is gzip encoded, compute MD5 on the decompressed object.
	var s3Md5, boltS3Md5 string
	if (s3Output.ContentEncoding != nil && len(*s3Output.ContentEncoding) > 0 && *s3Output.ContentEncoding == "gzip") ||
		strings.HasSuffix(key, ".gz") {

		// MD5 of the S3 object after gzip decompression.
		gr, err := gzip.NewReader(s3Output.Body)
		if err != nil {
			return nil, err
		}

		defer gr.Close()
		data, err := ioutil.ReadAll(gr)
		if err != nil {
			return nil, err
		}
		s3Md5 = fmt.Sprintf("%X", md5.Sum(data))

		// MD5 of the Bolt object after gzip decompression.
		gr, err = gzip.NewReader(boltOutput.Body)
		if err != nil {
			return nil, err
		}

		defer gr.Close()
		data, err = ioutil.ReadAll(gr)
		if err != nil {
			return nil, err
		}
		boltS3Md5 = fmt.Sprintf("%X", md5.Sum(data))
	} else {
		// MD5 of the S3 object
		data, err := ioutil.ReadAll(s3Output.Body)
		if err != nil {
			return nil, err
		}
		s3Md5 = fmt.Sprintf("%X", md5.Sum(data))

		//MD5 of the Bolt object
		data, err = ioutil.ReadAll(boltOutput.Body)
		if err != nil {
			return nil, err
		}
		boltS3Md5 = fmt.Sprintf("%X", md5.Sum(data))
	}

	respMap := make(map[string]interface{})
	respMap["s3-md5"] = s3Md5
	respMap["bolt-md5"] = boltS3Md5
	return respMap, nil
}

func main() {
	lambda.Start(HandleDataValidationRequest)
}
