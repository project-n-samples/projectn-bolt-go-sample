package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gitlab.com/projectn-oss/projectn-bolt-go-sample/bolts3opsclient"
	"gitlab.com/projectn-oss/projectn-bolt-go/bolts3"
	"time"
)

/*HandleAutoHealRequest is the handler function that is invoked by AWS Lambda to process an incoming event for
 performing auto-heal tests.

HandleAutoHealRequest accepts the following input parameters as part of the event:
 1) bucket - bucket name
 2) key - key name
 */
func HandleAutoHealRequest(ctx context.Context, event bolts3opsclient.BoltEvent) (map[string]interface{}, error) {

	bucket := event.Bucket
	key := event.Key

	// Create bolt client.
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	boltSvc := bolts3.New(sess)

	// Attempt to retrieve object repeatedly until it succeeds, which would indicate successful
	// auto-healing of the object.
	var autoHealTime int64
	autoHealStartTime := time.Now()
	for {
		// Get Object from Bolt.
		getObjInput := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key: aws.String(key),
		}

		req, _ := boltSvc.GetObjectRequest(getObjInput)
		req.HTTPRequest.Header.Set("Accept-Encoding", "gzip")
		if err := req.Send(); err == nil {
			autoHealTime = time.Since(autoHealStartTime).Milliseconds()
			// exit on success after auto-heal
			break
		}
	}

	autoHealRespMap := make(map[string]interface{})
	autoHealRespMap["auto_heal_time"] = fmt.Sprintf("%d ms", autoHealTime)
	return autoHealRespMap, nil
}

func main() {
	lambda.Start(HandleAutoHealRequest)
}
