package main

import (
	"context"
	"github.com/aws/aws-lambda-go/lambda"
	"gitlab.com/projectn-oss/projectn-bolt-go-sample/bolts3opsclient"
)



func HandleRequest(ctx context.Context, event bolts3opsclient.BoltEvent) (map[string]interface{}, error) {

	boltS3OpsClient := bolts3opsclient.BoltS3OpsClient{}
	return boltS3OpsClient.ProcessEvent(&event)
}

func main() {
	lambda.Start(HandleRequest)
}


