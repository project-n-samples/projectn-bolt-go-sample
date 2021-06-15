package main

import (
	"context"
	"github.com/aws/aws-lambda-go/lambda"
	"gitlab.com/projectn-oss/projectn-bolt-go-sample/bolts3perf"
)

func HandlePerfRequest(ctx context.Context, event bolts3perf.PerfEvent) (map[string]interface{}, error) {
	boltS3Perf := bolts3perf.BoltS3Perf{}
	return boltS3Perf.ProcessEvent(&event)
}

func main() {
	lambda.Start(HandlePerfRequest)
}
