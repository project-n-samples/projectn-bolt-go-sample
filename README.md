# AWS Lambda Function in Go for Bolt

Sample AWS Lambda Applications in Go that utilizes [Go SDK for Bolt](https://gitlab.com/projectn-oss/projectn-bolt-go)

### Prerequisites

- Go 1.15 or higher

### Build From Source

* Download the source and its dependencies to your `GOPATH` workspace:

```bash
go get -d gitlab.com/projectn-oss/projectn-bolt-go-sample/...
```

* Compile the executable:

```bash
cd $GOPATH/src/gitlab.com/projectn-oss/projectn-bolt-go-sample

GOOS=linux go build BoltS3OpsHandler.go
```

* Create Deployment package:

```bash
zip bolt-go-lambda-demo.zip BoltS3OpsHandler
```

### Deploy

* Deploy the function to AWS Lambda by uploading the deployment package (`bolt-go-lambda-demo.zip`):

```bash
aws lambda create-function \
    --function-name <function-name> \
    --runtime go1.x \
    --zip-file fileb://bolt-go-lambda-demo.zip \
    --handler BoltS3OpsHandler \
    --role <function-execution-role-ARN> \
    --environment "Variables={BOLT_URL=<Bolt-Service-Url>}" \
    --memory-size 512 \
    --timeout 30
```
