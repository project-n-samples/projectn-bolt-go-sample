# AWS Lambda Function in Go for Bolt

Sample AWS Lambda Function in Go that utilizes [Go SDK for Bolt](https://gitlab.com/projectn-oss/projectn-bolt-go)

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

GOOS=linux go build BoltS3ValidateObjHandler.go
```

* Create Deployment package:

```bash
zip bolt-go-lambda-demo.zip BoltS3OpsHandler BoltS3ValidateObjHandler
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

### Usage

The Sample AWS Lambda Function in Go illustrates the usage and various operations, via separate handlers,
that can be performed using [Go SDK for Bolt](https://gitlab.com/projectn-oss/projectn-bolt-go).
The deployed AWS lambda function can be tested from the AWS Management Console by creating a test event and
specifying its inputs in JSON format.

Please ensure that `Bolt` is deployed before testing the sample AWS lambda function. If you haven't deployed `Bolt`,
follow the instructions given [here](https://xyz.projectn.co/installation-guide#estimate-savings) to deploy `Bolt`.

#### Testing Bolt or S3 Operations

`BoltS3OpsHandler` is the handler that enables the user to perform Bolt or S3 operations.
It sends a Bucket or Object request to Bolt or S3 and returns an appropriate response based on the parameters
passed in as input.

* BoltS3OpsHandler is the handler that is invoked by AWS Lambda to process an incoming event.


* BoltS3OpsHandler accepts the following input parameters as part of the event:
    * sdkType - Endpoint to which request is sent. The following values are supported:
        * S3 - The Request is sent to S3.
        * Bolt - The Request is sent to Bolt, whose endpoint is configured via 'BOLT_URL' environment variable

    * requestType - type of request / operation to be performed. The following requests are supported:
        * list_objects_v2 - list objects
        * list_buckets - list buckets
        * head_object - head object
        * head_bucket - head bucket
        * get_object - get object (md5 hash)
        * put_object - upload object
        * delete_object - delete object

    * bucket - bucket name

    * key - key name


* Following are examples of events, for various requests, that can be used to invoke the handler.
    * Listing first 1000 objects from Bolt bucket:
      ```json
        {"requestType": "list_objects_v2", "sdkType": "BOLT", "bucket": "<bucket>"}
      ```
    * Listing buckets from S3:
      ```json
      {"requestType": "list_buckets", "sdkType": "S3"}
      ```
    * Get Bolt object metadata (HeadObject):
      ```json
      {"requestType": "head_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```
    * Check if S3 bucket exists (HeadBucket):
      ```json
      {"requestType": "head_bucket","sdkType": "S3", "bucket": "<bucket>"}
      ```  
    * Retrieve object (its MD5 Hash) from Bolt:
      ```json
      {"requestType": "get_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```  
    * Upload object to Bolt:
      ```json
      {"requestType": "put_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>", "value": "<value>"}
      ```  
    * Delete object from Bolt:
      ```json
      {"requestType": "delete_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```


#### Data Validation Tests

`BoltS3ValidateObjHandler` is the handler that enables the user to perform data validation tests. It retrieves
the object from Bolt and S3 (Bucket Cleaning is disabled), computes and returns their corresponding MD5 hash.
If the object is gzip encoded, object is decompressed before computing its MD5.

* BoltS3ValidateObjHandler is a handler that is invoked by AWS Lambda to process an incoming event for performing
  data validation tests. To use this handler, change the handler of the Lambda function to
  `BoltS3ValidateObjHandler`


* BoltS3ValidateObjHandler accepts the following input parameters as part of the event:
    * bucket - bucket name

    * key - key name

* Following is an example of an event that can be used to invoke the handler.
    * Retrieve object(its MD5 hash) from Bolt and S3:

      If the object is gzip encoded, object is decompressed before computing its MD5.
      ```json
      {"bucket": "<bucket>", "key": "<key>"}
      ```

### Getting Help

For additional assistance, please refer to [Project N Docs](https://xyz.projectn.co/) or contact us directly
[here](mailto:support@projectn.co)