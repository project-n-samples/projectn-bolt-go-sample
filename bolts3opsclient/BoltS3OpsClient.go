package bolts3opsclient

import (
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gitlab.com/projectn-oss/projectn-bolt-go/bolts3"
	"io/ioutil"
	"strings"
	"time"
)

type BoltEvent struct {
	SdkType string `json:"sdkType"`
	RequestType string `json:"requestType"`
	Bucket string `json:"bucket"`
	Key string `json:"key"`
	Value string `json:"value"`
	BucketClean string `json:"bucketClean"`
}

type BoltS3OpsClient struct {
	RequestType string
	SdkType string
	boltSvc *s3.S3
}

type ListObjectsV2Resp struct {
	Key string `json:"Key"`
	LastModified time.Time `json:"LastModified"`
	ETag string `json:"ETag"`
	Size int64 `json:"Size"`
	StorageClass string `json:"StorageClass"`
}

type ListBucketsResp struct {
	Name string `json:"Name"`
	CreationDate time.Time `json:"CreationDate"`
}

// ProcessEvent extracts the parameters (sdkType, requestType, bucket/key) from the event, uses those
// parameters to send an Object/Bucket CRUD request to Bolt/S3 and returns back an appropriate response.
func (c *BoltS3OpsClient) ProcessEvent(event *BoltEvent) (map[string]interface{}, error) {

	c.RequestType = strings.ToUpper(event.RequestType)

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	// create an S3/Bolt Client depending on the 'sdkType'
	// If sdkType is not specified, create an S3 Client.
	c.SdkType = strings.ToUpper(event.SdkType)
	if len(c.SdkType) == 0 || c.SdkType == "S3" {
		c.boltSvc = s3.New(sess)
	} else {
		c.boltSvc = bolts3.New(sess)
	}

	// Perform an S3 / Bolt operation based on the input 'requestType'
	switch c.RequestType {
	case "GET_OBJECT":
		return c.getObject(event.Bucket, event.Key)
	case "LIST_OBJECTS_V2":
		return c.listObjectsV2(event.Bucket)
	case "HEAD_OBJECT":
		return c.headObject(event.Bucket, event.Key)
	case "LIST_BUCKETS":
		return c.listBuckets()
	case "HEAD_BUCKET":
		return c.headBucket(event.Bucket)
	case "PUT_OBJECT":
		return c.putObject(event.Bucket, event.Key, event.Value)
	case "DELETE_OBJECT":
		return c.deleteObject(event.Bucket, event.Key)
	default:
		return map[string]interface{}{}, nil
	}
}

// Returns a list of 1000 objects from the given bucket in Bolt/S3
func (c *BoltS3OpsClient) listObjectsV2(bucket string) (map[string]interface{}, error) {

	resp, err := c.boltSvc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		return nil, err
	}

	var objects []ListObjectsV2Resp
	for _, item := range resp.Contents {
		object := ListObjectsV2Resp{
			Key:          *item.Key,
			LastModified: *item.LastModified,
			ETag:         *item.ETag,
			Size:         *item.Size,
			StorageClass: *item.StorageClass,
		}
		objects = append(objects, object)
	}

	respMap := make(map[string]interface{})
	respMap["objects"] = objects
	return respMap, nil
}

// Gets the object from Bolt/S3, computes and returns the object's MD5 hash.
//If the object is gzip encoded, object is decompressed before computing its MD5.
func (c *BoltS3OpsClient) getObject(bucket string, key string) (map[string]interface{}, error) {

	req, output := c.boltSvc.GetObjectRequest(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	req.HTTPRequest.Header.Set("Accept-Encoding", "gzip")
	if err := req.Send(); err != nil {
		return nil, err
	}

	defer output.Body.Close()

	// If Object is gzip encoded, compute MD5 on the decompressed object.
	var objMD5 string
	if (output.ContentEncoding != nil && len(*output.ContentEncoding) > 0 && *output.ContentEncoding == "gzip") ||
		strings.HasSuffix(key, ".gz") {

		gr, err := gzip.NewReader(output.Body)
		if err != nil {
			return nil, err
		}

		defer gr.Close()
		data, err := ioutil.ReadAll(gr)
		if err != nil {
			return nil, err
		}
		objMD5 = fmt.Sprintf("%X", md5.Sum(data))
	} else {
		data, err := ioutil.ReadAll(output.Body)
		if err != nil {
			return nil, err
		}
		objMD5 = fmt.Sprintf("%X", md5.Sum(data))
	}

	respMap := make(map[string]interface{})
	respMap["md5"] = objMD5
	return respMap, nil
}

// Retrieves the object's metadata from Bolt / S3.
func (c *BoltS3OpsClient) headObject(bucket string, key string) (map[string]interface{}, error) {

	resp, err := c.boltSvc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}

	respMap := make(map[string]interface{})
	respMap["ETag"] = resp.ETag
	respMap["StorageClass"] = resp.StorageClass
	respMap["LastModified"] = resp.LastModified
	respMap["ContentLength"] = resp.ContentLength
	return respMap, nil
}

// Returns list of buckets owned by the sender of the request
func (c *BoltS3OpsClient) listBuckets() (map[string]interface{}, error) {

	resp, err := c.boltSvc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []ListBucketsResp
	for _, item := range resp.Buckets {
		bucket := ListBucketsResp{
			Name:         *item.Name,
			CreationDate: *item.CreationDate,
		}
		buckets = append(buckets, bucket)
	}

	respMap := make(map[string]interface{})
	respMap["buckets"] = buckets
	return respMap, nil
}

// Checks if the bucket exists in Bolt/S3.
func (c *BoltS3OpsClient) headBucket(bucket string) (map[string]interface{}, error) {

	req, _ := c.boltSvc.HeadBucketRequest(&s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err := req.Send(); err != nil {
		return nil, err
	}

	respMap := make(map[string]interface{})
	respMap["statusText"] = req.HTTPResponse.Status
	respMap["region"] = req.HTTPResponse.Header.Get("x-amz-bucket-region")
	return respMap, nil
}

// Uploads an object to Bolt/S3.
func (c *BoltS3OpsClient) putObject(bucket string, key string, value string) (map[string]interface{}, error) {

	putObjInput := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(key),
		Body: strings.NewReader(value)}

	resp, err := c.boltSvc.PutObject(putObjInput)
	if err != nil {
		return nil, err
	}

	respMap := make(map[string]interface{})
	respMap["ETag"] = resp.ETag
	respMap["Expiration"] = resp.Expiration
	respMap["VersionId"] = resp.VersionId
	return respMap, nil
}

// Delete an object from Bolt/S3
func (c *BoltS3OpsClient) deleteObject(bucket string, key string) (map[string]interface{}, error) {

	req, _ := c.boltSvc.DeleteObjectRequest(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err := req.Send(); err != nil {
		return nil, err
	}

	respMap := make(map[string]interface{})
	respMap["statusText"] = req.HTTPResponse.Status
	return respMap, nil
}