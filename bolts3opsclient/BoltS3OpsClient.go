package bolts3opsclient

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gitlab.com/projectn-oss/projectn-bolt-go/bolts3"
	"strings"
	"time"
)

type BoltEvent struct {
	SdkType string `json:"sdkType"`
	RequestType string `json:"requestType"`
	Bucket string `json:"bucket"`
	Key string `json:"key"`
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

func (c *BoltS3OpsClient) ProcessEvent(event *BoltEvent) (map[string]interface{}, error) {

	c.RequestType = strings.ToUpper(event.RequestType)

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	c.SdkType = strings.ToUpper(event.SdkType)
	if len(c.SdkType) == 0 || c.SdkType == "S3" {
		c.boltSvc = s3.New(sess)
	} else {
		c.boltSvc = bolts3.New(sess)
	}

	switch c.RequestType {
	case "LIST_OBJECTS_V2":
		return c.listObjectsV2(event.Bucket)
	default:
		return map[string]interface{}{}, nil
	}
}

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