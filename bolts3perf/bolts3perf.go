package bolts3perf

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gitlab.com/projectn-oss/projectn-bolt-go/bolts3"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"
)

type PerfEvent struct {
	RequestType string `json:"requestType"`
	Bucket string `json:"bucket"`
	Key string `json:"key"`
	NumKeys string `json:"numKeys"`
	ObjLength string `json:"objLength"`
	NumIter string `json:"numIter"`
}

type BoltS3Perf struct {
	requestType string
	boltSvc *s3.S3
	s3Svc *s3.S3
	numKeys int
	objLength int
	numIter int
	keys []string
}

type PerfStats struct {
	Latency     *PerfStat `json:"latency,omitempty"`
	Throughput  *PerfStat `json:"throughput,omitempty"`
	ThroughputT string   `json:"throughputT,omitempty"`
	ObjectSize *PerfStat `json:"objectSize,omitempty"`
}

type PerfStat struct {
	Average string `json:"average"`
	P50 string `json:"p50"`
	P90 string `json:"p90"`
}

type ObjectCount struct {
	Compressed string `json:"compressed"`
	Uncompressed string `json:"uncompressed"`
}

func (p *BoltS3Perf) ProcessEvent(event *PerfEvent) (map[string]interface{}, error)  {

	// If requestType is not passed, perform all perf tests.
	if len(event.RequestType) > 0 {
		p.requestType = strings.ToUpper(event.RequestType)
	} else {
		p.requestType = "ALL"
	}

	// update max no. of keys and object data length, if passed as input.
	if len(event.NumKeys) > 0 {
		numKeys, err := strconv.Atoi(event.NumKeys)
		if err != nil {
			return nil, err
		}
		p.numKeys = numKeys
	} else {
		p.numKeys = 1000
	}
	if p.numKeys > 1000 {
		p.numKeys = 1000
	}

	if len(event.ObjLength) > 0 {
		objLength, err := strconv.Atoi(event.ObjLength)
		if err != nil {
			return nil, err
		}
		p.objLength = objLength
	} else {
		p.objLength = 100
	}

	// initialize S3 and Bolt clients
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	p.s3Svc = s3.New(sess)
	p.boltSvc = bolts3.New(sess)

	// If Put, Delete, All Object Perf test then generate key names.
	// If Get Object Perf Test (including passthrough), list objects (up to numKeys) to get key names.
	if p.requestType == "PUT_OBJECT" ||
		p.requestType == "DELETE_OBJECT" ||
		p.requestType == "ALL" {
		p.generateKeyNames(p.numKeys)
	} else if p.requestType == "GET_OBJECT" ||
		p.requestType == "GET_OBJECT_PASSTHROUGH" ||
		p.requestType == "GET_OBJECT_TTFB" ||
		p.requestType == "GET_OBJECT_PASSTHROUGH_TTFB" {
		err := p.listObjectsV2(event.Bucket)
		if err != nil {
			return nil, err
		}
	}

	//Perform Perf Test depending on the requestType.
	switch p.requestType {
	case "LIST_OBJECTS_V2":
		return p.listObjectsV2Perf(event.Bucket)
	case "PUT_OBJECT":
		return p.putObjectPerf(event.Bucket)
	case "DELETE_OBJECT":
		return p.deleteObjectPerf(event.Bucket)
	case "GET_OBJECT", "GET_OBJECT_TTFB":
		return p.getObjectPerf(event.Bucket)
	case "GET_OBJECT_PASSTHROUGH", "GET_OBJECT_PASSTHROUGH_TTFB":
		return p.getObjectPassthroughPerf(event.Bucket)
	case "ALL":
		return p.allPerf(event.Bucket)
	default:
		return map[string]interface{}{}, nil
	}
}

func (p *BoltS3Perf) listObjectsV2Perf(bucket string) (map[string]interface{}, error) {

	var s3ListObjTimes []int64
	var boltListObjTimes []int64
	var s3ListObjTp []float64
	var boltListObjTp []float64
	p.numIter = 10

	// list 1000 objects from S3, numIter times.
	for i := 0; i < p.numIter; i++ {

		req := &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			MaxKeys: aws.Int64(int64(p.numKeys)),
		}

		start := time.Now()
		resp, err := p.s3Svc.ListObjectsV2(req)
		if err != nil {
			return nil, err
		}
		
		// calc latency
		listObjTime := time.Since(start).Milliseconds()
		s3ListObjTimes = append(s3ListObjTimes, listObjTime)

		// calc throughput
		listObjV2Tp := float64(*resp.KeyCount) / float64(listObjTime)
		s3ListObjTp = append(s3ListObjTp, listObjV2Tp)
	}

	// list 1000 objects from Bolt, numIter times.
	for i := 0; i < p.numIter; i++ {

		req := &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			MaxKeys: aws.Int64(int64(p.numKeys)),
		}

		start := time.Now()
		resp, err := p.boltSvc.ListObjectsV2(req)
		if err != nil {
			return nil, err
		}

		// calc latency
		listObjTime := time.Since(start).Milliseconds()
		boltListObjTimes = append(boltListObjTimes, listObjTime)

		// calc throughput
		listObjV2Tp := float64(*resp.KeyCount) / float64(listObjTime)
		boltListObjTp = append(boltListObjTp, listObjV2Tp)
	}

	// calc s3 perf stats.
	s3ListObjPerfStats := p.computePerfStats(s3ListObjTimes, s3ListObjTp, nil)

	// calc bolt perf stats.
	boltListObjPerfStats := p.computePerfStats(boltListObjTimes, boltListObjTp, nil)

	listObjPerfRespMap := make(map[string]interface{})
	listObjPerfRespMap["s3_list_objects_v2_perf_stats"] = s3ListObjPerfStats
	listObjPerfRespMap["bolt_list_objects_v2_perf_stats"] = boltListObjPerfStats
	return listObjPerfRespMap, nil
}

func (p *BoltS3Perf) putObjectPerf(bucket string) (map[string]interface{}, error) {
	var s3PutObjTimes []int64
	var boltPutObjTimes []int64

	// Upload object to Bolt / S3.
	for _, key := range p.keys {
		value := p.generate(p.objLength)

		putObjInput := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key: aws.String(key),
			Body: strings.NewReader(value),
		}

		// Upload object to s3.
		start := time.Now()
		_, err := p.s3Svc.PutObject(putObjInput)
		if err != nil {
			return nil, err
		}

		// calc latency
		putObjTime := time.Since(start).Milliseconds()
		s3PutObjTimes = append(s3PutObjTimes, putObjTime)

		// Upload object to Bolt.
		start = time.Now()
		_, err = p.boltSvc.PutObject(putObjInput)
		if err != nil {
			return nil, err
		}

		// calc latency
		putObjTime = time.Since(start).Milliseconds()
		boltPutObjTimes = append(boltPutObjTimes, putObjTime)
	}

	// calc s3 perf stats.
	s3PutObjPerfStats := p.computePerfStats(s3PutObjTimes, nil, nil)

	// calc bolt perf stats.
	boltPutObjPerfStats := p.computePerfStats(boltPutObjTimes, nil, nil)

	putObjPerfRespMap := make(map[string]interface{})
	putObjPerfRespMap["s3_put_obj_perf_stats"] = s3PutObjPerfStats
	putObjPerfRespMap["bolt_put_obj_perf_stats"] = boltPutObjPerfStats
	return putObjPerfRespMap, nil
}

func (p *BoltS3Perf) deleteObjectPerf(bucket string) (map[string]interface{}, error) {
	var s3DelObjTimes []int64
	var boltDelObjTimes []int64

	// Delete Objects from Bolt / S3.
	for _, key := range p.keys {

		delObjInput := &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key: aws.String(key),
		}

		// Delete object from S3.
		start := time.Now()
		_, err := p.s3Svc.DeleteObject(delObjInput)
		if err != nil {
			return nil, err
		}

		// calc latency
		delObjTime := time.Since(start).Milliseconds()
		s3DelObjTimes = append(s3DelObjTimes, delObjTime)

		// Delete object from Bolt.
		start = time.Now()
		_, err = p.boltSvc.DeleteObject(delObjInput)
		if err != nil {
			return nil, err
		}

		// calc latency
		delObjTime = time.Since(start).Milliseconds()
		boltDelObjTimes = append(boltDelObjTimes, delObjTime)
	}

	// calc s3 perf stats.
	s3DelObjPerfStats := p.computePerfStats(s3DelObjTimes, nil, nil)

	// calc bolt perf stats.
	boltDelObjPerfStats := p.computePerfStats(boltDelObjTimes, nil, nil)

	delObjPerfRespMap := make(map[string]interface{})
	delObjPerfRespMap["s3_del_obj_perf_stats"] = s3DelObjPerfStats
	delObjPerfRespMap["bolt_del_obj_perf_stats"] = boltDelObjPerfStats
	return delObjPerfRespMap, nil
}

func (p *BoltS3Perf) getObjectPerf(bucket string) (map[string]interface{}, error) {
	var s3GetObjTimes []int64
	var boltGetObjTimes []int64

	var s3ObjSizes []int64
	var boltObjSizes []int64

	s3CmpObjCount := 0
	s3UnCmpObjCount := 0
	boltCmpObjCount := 0
	boltUnCmpObjCount := 0

	// Get Objects from S3.
	for _, key := range p.keys {

		getObjInput := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key: aws.String(key),
		}

		req, output := p.s3Svc.GetObjectRequest(getObjInput)
		req.HTTPRequest.Header.Set("Accept-Encoding", "gzip")
		start := time.Now()
		if err := req.Send(); err != nil {
			return nil, err
		}

		// If getting first byte object latency, read at most 1 byte,
		// otherwise read the entire body.
		if p.requestType == "GET_OBJECT_TTFB" {
			// read only the first byte from the stream.
			buf := make([]byte, 1)
			_, _ = output.Body.Read(buf)
		} else {
			// read all data from the stream.
			buf := make([]byte, 4096)
			for {
				_, err := output.Body.Read(buf)
				if err == io.EOF {
					break
				}
			}
		}

		// calc latency
		getObjTime := time.Since(start).Milliseconds()
		s3GetObjTimes = append(s3GetObjTimes, getObjTime)

		// count object.
		if (output.ContentEncoding != nil &&
			len(*output.ContentEncoding) > 0 && *output.ContentEncoding == "gzip") ||
			strings.HasSuffix(key, ".gz") {
			s3CmpObjCount++
		} else {
			s3UnCmpObjCount++
		}

		// get object sizes.
		s3ObjSizes = append(s3ObjSizes, *output.ContentLength)
		// close response stream.
		output.Body.Close()
	}

	// Get Objects from Bolt.
	for _, key := range p.keys {

		getObjInput := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key: aws.String(key),
		}

		req, output := p.boltSvc.GetObjectRequest(getObjInput)
		req.HTTPRequest.Header.Set("Accept-Encoding", "gzip")
		start := time.Now()
		if err := req.Send(); err != nil {
			return nil, err
		}

		// If getting first byte object latency, read at most 1 byte,
		// otherwise read the entire body.
		if p.requestType == "GET_OBJECT_TTFB" {
			// read only the first byte from the stream.
			buf := make([]byte, 1)
			_, _ = output.Body.Read(buf)
		} else {
			// read all data from the stream.
			buf := make([]byte, 4096)
			for {
				_, err := output.Body.Read(buf)
				if err == io.EOF {
					break
				}
			}
		}

		// calc latency
		getObjTime := time.Since(start).Milliseconds()
		boltGetObjTimes = append(boltGetObjTimes, getObjTime)

		// count object.
		if (output.ContentEncoding != nil &&
			len(*output.ContentEncoding) > 0 && *output.ContentEncoding == "gzip") ||
			strings.HasSuffix(key, ".gz") {
			boltCmpObjCount++
		} else {
			boltUnCmpObjCount++
		}

		// get object sizes.
		boltObjSizes = append(boltObjSizes, *output.ContentLength)
		// close response stream.
		output.Body.Close()
	}

	// calc s3 perf stats.
	s3GetObjPerfStats := p.computePerfStats(s3GetObjTimes, nil, s3ObjSizes)

	// calc bolt perf stats.
	boltGetObjPerfStats := p.computePerfStats(boltGetObjTimes, nil, boltObjSizes)

	var s3GetObjStatName, boltGetObjStatName string
	if p.requestType == "GET_OBJECT_TTFB" {
		s3GetObjStatName = "s3_get_obj_ttfb_perf_stats"
		boltGetObjStatName = "bolt_get_obj_ttfb_perf_stats"
	} else {
		s3GetObjStatName = "s3_get_obj_perf_stats"
		boltGetObjStatName = "bolt_get_obj_perf_stats"
	}

	s3Count := &ObjectCount{
		Compressed:   fmt.Sprintf("%d", s3CmpObjCount),
		Uncompressed: fmt.Sprintf("%d", s3UnCmpObjCount),
	}

	boltCount := &ObjectCount{
		Compressed:   fmt.Sprintf("%d", boltCmpObjCount),
		Uncompressed: fmt.Sprintf("%d", boltUnCmpObjCount),
	}

	getObjPerfRespMap := make(map[string]interface{})
	getObjPerfRespMap[s3GetObjStatName] = s3GetObjPerfStats
	getObjPerfRespMap[boltGetObjStatName] = boltGetObjPerfStats
	getObjPerfRespMap["s3Count"] = s3Count
	getObjPerfRespMap["boltCount"] = boltCount
	return getObjPerfRespMap, nil
}

func (p *BoltS3Perf) getObjectPassthroughPerf(bucket string) (map[string]interface{}, error) {
	var boltGetObjTimes []int64

	var boltObjSizes []int64

	boltCmpObjCount := 0
	boltUnCmpObjCount := 0

	// Get Objects via passthrough from Bolt.
	for _, key := range p.keys {

		getObjInput := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key: aws.String(key),
		}

		req, output := p.boltSvc.GetObjectRequest(getObjInput)
		req.HTTPRequest.Header.Set("Accept-Encoding", "gzip")
		start := time.Now()
		if err := req.Send(); err != nil {
			return nil, err
		}

		// If getting first byte object latency, read at most 1 byte,
		// otherwise read the entire body.
		if p.requestType == "GET_OBJECT_PASSTHROUGH_TTFB" {
			// read only the first byte from the stream.
			buf := make([]byte, 1)
			_, _ = output.Body.Read(buf)
		} else {
			// read all data from the stream.
			buf := make([]byte, 4096)
			for {
				_, err := output.Body.Read(buf)
				if err == io.EOF {
					break
				}
			}
		}

		// calc latency
		getObjTime := time.Since(start).Milliseconds()
		boltGetObjTimes = append(boltGetObjTimes, getObjTime)

		// count object.
		if (output.ContentEncoding != nil &&
			len(*output.ContentEncoding) > 0 && *output.ContentEncoding == "gzip") ||
			strings.HasSuffix(key, ".gz") {
			boltCmpObjCount++
		} else {
			boltUnCmpObjCount++
		}

		// get object sizes.
		boltObjSizes = append(boltObjSizes, *output.ContentLength)
		// close response stream.
		output.Body.Close()
	}

	// calc bolt perf stats.
	boltGetObjPtPerfStats := p.computePerfStats(boltGetObjTimes, nil, boltObjSizes)

	var boltGetObjPtStatName string
	if p.requestType == "GET_OBJECT_PASSTHROUGH_TTFB" {
		boltGetObjPtStatName = "bolt_get_obj_pt_ttfb_perf_stats"
	} else {
		boltGetObjPtStatName = "bolt_get_obj_pt_perf_stats"
	}

	boltCount := &ObjectCount{
		Compressed:   fmt.Sprintf("%d", boltCmpObjCount),
		Uncompressed: fmt.Sprintf("%d", boltUnCmpObjCount),
	}

	getObjPtPerfRespMap := make(map[string]interface{})
	getObjPtPerfRespMap[boltGetObjPtStatName] = boltGetObjPtPerfStats
	getObjPtPerfRespMap["boltCount"] = boltCount
	return getObjPtPerfRespMap, nil
}

func (p *BoltS3Perf) allPerf(bucket string) (map[string]interface{}, error) {
	// Put, Delete Object Perf tests using generated key names.
	putObjPerfStats, err := p.putObjectPerf(bucket)
	if err != nil {
		return nil, err
	}

	delObjPerfStats, err := p.deleteObjectPerf(bucket)
	if err != nil {
		return nil, err
	}

	// List Objects perf tests on existing objects.
	listObjPerfStats, err := p.listObjectsV2Perf(bucket)
	if err != nil {
		return nil, err
	}

	// Get the list of objects before get object perf test.
	err = p.listObjectsV2(bucket)
	if err != nil {
		return nil, err
	}

	getObjPerfStats, err := p.getObjectPerf(bucket)
	if err != nil {
		return nil, err
	}

	// merge all perf stats
	mergedPerfStats := p.mergePerfStats(putObjPerfStats, delObjPerfStats, listObjPerfStats, getObjPerfStats)
	return mergedPerfStats, nil
}

func (p *BoltS3Perf) mergePerfStats(perfStats ...map[string]interface{}) map[string]interface{} {
	mergedPerfStats := make(map[string]interface{})

	for _, perfStat := range perfStats {
		for statName, statVal := range perfStat {
			mergedPerfStats[statName] = statVal
		}
	}
	return mergedPerfStats
}

func (p *BoltS3Perf) listObjectsV2(bucket string) error {
	listObjsV2Input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	resp, err := p.s3Svc.ListObjectsV2(listObjsV2Input)
	if err != nil {
		return err
	}

	for _, item := range resp.Contents {
		p.keys = append(p.keys, *item.Key)
	}
	return nil
}

func (p *BoltS3Perf) generateKeyNames(numObjects int)  {
	for i := 0; i < numObjects; i++ {
		key := "bolt-s3-perf" + strconv.Itoa(i)
		p.keys = append(p.keys, key)
	}
}

func (p *BoltS3Perf) generate(objLength int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, objLength)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func (p *BoltS3Perf) computePerfStats(opTimes []int64, opTp []float64, objSizes []int64) *PerfStats {

	perfStats := &PerfStats{}

	// calc op latency perf
	sort.SliceStable(opTimes, func(i, j int) bool {
		return opTimes[i] < opTimes[j]
	})

	var opTimesSum int64 = 0
	for _, opTime := range opTimes {
		opTimesSum += opTime
	}

	opAvgTime := float64(opTimesSum) / float64(len(opTimes))
	opTimeP50 := opTimes[len(opTimes) / 2]
	opTimeP90 := opTimes[int(float64(len(opTimes)) * 0.9)]

	latencyPerfStats := &PerfStat{
		Average: fmt.Sprintf("%.2f ms", opAvgTime),
		P50:     fmt.Sprintf("%d ms", opTimeP50),
		P90:     fmt.Sprintf("%d ms", opTimeP90),
	}
	perfStats.Latency = latencyPerfStats

	// calc op throughput perf.
	var opAvgTp,opTpP50, opTpP90 float64
	if opTp != nil {
		sort.Float64s(opTp)

		var opTpSum = 0.0
		for _, tp := range opTp {
			opTpSum += tp
		}

		opAvgTp = opTpSum / float64(len(opTp))
		opTpP50 = opTp[len(opTp) / 2]
		opTpP90 = opTp[int(float64(len(opTp)) * 0.9)]

		tpPerfStats := &PerfStat{
			Average: fmt.Sprintf("%.2f objects/ms", opAvgTp),
			P50:     fmt.Sprintf("%.2f objects/ms", opTpP50),
			P90:     fmt.Sprintf("%.2f objects/ms", opTpP90),
		}
		perfStats.Throughput = tpPerfStats
	} else {
		tp := float64(len(opTimes)) / float64(opTimesSum)
		perfStats.ThroughputT = fmt.Sprintf("%.2f objects/ms", tp)
	}

	// calc obj size metrics.
	if objSizes != nil {
		sort.SliceStable(objSizes, func(i, j int) bool {
			return objSizes[i] < objSizes[j]
		})

		var objSizesSum int64 = 0
		for _, objSize := range objSizes {
			objSizesSum += objSize
		}

		objAvgSize := float64(objSizesSum) / float64(len(objSizes))
		objSizeP50 := objSizes[len(objSizes) / 2]
		objSizeP90 := objSizes[int(float64(len(objSizes)) * 0.9)]

		objSizesPerfStats := &PerfStat{
			Average: fmt.Sprintf("%.2f bytes", objAvgSize),
			P50:     fmt.Sprintf("%d bytes", objSizeP50),
			P90:     fmt.Sprintf("%d bytes", objSizeP90),
		}
		perfStats.ObjectSize = objSizesPerfStats
	}
	return perfStats
}