package main

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	bucketName = "<bucket_name>"
	depth      = 2
	maxKeys    = 100
)

func extractDateFromString(s string) (time.Time, error) {
	var datePattern = regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})-(\d{2})`)
	match := datePattern.FindStringSubmatch(s)
	if match == nil {
		return time.Time{}, fmt.Errorf("date not found in string: %s", s)
	}

	year, month, day, hour := match[1], match[2], match[3], match[4]
	layout := "2006-01-02-15"
	t, err := time.Parse(layout, fmt.Sprintf("%s-%s-%s-%s", year, month, day, hour))
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse date string: %s", err)
	}

	return t, nil
}

func getNewFilePath(event string, date time.Time, filename string) string {
	return fmt.Sprintf("%s/year=%04d/month=%02d/day=%02d/hour=%02d/%s", event, date.Year(), date.Month(), date.Day(), date.Hour(), filename)
}

func logic(path []string, fileKey string, svc *s3.S3, wg *sync.WaitGroup) error {
	defer wg.Done()
	event := path[0]
	fileName := path[1]

	date, err := extractDateFromString(fileName)
	if err != nil {
		fmt.Println("ExtractDateFromString", err, fileKey)
		return nil
	}

	newFilePath := getNewFilePath(event, date, fileName)

	_, err = svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, fileKey)),
		Key:        aws.String(newFilePath),
	})
	if err != nil {
		fmt.Println("Copy", err, fileKey)
		return nil
	}

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileKey),
	})
	if err != nil {
		fmt.Println("Delete", err, fileKey)
		return nil
	}

	return nil
}

func splitKey(key string) []string {
	split := strings.Split(key, "/")
	var nonEmpty []string
	for _, elem := range split {
		if elem != "" {
			nonEmpty = append(nonEmpty, elem)
		}
	}
	return nonEmpty
}

func partition() {
	fmt.Println("Processing files....")
	total := 0
	var wg sync.WaitGroup
	session := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := s3.New(session, &aws.Config{Region: aws.String("us-east-1")})
	var continuationToken *string
	for {
		params := &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int64(maxKeys),
		}
		if continuationToken != nil {
			params.ContinuationToken = continuationToken
		}

		result, err := svc.ListObjectsV2(params)
		if err != nil {
			fmt.Println("error listing objects:", err)
			return
		}
		total += len(result.Contents)
		for _, obj := range result.Contents {
			path := splitKey(*obj.Key)
			if len(path) == depth {
				wg.Add(1)
				go logic(path, *obj.Key, svc, &wg)
			}
		}
		wg.Wait()

		if !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}
	fmt.Println("Total files", total)
}

func main() {
	fmt.Println("############## Start ##################")
	partition()
	fmt.Println("############## Finish ##################")
}
