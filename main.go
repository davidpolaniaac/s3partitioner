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
	depth      = 1
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

func getNewFilePath(prefix string, date time.Time, fileName string) string {
	partition := fmt.Sprintf("year=%04d/month=%02d/day=%02d/hour=%02d/%s", date.Year(), date.Month(), date.Day(), date.Hour(), fileName)
	if len(prefix) != 0 {
		return fmt.Sprintf("%s/%s", prefix, partition)
	}
	return partition
}

func getPrefix(path []string) string {
	if len(path) > depth {
		return strings.Join(path[:depth], "/")
	}
	return ""
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

func getPartition(key string) (string, error) {
	path := splitKey(key)
	prefix := getPrefix(path)
	fileName := path[len(path)-1]
	date, err := extractDateFromString(fileName)
	if err != nil {
		fmt.Println("ExtractDateFromString", err, key)
		return "", fmt.Errorf("date not found in string: %s", fileName)
	}
	newKey := getNewFilePath(prefix, date, fileName)

	if key != newKey {
		return newKey, nil
	} else {
		return "", fmt.Errorf("file is ok: %s == %s", key, newKey)
	}
}

func logic(fileKey string, svc *s3.S3, wg *sync.WaitGroup) error {
	defer wg.Done()

	newKey, err := getPartition(fileKey)
	if err != nil {
		return nil
	}
	_, err = svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, fileKey)),
		Key:        aws.String(newKey),
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

func isFile(key string) bool {
	return !strings.HasSuffix(key, "/")
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
			if isFile(*obj.Key) {
				wg.Add(1)
				go logic(*obj.Key, svc, &wg)
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
