package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hashicorp/go-hclog"
)

type S3Client struct {
	s3     *s3.S3
	logger hclog.Logger
}

func (c *S3Client) ScanBucket(bucket, prefix string) ([]*s3.Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	var objs []*s3.Object
	err := c.s3.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, o := range page.Contents {
			objs = append(objs, o)
		}
		return !lastPage
	})
	if err != nil {
		return nil, err
	}
	return objs, nil
}

func (c *S3Client) ScanBucketChan(bucket, prefix string) <-chan *s3.Object {
	out := make(chan *s3.Object, 1000)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	go func() {
		err := c.s3.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, o := range page.Contents {
				out <- o
			}
			return !lastPage
		})
		close(out)
		if err != nil {
			c.logger.Error(err.Error())
		}
	}()
	return out
}

type Day struct {
	name string
	objs []*s3.Object
}

func (c *S3Client) ScanDaysCh(ctx context.Context, bucket, prefix string) <-chan *Day {
	out := make(chan *Day, 1)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	go func() {
		var day Day
		err := c.s3.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if ctx.Err() != nil {
				c.logger.Info(ctx.Err().Error())
				return false
			}
			for _, o := range page.Contents {
				if day.name == "" {
					day.name = dayString(o)
				}
				if len(day.objs) != 0 && day.name != dayString(o) {
					d := day
					out <- &d
					day = Day{}
				}
				day.objs = append(day.objs, o)
			}
			if lastPage {
				out <- &day
				return false
			}
			return true
		})
		close(out)
		if err != nil {
			c.logger.Error(err.Error())
		}
	}()
	return out
}

func DedupeObjects(objs []*s3.Object) []*s3.Object {
	set := make(map[string]struct{})
	out := make([]*s3.Object, 0, len(objs))
	for _, obj := range objs {
		base := baseName(*obj.Key)
		if _, ok := set[base]; !ok {
			out = append(out, obj)
			set[base] = struct{}{}
		}
	}
	return out
}

func (c *S3Client) MoveObj(obj *s3.Object, srcBucket, dstBucket, newPrefix string, deleteAfter bool) error {
	newKey := newKey(*obj.Key, newPrefix)
	copyIn := &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket),
		CopySource: aws.String(srcBucket + "/" + *obj.Key),
		Key:        aws.String(newKey),
	}
	_, err := c.s3.CopyObject(copyIn)
	if err != nil {
		return fmt.Errorf("failed moving %s: %w", *obj.Key, err)
	}
	if deleteAfter {
		if err = c.DeleteObj(srcBucket, *obj.Key); err != nil {
			return err
		}
	}
	c.logger.Debug("moved object",
		"srcBucket", srcBucket,
		"dstBucket", dstBucket,
		"oldKey", *obj.Key,
		"newKey", newKey)
	return nil
}

func (c *S3Client) DeleteObj(srcBucket, key string) error {
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(key),
	}
	_, err := c.s3.DeleteObject(deleteInput)
	if err != nil {
		return fmt.Errorf("unable to delete %s: %w", key, err)
	}
	return nil
}

func newKey(currentKey, newPrefx string) string {
	split := strings.Split(currentKey, "/")
	item := split[len(split)-1]
	newPrefix := strings.TrimRight(newPrefx, "/")
	return newPrefix + "/" + item
}

func baseName(key string) string {
	splt := strings.Split(key, "/")
	return splt[len(splt)-1]
}

func dayString(obj *s3.Object) string {
	//2019-02-23
	base := baseName(*obj.Key)
	return base[:10]
}
