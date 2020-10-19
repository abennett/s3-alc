package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hashicorp/go-hclog"
)

type LogLines [][]string

func (ll LogLines) Len() int {
	return len(ll)
}

func (ll LogLines) Less(i, j int) bool {
	// Mon Jan 2 15:04:05 -0700 MST 2006
	// 06/Feb/2019:00:00:38 +0000

	iTime, err := time.Parse("02/Jan/2006:15:04:05 -0700", ll[i][2])
	if err != nil {
		panic(err)
	}
	jTime, err := time.Parse("02/Jan/2006:15:04:05 -0700", ll[j][2])
	if err != nil {
		panic(err)
	}
	return iTime.Before(jTime)
}

func (ll LogLines) Swap(i, j int) {
	ll[i], ll[j] = ll[j], ll[i]
}

type S3Compactor struct {
	s3c    *S3Client
	log    hclog.Logger
	config *Config
}

func NewS3Compactor(log hclog.Logger, config *Config) (*S3Compactor, error) {
	sess := session.Must(session.NewSession())
	s3c := &S3Client{
		s3:     s3.New(sess, aws.NewConfig().WithRegion("us-west-2")),
		logger: log.Named("s3_client"),
	}
	return &S3Compactor{
		s3c:    s3c,
		log:    log,
		config: config,
	}, nil
}

func (cp *S3Compactor) Compact() error {
	os.Mkdir("compacted", 0700)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	days := cp.s3c.ScanDaysCh(ctx, cp.config.SrcBucket, cp.config.SourcePrefix)
	for day := range days {
		if ctx.Err() != nil {
			cp.log.Info("finished")
			return nil
		}
		readStart := time.Now()
		cp.log.Debug("reading group: " + day.name)
		ll, err := cp.readDay(ctx, day)
		cp.log.Debug("finished reading group: "+day.name, "runtime", time.Now().Sub(readStart).String())
		if err != nil {
			return err
		}
		reader, err := cp.writeLines(ll)
		if err != nil {
			return err
		}
		fname := "compacted/" + day.name + ".csv.gz"
		f, err := os.Create(fname)
		if err != nil {
			return err
		}
		if _, err = io.Copy(f, reader); err != nil {
			return err
		}
		logLine := fmt.Sprintf("wrote %d log lines to %s", len(ll), fname)
		cp.log.Info(logLine)
	}
	return nil
}

func (cp *S3Compactor) writeLines(ll LogLines) (io.ReadSeeker, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	cw := csv.NewWriter(gz)
	if err := cw.WriteAll(ll); err != nil {
		return nil, err
	}
	cw.Flush()
	gz.Close()
	return bytes.NewReader(b.Bytes()), nil
}

func (cp *S3Compactor) readDay(ctx context.Context, day *Day) (LogLines, error) {
	objCh := make(chan *s3.Object, len(day.objs))
	for _, obj := range day.objs {
		objCh <- obj
	}
	close(objCh)
	mergeCh := make(chan LogLines, 10)
	var wg sync.WaitGroup
	for x := 0; x <= runtime.NumCPU(); x++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range objCh {
				if ctx.Err() != nil {
					cp.log.Info("exceeded timeout")
					return
				}
				cp.log.Debug("processing " + *obj.Key)
				input := &s3.GetObjectInput{
					Bucket: aws.String(cp.config.SrcBucket),
					Key:    obj.Key,
				}
				output, err := cp.s3c.s3.GetObject(input)
				if err != nil {
					cp.log.Error(err.Error())
					return
				}
				l := extractLogLines(output.Body)
				if err != nil {
					cp.log.Error(err.Error())
					return
				}
				cp.log.Debug("finished extracting log lines from "+day.name, "day_lines", len(l))
				mergeCh <- l
			}
		}()
	}
	go func() {
		wg.Wait()
		close(mergeCh)
	}()
	var ll LogLines
	for l := range mergeCh {
		cp.log.Debug("merging lines", "lines_merged", len(l))
		ll = append(ll, l...)
	}
	//sort
	sort.Sort(ll)
	cp.log.Debug("done proccess log lines for "+day.name, "total_lines", len(ll))
	return ll, nil
}

func extractLogLines(rc io.ReadCloser) LogLines {
	var ll LogLines
	defer rc.Close()
	scanner := NewScanner(rc)
	for {
		line, atEOF := scanner.ScanLine()
		if len(line) > 0 {
			ll = append(ll, line)
		}
		if atEOF {
			break
		}
	}
	return ll
}
