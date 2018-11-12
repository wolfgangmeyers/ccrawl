package commoncrawl

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3OutputFileWriter implements the OutputFileWriter interface.
// Writes files to an S3 bucket
type S3OutputFileWriter struct {
	s3BucketName string
	s3Connection *s3.S3
}

// NewS3OutputFileWriter returns a new instance of NewS3OutputFileWriter
//
// * regionName - AWS region
// * s3BucketName - S3 output bucket name
func NewS3OutputFileWriter(regionName string, s3BucketName string) *S3OutputFileWriter {
	fileWriter := new(S3OutputFileWriter)
	fileWriter.s3Connection = s3.New(session.New(), &aws.Config{Region: aws.String(regionName)})
	fileWriter.s3BucketName = s3BucketName
	return fileWriter
}

// WriteOutputFile writes out the specified file to the local filesystem
func (fileWriter *S3OutputFileWriter) WriteOutputFile(filepath string, data []byte) error {
	// // Send the file to S3
	input := s3.PutObjectInput{
		Bucket: aws.String(fileWriter.s3BucketName),
		Key:    aws.String(filepath),
		Body:   bytes.NewReader(data),
	}
	// TODO: could retry this a few times with a backoff...
	_, err := fileWriter.s3Connection.PutObject(&input)
	return err
}
