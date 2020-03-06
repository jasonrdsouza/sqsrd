package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func handleErrors(err error) {
	// todo: handle recoverable errors more gracefully
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			log.Fatalf("operation canceled due to timeout, %v\n", err)
		} else {
			log.Fatalf("operation failed, %v\n", err)
		}
	}
}

type SqsRedriver struct {
	svc              *sqs.SQS
	sourceQueue      string
	destQueue        string
	maxEmptyReceives int
}

func (s SqsRedriver) sendMessages(messages <-chan *sqs.Message, parallelism int) {
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go s.sendMessageWorker(messages, &wg)
	}
	wg.Wait()
}

func (s SqsRedriver) sendMessageWorker(messages <-chan *sqs.Message, wg *sync.WaitGroup) {
	// chunk up into batches of 10 and send to sqs via underlying client
	var sendMessageBatch [10]*sqs.SendMessageBatchRequestEntry
	var deleteMessageBatch [10]*sqs.DeleteMessageBatchRequestEntry
	currentIdx := 0

	for {
		message, more := <-messages

		if !more {
			// clean up by sending any enqueued messages
			// todo: error checking
			s.svc.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueUrl: aws.String(s.destQueue),
				Entries:  sendMessageBatch[:currentIdx],
			})
			s.svc.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(s.sourceQueue),
				Entries:  deleteMessageBatch[:currentIdx],
			})
			log.Printf("Sent %v messages\n", currentIdx)
			// and then report that you're done
			wg.Done()
		} else {
			sendMessageBatch[currentIdx] = &sqs.SendMessageBatchRequestEntry{
				Id:                message.MessageId,
				MessageAttributes: message.MessageAttributes,
				MessageBody:       message.Body,
			}
			deleteMessageBatch[currentIdx] = &sqs.DeleteMessageBatchRequestEntry{
				Id:            message.MessageId,
				ReceiptHandle: message.ReceiptHandle,
			}
			currentIdx++

			if currentIdx == 10 {
				// todo: error checking
				s.svc.SendMessageBatch(&sqs.SendMessageBatchInput{
					QueueUrl: aws.String(s.destQueue),
					Entries:  sendMessageBatch[:],
				})
				s.svc.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
					QueueUrl: aws.String(s.sourceQueue),
					Entries:  deleteMessageBatch[:],
				})
				log.Println("Sent 10 messages")
				currentIdx = 0
			}
		}
	}
}

func (s SqsRedriver) receiveMessages(messages chan<- *sqs.Message, parallelism int) {
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go s.receiveMessageWorker(messages, &wg)
	}
	wg.Wait()

	// done redriving, so close channel
	close(messages)
}

func (s SqsRedriver) receiveMessageWorker(messages chan<- *sqs.Message, wg *sync.WaitGroup) {
	emptyReceives := 0

	for emptyReceives < s.maxEmptyReceives {
		resp, err := s.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames:        []*string{aws.String("All")},
			MaxNumberOfMessages:   aws.Int64(10),
			MessageAttributeNames: []*string{aws.String("All")},
			QueueUrl:              aws.String(s.sourceQueue),
			//WaitTimeSeconds:       aws.Int64(2),
		})
		handleErrors(err)

		if len(resp.Messages) == 0 {
			emptyReceives++
			log.Printf("Empty Receive #%v", emptyReceives)
		} else {
			log.Printf("Received %v messages", len(resp.Messages))
		}

		for _, message := range resp.Messages {
			messages <- message
		}
	}

	wg.Done()
}

func injectFakeMessages(numFakeMessages int, channel chan<- *sqs.Message) {
	for i := 0; i < numFakeMessages; i++ {
		message := sqs.Message{}
		message.SetMessageId(strconv.Itoa(i))
		message.SetBody(fmt.Sprintf("Test body #%v", i))
		message.SetReceiptHandle(strconv.Itoa(i))
		channel <- &message
	}
	close(channel)
}

func main() {
	var redriver SqsRedriver
	var region, profile string
	var timeout time.Duration
	var parallelism, bufferSize int

	flag.StringVar(&redriver.sourceQueue, "source", "", "Source Queue URL.")
	flag.StringVar(&redriver.destQueue, "dest", "", "Destination Queue URL.")
	flag.DurationVar(&timeout, "timeout", time.Minute*5, "Upload timeout.")
	flag.IntVar(&redriver.maxEmptyReceives, "emptyReceives", 3, "Maximum empty message receives.")
	flag.IntVar(&parallelism, "parallelism", 1, "Parallelism to run with.")
	flag.IntVar(&bufferSize, "buffer", 100, "Size of message buffer to keep in memory.")
	flag.StringVar(&region, "region", endpoints.UsEast1RegionID, "AWS Region.")
	flag.StringVar(&profile, "profile", "default", "Optional credentials profile to use")
	flag.Parse()

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewSharedCredentials("", profile),
	}))

	redriver.svc = sqs.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	if cancelFn != nil {
		defer cancelFn()
	}

	messagesToRedrive := make(chan *sqs.Message, bufferSize)
	go redriver.receiveMessages(messagesToRedrive, parallelism)
	//injectFakeMessages(100, messagesToRedrive)
	redriver.sendMessages(messagesToRedrive, parallelism)
}
