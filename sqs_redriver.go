package sqsrd

import (
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
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
	Svc              *sqs.SQS
	SourceQueue      string
	DestQueue        string
	MaxEmptyReceives int
}

func (s SqsRedriver) SendMessages(messages <-chan *sqs.Message, parallelism int) {
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
			s.Svc.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueUrl: aws.String(s.DestQueue),
				Entries:  sendMessageBatch[:currentIdx],
			})
			s.Svc.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(s.SourceQueue),
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
				s.Svc.SendMessageBatch(&sqs.SendMessageBatchInput{
					QueueUrl: aws.String(s.DestQueue),
					Entries:  sendMessageBatch[:],
				})
				s.Svc.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
					QueueUrl: aws.String(s.SourceQueue),
					Entries:  deleteMessageBatch[:],
				})
				log.Println("Sent 10 messages")
				currentIdx = 0
			}
		}
	}
}

func (s SqsRedriver) ReceiveMessages(messages chan<- *sqs.Message, parallelism int) {
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

	for emptyReceives < s.MaxEmptyReceives {
		resp, err := s.Svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames:        []*string{aws.String("All")},
			MaxNumberOfMessages:   aws.Int64(10),
			MessageAttributeNames: []*string{aws.String("All")},
			QueueUrl:              aws.String(s.SourceQueue),
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
