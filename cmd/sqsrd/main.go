package main

import (
	"flag"
	"fmt"
	"strconv"

	"dsouza.io/sqsrd"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

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
	var redriver sqsrd.SqsRedriver
	var region, profile string
	var parallelism, bufferSize int

	flag.StringVar(&redriver.SourceQueue, "source", "", "Source Queue URL.")
	flag.StringVar(&redriver.DestQueue, "dest", "", "Destination Queue URL.")
	flag.IntVar(&redriver.MaxEmptyReceives, "emptyReceives", 3, "Maximum empty message receives.")
	flag.IntVar(&redriver.VisibilityTimeout, "visibility", 60, "Redriven message visibility timeout (in seconds)")
	flag.IntVar(&parallelism, "parallelism", 1, "Parallelism to run with.")
	flag.IntVar(&bufferSize, "buffer", 100, "Size of message buffer to keep in memory.")
	flag.StringVar(&region, "region", endpoints.UsEast1RegionID, "AWS Region.")
	flag.StringVar(&profile, "profile", "default", "Optional credentials profile to use")
	flag.Parse()

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String(region),
		},
		Profile: profile,
		// Enable MFA support
		AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
		// Enable Shared Config support
		SharedConfigState: session.SharedConfigEnable,
	}))

	redriver.Svc = sqs.New(sess)

	messagesToRedrive := make(chan *sqs.Message, bufferSize)
	go redriver.ReceiveMessages(messagesToRedrive, parallelism)
	//injectFakeMessages(100, messagesToRedrive)
	redriver.SendMessages(messagesToRedrive, parallelism)
}
