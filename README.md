# SQS Message Redriver
Sqsrd is a utility to aid in "redriving" SQS messages. Typically, this is useful when an SQS queue has an associated [dead-letter queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html) (DLQ), and you wish to move messages from the DLQ back onto the source queue for reprocessing.

# Quickstart
Download the latest release of sqsrd
```
wget https://github.com/jasonrdsouza/sqsrd/releases/download/v0.2/sqsrd
```

Make the sqsrd binary executable (optionally, add it to your shell path)
```
chmod +x sqsrd
```

Run sqsrd with the desired options
```
# example help dialog
./sqsrd -h

# minimal example of redriving a DLQ
# replace region and queue URLs with your own values
sqsrd -region "us-west-2" -source "https://sqs.us-west-2.amazonaws.com/123456789000/sqs-queue-name" -dest "https://sqs.us-west-2.amazonaws.com/123456789000/sqs-queue-name-dlq"

# redrive using custom AWS credential profile
sqsrd -region "us-west-2" -profile "myprofile" -source "https://sqs.us-west-2.amazonaws.com/123456789000/sqs-queue-name" -dest "https://sqs.us-west-2.amazonaws.com/123456789000/sqs-queue-name-dlq"
```

By default, sqsrd will pull credentials from the [default AWS credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). It also supports using [named profiles](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) via the `-profile` flag.
