# Web Crawler

A scalable web crawler built for AWS Lambda with SQS and DynamoDB.

## Architecture

- **S3Manager**: Handles document storage in S3
- **QueueManager**: Manages URL queue using SQS
- **CrawlRecordManager**: Tracks crawl history in DynamoDB
- **Crawler**: Core crawling logic with Jsoup

## Features

- ✅ Stateless design for AWS Lambda
- ✅ Content change detection using SHA-256 hashes
- ✅ Duplicate URL prevention
- ✅ Multiple consumer support
- ✅ Automatic link discovery

## Setup

1. Configure AWS credentials
2. Run `mvn clean install`
3. Execute `Main.java` or deploy to Lambda

## Dependencies

- AWS SDK (S3, SQS, DynamoDB)
- Jsoup for HTML parsing
- Java 20

## Usage

```java
// Single execution
java Main

// Multiple consumers demo
java MultiConsumerDemo
```