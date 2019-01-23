# elasticsearch-lambda-ingester

A Java lambda function to read messages from an SQS queue (using S3 to store large messages), optionally runs the
content through the Tika library to extract textual file contents i.e. pdf's, office documents, etc...

## Build instructions

To build the lambda function into a deploy .jar artefact just run the following which should produce a working .jar file
in the target folder;

```
mvn package shade:shade
```
