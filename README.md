# elasticsearch-lambda-ingester

JNCC Website (and microsites) Elasticsearch message queue handler.

This is an AWS lambda function written in Java to read messages from the dedicated SQS queue (using S3 to store large messages) and update the specified index.

It uses the Tika library to extract textual file contents i.e. PDFs, Office documents, etc...

## Development

One option is to use VS Code. I needed Java and Maven installed, of course, and I also installed the VS Code Java Extension pack: https://code.visualstudio.com/docs/languages/java

## Build

To build the lambda function into a deploy .jar artefact just run the following which should produce a working .jar file
in the target folder;

    mvn package shade:shade

## Deployment

- Bump the version in the `pom.xml` (the `project/version` node)
- Build `mvn package shade:shade`
- Upload the `.jar` in the `target` dir (not the `original...` one!) to the JNCC Deployment Artefacts bucket in the search-ingester "folder"
- Copy the path of the new `.jar` in the S3 console (by selecting it)
- In the `jncc-website-search-ingester-java` AWS Lambda Management console, choose 'Upload a file from Amazon S3'
