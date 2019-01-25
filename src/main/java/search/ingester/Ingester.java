package search.ingester;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.xcontent.XContentType;
import org.xml.sax.SAXException;
import search.ingester.models.Document;
import search.ingester.models.Message;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.*;

import java.util.Base64;
import java.util.Set;
import java.util.stream.Collectors;

public class Ingester implements RequestHandler<SQSEvent, Void> {
    /**
     * Environment variable names
     */
    private static final String ENV_AWS_REGION = "AWS_REGION";
    private static final String ENV_ES_ENDPOINT = "ES_ENDPOINT";
    private static final String ENV_ES_MAX_PAYLOAD_SIZE = "ES_MAX_PAYLOAD_SIZE";
    private static final String ENV_ES_PIPELINE = "ES_PIPELINE";
    private static final String ENV_ES_DOCTYPE = "ES_DOCTYPE";

    /**
     * Available actions
     */
    private static final String UPSERT = "upsert";
    private static final String DELETE = "delete";

    /**
     * Elasticsearch parameters
     */
    private static final String ES = "es";

    /**
     * Tika parameters
     */
    private static final int TIKA_MAX_CHARACTER_LIMIT = -1;

    // Only set up if we need to read an S3 message, otherwise left as null
    private AmazonS3 s3Client;

    /**
     * Handle an incoming SQS Message and insert into or delete from the relevant search index on a specified AWS
     * Elasticsearch index
     *
     * @param event An incoming SQS event
     * @param context Context object for that incoming event
     * @throws RuntimeException Throws a runtime exception in the case of any caught exceptions
     * @return Returns null if successful or throws a RuntimeException if something goes wrong
     */
    public Void handleRequest(SQSEvent event, Context context)  {
        for (SQSMessage msg : event.getRecords()) {
            String bucket = "", key = "";
            Jsonb jsonb = JsonbBuilder.create();
            Message message = jsonb.fromJson(msg.getBody(), Message.class);

            if (message.getS3BucketName() != null && message.getS3Key() != null) {
                this.s3Client = getS3Client();
                bucket = message.getS3BucketName();
                key = message.getS3Key();

                try {
                    message = getMessageFromS3(bucket, key);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            Document document = message.getDocument();

            if (document.getFileBase64() != null) {
                /** Something odd happening on some files being submitted to elasticsearch
                if (s3Client == null ||
                        (s3Client != null
                                && s3Client.getObjectMetadata(bucket, key).getContentLength()
                                    > Integer.parseInt(System.getenv(ENV_ES_MAX_PAYLOAD_SIZE)))) {
                    try {
                        document = parseFile(document);
                    } catch (Exception err) {
                        throw new RuntimeException(err);
                    }
                }**/
                try {
                    document = parseFile(document);
                } catch (Exception err) {
                    throw new RuntimeException(err);
                }
            }

            // Do some validation
            ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
            Validator validator = factory.getValidator();
            Set<ConstraintViolation<Document>> violations = validator.validate(document);

            if (violations.size() > 0) {
                throw new RuntimeException(
                        violations.stream()
                                .map(violation -> violation.getPropertyPath().toString() + ": " + violation.getMessage())
                                .collect(Collectors.joining("\n")));
            }

            // Send index request
            if (message.getVerb().equals(UPSERT)) {
                try {
                    IndexRequest req = new IndexRequest(message.getIndex(), System.getenv(ENV_ES_DOCTYPE), document.getId());
                    req.source(jsonb.toJson(document), XContentType.JSON);
                    // If a pipeline is specified, use it
                    if (System.getenv(ENV_ES_PIPELINE) != null) {
                        req.setPipeline(System.getenv(ENV_ES_PIPELINE));
                    }
                    IndexResponse resp = esClient().index(req, RequestOptions.DEFAULT);
                    if (!(resp.getResult() == DocWriteResponse.Result.CREATED
                            || resp.getResult() == DocWriteResponse.Result.UPDATED)) {
                        throw new RuntimeException(
                                String.format("Index Response return was not as expected got (%d) with the following " +
                                        "returned %s", resp.status().getStatus(), resp.toString()));
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } else if (message.getVerb().equals(DELETE)) {
                DeleteRequest req = new DeleteRequest(message.getIndex(), System.getenv(ENV_ES_DOCTYPE), document.getId());
                try {
                    DeleteResponse resp = esClient().delete(req, RequestOptions.DEFAULT);
                    if (resp.getResult() != DocWriteResponse.Result.DELETED) {
                        throw new RuntimeException(
                                String.format("Index Response return was not as expected got (%d) with the following " +
                                        "returned %s", resp.status().getStatus(), resp.toString()));
                    }
                } catch(IOException ex) {
                    throw new RuntimeException(ex);
                }

            } else {
                throw new RuntimeException(String.format("Expected verb to be 'upsert' or 'delete' but got %s",
                        message.getVerb()));
            }

            // Cleanup if S3 message
            if (this.s3Client != null) {
                deleteMessageFromS3(bucket, key);
            }
        }
        return null;
    }

    /**
     * Create configured a High Level Elasticsearch REST client with an AWS http interceptor to sign the data package
     * being sent
     *
     * @return A Configured High Level Elasticsearch REST client to send packets to an AWS ES service
     */
    private RestHighLevelClient esClient() {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(ES);
        signer.setRegionName(System.getenv(ENV_AWS_REGION));
        HttpRequestInterceptor interceptor =
                new AWSRequestSigningApacheInterceptor(ES, signer, new DefaultAWSCredentialsProviderChain());
        return new RestHighLevelClient(
                RestClient.builder(HttpHost.create(System.getenv(ENV_ES_ENDPOINT)))
                        .setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor)));
    }

    /**
     * Extracts a message from an S3 file (JSON)
     *
     * @param bucket The bucket that the file exists in
     * @param key The full key of the file in the S3 Bucket
     * @return A translated Message object generated from the JSON read from the given S3 file
     * @throws IOException If the S3 file cannot be streamed down from S3 this error will be thrown
     */
    private Message getMessageFromS3(String bucket, String key) throws IOException {
        // Get the object reference and build a buffered reader around it
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucket, key));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));

        // Extract the JSON object as text from the input stream
        String text = "";
        String line;
        while ((line = reader.readLine()) != null){
            text = text + "\n" + line;
        }

        // Return the extracted message object from the S3 JSON file
        return JsonbBuilder.create().fromJson(text, Message.class);
    }

    /**
     * Deletes a given file from S3 at the provided bucket / key location, this may fail but its not important if it
     * does as it will be eventually caught by the buckets' lifecycle rules
     *
     * @param bucket The bucket to delete from
     * @param key The full key of the object to be removed from the bucket
     */
    private void deleteMessageFromS3(String bucket, String key) {
        DeleteObjectRequest req = new DeleteObjectRequest(bucket, key);
        s3Client.deleteObject(req);
    }

    /**
     * Returns a configured S3 client using environment variables / default provider chains
     *
     * @return A configured S3 client
     */
    private AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(System.getenv(ENV_AWS_REGION))
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();
    }

    /**
     * Creates a document template from an existing document template with an attached base64 encoded file in the
     * content_base64 field. Attempt to overwrite the relevant parts of the given document template and remove extra
     * whitespace characters from the content as they are not needed
     *
     * @param document A document template with a base64 encoded file attached in the content_base64 field
     * @return A Document object with the base64 files text content and title in place of the given document template
     * @throws IOException Thrown on an issue with opening the input stream containing the base64 encoded file
     * @throws SAXException Thrown as part of the Tika package parsing the given document
     * @throws TikaException Thrown as part of the Tika package parsing the given document
     */
    private Document parseFile(Document document) throws IOException, SAXException, TikaException {
        // Create auto document parser and try to extract some textual info from the base64 encoded string passed to it
        BodyContentHandler handler = new BodyContentHandler(TIKA_MAX_CHARACTER_LIMIT);
        AutoDetectParser parser = new AutoDetectParser();
        Metadata metadata = new Metadata();
        InputStream stream = new ByteArrayInputStream(Base64.getDecoder().decode(document.getFileBase64()));

        try {
            parser.parse(stream, handler, metadata);
        } catch(SAXException ex) {
            if (ex.getClass().getCanonicalName() != "org.apache.tika.sax.WriteOutContentHandler$WriteLimitReachedException") {
                throw ex;
            } else {
                System.out.println(String.format("Got more characters than current Tika limit (%d), truncating to limit", TIKA_MAX_CHARACTER_LIMIT));
            }
        }

        // Grab the extracted content from the parser and strip out all repeated whitespace characters as we don't need
        // them, if no content don't replace the existing content
        String newContent = handler.toString().replaceAll("\\s+", " ").trim();
        if (!newContent.isEmpty()) {
            document.setContent(newContent);
        }

        // If a title exists in the document metadata replace the document title with it
        if (metadata.get("title") != null) {
            document.setTitle(metadata.get("title"));
        }

        // Clear b64 encoded file
        document.setFileBase64(null);

        return document;
    }
}
