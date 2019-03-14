package search.ingester;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.io.*;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import search.ingester.models.Message;

public class Ingester implements RequestHandler<SQSEvent, Void> {

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
    public Void handleRequest(SQSEvent event, Context context) {

        // unless the messages are "batched", we only expect one message,
        // but use a loop anyway in case they are batched in the future
        for (SQSMessage msg : event.getRecords()) {
            
            // uncomment to log the message body in cloudwatch
            // System.out.println(msg.getBody());

            // deserialize a Message from the JSON body of the SQS message 
            Jsonb jsonb = JsonbBuilder.create();
            Message message = jsonb.fromJson(msg.getBody(), Message.class);

            // workaround Java's checked exceptions
            try {
                handleMessage(message, new Processor(new ElasticService(new Env()), new FileParser()));
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        // apparently you return null from an AWS Lambda handler on success?
        return null;
    }

    void handleMessage(Message original, Processor ingester) throws IOException {

        Message m = resolveMessagePossiblyFromS3(original);

        ingester.process(m);

        if (this.s3Client != null) {
            deleteObjectFromS3(m.getS3BucketName(), m.getS3Key());
        }
    }

    private Message resolveMessagePossiblyFromS3(Message original) throws IOException {

        // the "real" message might be on S3 storage via the SQS Extended Client
        // in which case we will have two properties pointing to the S3 object
        boolean isMessageReallyOnS3 = original.getS3BucketName() != null && original.getS3Key() != null;

        if (isMessageReallyOnS3) {
            this.s3Client = getS3Client();
            return getMessageFromS3(original.getS3BucketName(), original.getS3Key());
        }
        else {
            return original;
        }
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
    private void deleteObjectFromS3(String bucket, String key) {
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
                .withRegion(new Env().AWS_REGION())
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();
    }
}
