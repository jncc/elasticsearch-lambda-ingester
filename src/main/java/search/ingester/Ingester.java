package search.ingester;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
import java.io.*;



import java.util.Base64;

public class Ingester implements RequestHandler<SQSEvent, Void> {
    private static final String UPSERT = "upsert";
    private static final String DELETE = "delete";

    public Void handleRequest(SQSEvent event, Context context)  {
        for (SQSMessage msg : event.getRecords()) {
            boolean isS3 = false;
            String bucket = "", key = "";

            Jsonb jsonb = JsonbBuilder.create();
            Message message = jsonb.fromJson(msg.getBody(), Message.class);

            if (message.getS3Bucket() != null && message.getS3Key() != null) {
                // Is S3 message
                System.out.println("Is an S3 message");
                isS3 = true;
                bucket = message.getS3Bucket();
                key = message.getS3Key();

                try {
                    message = getMessageFromS3(bucket, key);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

            }

            Document document = message.getDocument();

            if (document.getContentBase64() != null) {
                try {
                    document = parseFile(document);
                } catch (Exception err) {
                    throw new RuntimeException(err);
                }
            }

            // Send index request
            if (message.getVerb().equals(UPSERT)) {
                try {
                    IndexRequest req = new IndexRequest(message.getIndex(), "_doc", document.getId());
                    req.source(jsonb.toJson(document), XContentType.JSON);
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
                DeleteRequest req = new DeleteRequest(message.getIndex(), "_doc", document.getId());
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
            if (isS3) {
                deleteMessageFromS3(bucket, key);
            }
        }
        return null;
    }

    private RestHighLevelClient esClient() {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName("es");
        signer.setRegionName(System.getenv("AWS_REGION"));
        HttpRequestInterceptor interceptor =
                new AWSRequestSigningApacheInterceptor("es", signer, new DefaultAWSCredentialsProviderChain());
        return new RestHighLevelClient(
                RestClient.builder(HttpHost.create(System.getenv("ES_ENDPOINT")))
                        .setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor)));
    }

    private Message getMessageFromS3(String bucket, String key) throws IOException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(System.getenv("AWS_REGION"))
                .withCredentials(new ProfileCredentialsProvider())
                .build();
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucket, key));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));

        String text = "";
        String line;
        while ((line = reader.readLine()) != null){
            text = text + "\n" + line;
        }
        System.out.println("Message from s3 is: " + text);
        return JsonbBuilder.create().fromJson(text, Message.class);
    }

    private void deleteMessageFromS3(String bucket, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(System.getenv("AWS_REGION"))
                .withCredentials(new ProfileCredentialsProvider())
                .build();
        DeleteObjectRequest req = new DeleteObjectRequest(bucket, key);
        s3Client.deleteObject(req);
    }

    private Document parseFile(Document document) throws IOException, SAXException, TikaException {
        BodyContentHandler handler = new BodyContentHandler();
        AutoDetectParser parser = new AutoDetectParser();
        Metadata metadata = new Metadata();
        InputStream stream = new ByteArrayInputStream(Base64.getDecoder().decode(document.getContentBase64()));
        parser.parse(stream, handler, metadata);
        document.setContent(handler.toString());
        return document;
    }
}
