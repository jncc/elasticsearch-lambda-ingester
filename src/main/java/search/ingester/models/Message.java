package search.ingester.models;

import javax.json.bind.annotation.JsonbProperty;

public class Message {
    @JsonbProperty("index")
    private String index;
    @JsonbProperty("verb")
    private String verb;
    @JsonbProperty("document")
    private Document document;

    @JsonbProperty("S3BucketName")
    private String s3BucketName;
    @JsonbProperty("S3Key")
    private String s3Key;

    public String getIndex() { return index; }
    public void setIndex(String index) { this.index = index; }
    public String getVerb() { return verb; }
    public void setVerb(String verb) { this.verb = verb; }
    public Document getDocument() { return document; }
    public void setDocument(Document document) { this.document = document; }
    public String getS3BucketName() { return s3BucketName; }
    public void setS3BucketName(String s3BucketName) {
        this.s3BucketName = s3BucketName;
    }
    public String getS3Key() {
        return s3Key;
    }
    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }
}

