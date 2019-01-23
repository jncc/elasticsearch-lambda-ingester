package search.ingester.models;

import javax.json.bind.annotation.JsonbProperty;
import java.util.List;

public class Document {
    @JsonbProperty("id")
    private String id;
    @JsonbProperty("site")
    private String site;
    @JsonbProperty("title")
    private String title;
    @JsonbProperty("keywords")
    private List<Keyword> keywords;

    @JsonbProperty("content")
    private String content;
    @JsonbProperty("content_truncated")
    private String contentTruncated;
    @JsonbProperty("content_base64")
    private String contentBase64;

    @JsonbProperty("url")
    private String url;
    @JsonbProperty("data_type")
    private String dataType;
    @JsonbProperty("published")
    private String published;
    @JsonbProperty("parent_id")
    private String parentId;
    @JsonbProperty("parent_title")
    private String parentTitle;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Keyword> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<Keyword> keywords) {
        this.keywords = keywords;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContentTruncated() {
        return contentTruncated;
    }

    public void setContentTruncated(String contentTruncated) {
        this.contentTruncated = contentTruncated;
    }

    public String getContentBase64() {
        return contentBase64;
    }

    public void setContentBase64(String contentBase64) {
        this.contentBase64 = contentBase64;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getPublished() {
        return published;
    }

    public void setPublished(String published) {
        this.published = published;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getParentTitle() {
        return parentTitle;
    }

    public void setParentTitle(String parentTitle) {
        this.parentTitle = parentTitle;
    }
}
