package search.ingester.models;

import javax.json.bind.annotation.JsonbProperty;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import search.ingester.models.validators.NotBlankIfAnotherFieldIsBlank;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.List;

@NotBlankIfAnotherFieldIsBlank(fieldName="content", dependFieldName = "fileBase64")
public class Document {
    
    @NotBlank
    //@Pattern(regexp = "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", message = "ID must be a UUID")
    @JsonbProperty("id")
    private String id;

    @NotBlank
    @Pattern(regexp = "datahub|website|mhc|sac", flags = Pattern.Flag.CASE_INSENSITIVE, message = "Site must be datahub|website|mhc|sac")
    @JsonbProperty("site")
    private String site;

    @NotBlank
    @JsonbProperty("title")
    private String title;

    @NotEmpty
    @JsonbProperty("keywords")
    private List<Keyword> keywords;

    @JsonbProperty("content")
    private String content;

    @JsonbProperty("content_truncated")
    private String contentTruncated;

    @JsonbProperty("file_base64")
    private String fileBase64;

    @JsonbProperty("file_bytes")
    private Integer fileBytes;

    @JsonbProperty("file_extension")
    private String fileExtension;

    //@NotBlank
    @JsonbProperty("url")
    private String url;

    // todo: what was this?
    // //@NotBlank
    // @JsonbProperty(value = "data_type", nillable = true)
    // private String dataType;

    @NotNull
    @Pattern(regexp = "^([\\+-]?\\d{4}(?!\\d{2}\\b))((-?)((0[1-9]|1[0-2])(\\3([12]\\d|0[1-9]|3[01]))?|W([0-4]\\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\\d|[12]\\d{2}|3([0-5]\\d|6[1-6])))([T\\s]((([01]\\d|2[0-3])((:?)[0-5]\\d)?|24\\:?00)([\\.,]\\d+(?!:))?)?(\\17[0-5]\\d([\\.,]\\d+)?)?([zZ]|([\\+-])([01]\\d|2[0-3]):?([0-5]\\d)?)?)?)?$", message = "Must be an ISO 8601 date")
    @JsonbProperty("published_date")
    private String publishedDate;

    @Pattern(regexp = "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", message = "ID must be a UUID")
    @JsonbProperty("parent_id")
    private String parentId;

    @JsonbProperty("parent_title")
    private String parentTitle;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getSite() { return site; }
    public void setSite(String site) { this.site = site; }

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public List<Keyword> getKeywords() { return keywords;}
    public void setKeywords(List<Keyword> keywords) { this.keywords = keywords; }

    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }

    public String getContentTruncated() { return contentTruncated; }
    public void setContentTruncated(String contentTruncated) { this.contentTruncated = contentTruncated; }

    public String getFileBase64() { return fileBase64; }
    public void setFileBase64(String fileBase64) { this.fileBase64 = fileBase64; }

    public Integer getFileBytes() { return fileBytes; }
    public void setFileBytes(Integer fileBytes) { this.fileBytes = fileBytes; }

    public String getFileExtension() { return fileExtension; }
    public void setFileExtension(String fileExtension) { this.fileExtension = fileExtension; }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getPublishedDate() { return publishedDate; }
    public void setPublishedDate(String publishedDate) { this.publishedDate = publishedDate; }

    public String getParentId() { return parentId; }
    public void setParentId(String parentId) { this.parentId = parentId; }

    public String getParentTitle() { return parentTitle; }
    public void setParentTitle(String parentTitle) { this.parentTitle = parentTitle; }

    public ImmutablePair<Boolean, String> nonAnnotationValidation() {
        if (StringUtils.isBlank(fileBase64) && StringUtils.isBlank(content)) {
            return new ImmutablePair<>(false, "content and content_base64 fields are blank");
        }
        return new ImmutablePair<>(true, null);
    }
}