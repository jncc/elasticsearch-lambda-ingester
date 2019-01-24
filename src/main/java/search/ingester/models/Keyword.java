package search.ingester.models;

import javax.json.bind.annotation.JsonbProperty;
import org.hibernate.validator.constraints.NotBlank;

public class Keyword {
    @NotBlank
    @JsonbProperty("vocab")
    public String vocab;
    @NotBlank
    @JsonbProperty("value")
    public String value;

    public String getVocab() {
        return vocab;
    }
    public void setVocab(String vocab) {
        this.vocab = vocab;
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
