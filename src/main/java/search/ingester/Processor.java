package search.ingester;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import search.ingester.models.Document;
import search.ingester.models.Message;

public class Processor {

    private ElasticService elasticService;
    private FileParser fileParser;

    public Processor(ElasticService elasticService, FileParser fileParser) {
      this.elasticService = elasticService;
      this.fileParser = fileParser;
    }

    public void process(Message m) throws IOException {

        switch (m.getVerb()) {
            case "upsert": processUpsert(m); break;
            case "delete": processDelete(m); break;
            case "spike":  processSpike(m);  break;
            default: throw new RuntimeException(
                String.format("Expected verb to be 'upsert' or 'delete' but got %s", m.getVerb()));
        }        
    }

    void processUpsert(Message m) throws IOException {
        
        Document doc = m.getDocument();

        deleteDatahubResourcesIfNecessary(m.getIndex(), doc);
        extractContentFromFileBase64IfNecessary(doc);        
        validateDocument(doc);
        DocumentTweaker.setContentTruncatedField(doc);
        elasticService.putDocument(m.getIndex(), doc);
        upsertDatahubResourcesIfAny(m.getIndex(), doc);
    }
    
    void deleteDatahubResourcesIfNecessary(String index, Document doc) throws IOException {

        // if this is a datahub doc, delete any existing resources
        if (doc.getSite() == "datahub") {
            elasticService.deleteByParentId(index, doc.getParentId());
        }
    }

    void extractContentFromFileBase64IfNecessary(Document doc) {
        
        // if this doc represents a "file" (e.g. a PDF) then it will have a file_base64 field
        // which we need to extract into the content field etc.
        if (doc.getFileBase64() != null) {
            try {
                // note this function mutates its argument (and returns it for good measure!)
                doc = fileParser.parseFile(doc);
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        }
    }

    void validateDocument(Document doc) {

        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Document>> violations = validator.validate(doc);
    
        if (violations.size() > 0) {
            throw new RuntimeException(
                    violations.stream()
                            .map(violation -> violation.getPropertyPath().toString() + ": " + violation.getMessage())
                            .collect(Collectors.joining("\n")));
        }
    }

    void upsertDatahubResourcesIfAny(String index, Document doc) {

        // if this is a datahub doc, upsert any existing resources
        if (doc.getSite() == "datahub") {
            System.out.println("::upserting " + doc.getResources().size() + " resources:: ");
            for (Document resource : doc.getResources()) {
                // todo ...construct a stable ID from the docId and the title
                resource.setId(UUID.randomUUID().toString());
                resource.setUrl(UUID.randomUUID().toString());
                
                // set the parent information
                resource.setParentId(doc.getId());
                resource.setParentTitle(doc.getTitle());
            }
        }
    }

    void processDelete(Message m) throws IOException {

        String index = m.getIndex();
        Document doc = m.getDocument();

        // delete any child resources, and the document itself
        deleteDatahubResourcesIfNecessary(index, m.getDocument());
        elasticService.deleteDocument(index, doc.getId());
    }

    void processSpike(Message m) throws IOException {
        // temporary method for testing on AWS Lambda...
    }
}