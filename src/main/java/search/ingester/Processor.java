package search.ingester;

import java.io.IOException;
import java.util.List;
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

        System.out.println(":: Upserting doc " + doc.getId() + " for site " + doc.getSite() + " in index " + m.getIndex() + " ::");

        deleteDatahubResourcesIfNecessary(m.getIndex(), doc);
        extractContentFromFileBase64IfNecessary(doc);        
        validateDocument(doc);
        DocumentTweaker.setContentTruncatedField(doc);
        elasticService.putDocument(m.getIndex(), doc);
        upsertDatahubResourcesIfAny(m.getIndex(), doc, m.getResources());
    }
    
    void deleteDatahubResourcesIfNecessary(String index, Document doc) throws IOException {

        // if this is a datahub doc, delete any existing resources
        if (doc.getSite().equals("datahub")) {
            elasticService.deleteByParentId(index, doc.getId());
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

    void upsertDatahubResourcesIfAny(String index, Document doc, List<Document> resources) throws IOException {

        // upsert any additional resources if this is a datahub doc
        if (resources != null && doc.getSite().equals("datahub")) {
            
            System.out.println(":: Upserting " + resources.size() + " resources :: ");
            for (Document r : resources) {
                    
                // todo ...construct a stable ID from the docId and the title
                r.setId(UUID.randomUUID().toString());
                r.setUrl(UUID.randomUUID().toString());
                
                // ensure the site it set

                r.setSite(doc.getSite());
                // set the parent information
                r.setParentId(doc.getId());
                r.setParentTitle(doc.getTitle());

                elasticService.putDocument(index, r);
            }
        }
    }

    void processDelete(Message m) throws IOException {

        String index = m.getIndex();
        Document doc = m.getDocument();

        System.out.println(":: Deleting doc " + doc.getId() + " for site " + doc.getSite() + " in index " + m.getIndex() + " ::");

        // delete any child resources, and the document itself
        deleteDatahubResourcesIfNecessary(index, m.getDocument());
        elasticService.deleteDocument(index, doc.getId());
    }

    void processSpike(Message m) throws IOException {
        // temporary method for testing on AWS Lambda...
    }
}