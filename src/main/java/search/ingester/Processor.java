package search.ingester;

import java.io.IOException;
import java.util.Set;
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
            case "upsert": processUpsertMessage(m); break;
            case "delete": processDeleteMessage(m); break;
            case "spike":  processSpikeMessage(m);  break;
            default: throw new RuntimeException(
                String.format("Expected verb to be 'upsert' or 'delete' but got %s", m.getVerb()));
        }        
    }

    void processUpsertMessage(Message m) throws IOException {
        
        // todo:
        // 1. delete existing resources
        // 3. put asset
        // 2. put resources

        Document document = m.getDocument();

        if (document.getFileBase64() != null) {
            try {
                document = fileParser.parseFile(document);
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

        elasticService.putDocument(m.getIndex(), document);
    }

    void processDeleteMessage(Message m) throws IOException {

        String index = m.getIndex();
        String docId = m.getDocument().getId();

        // delete any child resources and the document itself
        //todo deleteResources(index, docId);
        elasticService.deleteDocument(index, docId);
    }

    void processSpikeMessage(Message m) throws IOException {

        String index = m.getIndex();
        String docId = m.getDocument().getId();

        // todo deleteResources(index, docId);
    }
}