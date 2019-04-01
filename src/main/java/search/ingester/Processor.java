package search.ingester;

import java.io.IOException;
import java.util.ArrayList;
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
        case "upsert":
            processUpsert(m);
            break;
        case "delete":
            processDelete(m);
            break;
        case "spike":
            processSpike(m);
            break;
        default:
            throw new RuntimeException(
                    String.format("Expected verb to be 'upsert' or 'delete' but got %s", m.getVerb()));
        }
    }

    private void processUpsert(Message m) throws IOException {
        Document doc = m.getDocument();

        System.out.println(
                ":: Upserting doc " + doc.getId() + " for site " + doc.getSite() + " in index " + m.getIndex() + " ::");

        deleteDatahubResourcesIfNecessary(m.getIndex(), doc);

        // Prepare main document
        prepareDocument(doc);

        // Process resources if they exist
        List<Document> resources = prepareResourceDocuments(m.getResources(), doc);

        // Upload main document and resources
        upsertDocument(m.getIndex(), doc);
        upsertDatahubResourcesIfAny(m.getIndex(), resources);
    }

    /**
     * Prepares an individual document to be indexed in an Elasticsearch instance,
     * extract text from any attached base64 encoded file, validate the document and
     * do some general cleanup before forwarding the document to Elasticsearch
     * 
     * @param doc The Document object to prepare
     * @return A finalised document ready to be pushed into an ElasticSearch index
     * @throws IOException
     */
    private void prepareDocument(Document doc) throws IOException {
        extractContentFromFileBase64IfNecessary(doc);
        DocumentTweaker.setContentTruncatedField(doc);
        validateDocument(doc);
    }    

    /**
     * Prepares resources attached to a parent document to be indexed in an ElasticSearch
     * instance, calls the prepareDocument function to do generic tasks and adds in relevant
     * info about the parent document to each document so that it can be submitted to 
     * ElasticSearch, only used as part of datahub assets
     * 
     * TODO: Potentially need to kick out files we don't want to index i.e. non-pdf, currently 
     * dealt with at supplier end of this process, could deal with it more cleanly
     * 
     * @param parent The parent document that contains these resources
     * @param docs The attached list of resources (documents) for the parent resource
     * @return A List of prepared documents ready to be pushed into an ElasticSearch index
     * @throws IOException
     */
    private List<Document> prepareResourceDocuments(List<Document> docs, Document parent) throws IOException {
        List<Document> outputs = new ArrayList<Document>();

        if (docs != null && !docs.isEmpty()) {
            System.out.println(":: Preparing " + docs.size() + " resources for indexing :: ");
            
            for (Document doc : docs) {
                // TODO ...construct a stable ID from the docId and the title
                // elasticsearch needs an ID but an ID of a "resource" is never really surfaced
                doc.setId(UUID.randomUUID().toString());

                // ensure the site it set (it might not have been in the incoming message)
                doc.setSite(parent.getSite());

                // set the parent information
                doc.setParentId(parent.getId());
                doc.setParentTitle(parent.getTitle());
                doc.setParentResourceType(parent.getResourceType());

                // grab some generic info from the parent that should just be copied to the resources
                // i.e. keywords, published date
                doc.setKeywords(parent.getKeywords());
                doc.setPublishedDate(parent.getPublishedDate());
                
                // if the content is blank (shouldn't happen, copy from parent)
                if (doc.getContent() == null || doc.getContent().isEmpty()) {
                    doc.setContent(parent.getContent());
                }

                // set the resource type to the file extension if it exists, set it to other if not
                // TODO: decide if `other` is the correct call here
                if (doc.getFileExtension() != null && !doc.getFileExtension().isEmpty()) {
                    doc.setResourceType(doc.getFileExtension());
                } else {
                    doc.setResourceType("other");
                }

                prepareDocument(doc);

                outputs.add(doc);
            }
            return outputs;
        }

        return outputs;
    }    

    /**
     * Upserts a prepared document into the current ElasticSearch index
     * 
     * @param index The index to put document into on the instance
     * @param doc The document to put into the given index
     * @throws IOException
     */
    private void upsertDocument(String index, Document doc) throws IOException {
        elasticService.putDocument(index, doc);
    }

    /**
     * Upserts a list of prepared documents into the current ElasticSearch index
     * 
     * @param index The index to put the attached documents into on the instance
     * @param resources The list of resources (documents) to be put into the given index
     * @throws IOException
     */
    private void upsertDatahubResourcesIfAny(String index, List<Document> resources) throws IOException {
        for (Document resource: resources) {
            upsertDocument(index, resource);
        }
    }    

    private void deleteDatahubResourcesIfNecessary(String index, Document doc) throws IOException {

        // if this is a datahub doc, delete any existing resources
        if (doc.getSite().equals("datahub")) {
            elasticService.deleteByParentId(index, doc.getId());
        }
    }

    private void extractContentFromFileBase64IfNecessary(Document doc) {
        // if this doc represents a "file" (e.g. a PDF) then it will have a file_base64
        // field
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

    private void validateDocument(Document doc) {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Document>> violations = validator.validate(doc);

        if (violations.size() > 0) {
            throw new RuntimeException(violations.stream()
                    .map(violation -> violation.getPropertyPath().toString() + ": " + violation.getMessage())
                    .collect(Collectors.joining("\n")));
        }
    }

    private void processDelete(Message m) throws IOException {
        String index = m.getIndex();
        Document doc = m.getDocument();

        System.out.println(
                ":: Deleting doc " + doc.getId() + " for site " + doc.getSite() + " in index " + m.getIndex() + " ::");

        // delete any child resources, and the document itself
        deleteDatahubResourcesIfNecessary(index, m.getDocument());
        elasticService.deleteDocument(index, doc.getId());
    }

    private void processSpike(Message m) throws IOException {
        // temporary method for testing on AWS Lambda...
    }
}