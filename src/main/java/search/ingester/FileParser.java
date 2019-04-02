package search.ingester;

import java.io.*;
import java.util.Base64;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;
import search.ingester.models.Document;

public class FileParser {

    private static final int TIKA_MAX_CHARACTER_LIMIT = -1;

    /**
     * Creates a document template from an existing document template with an attached base64 encoded file in the
     * content_base64 field. Attempt to overwrite the relevant parts of the given document template and remove extra
     * whitespace characters from the content as they are not needed
     *
     * @param document A document template with a base64 encoded file attached in the content_base64 field
     * @return A Document object with the base64 files text content and title in place of the given document template
     * @throws IOException Thrown on an issue with opening the input stream containing the base64 encoded file
     * @throws SAXException Thrown as part of the Tika package parsing the given document
     * @throws TikaException Thrown as part of the Tika package parsing the given document
     */
    public Document parseFile(Document document) throws IOException, SAXException, TikaException {

        // Create auto document parser and try to extract some textual info from the base64 encoded string passed to it
        BodyContentHandler handler = new BodyContentHandler(TIKA_MAX_CHARACTER_LIMIT);
        AutoDetectParser parser = new AutoDetectParser();
        Metadata metadata = new Metadata();
        InputStream stream = new ByteArrayInputStream(Base64.getDecoder().decode(document.getFileBase64()));

        try {
            parser.parse(stream, handler, metadata);
        } catch(SAXException ex) {
            if (ex.getClass().getCanonicalName() != "org.apache.tika.sax.WriteOutContentHandler$WriteLimitReachedException") {
                throw ex;
            } else {
                System.out.println(String.format("Got more characters than current Tika limit (%d), truncating to limit", TIKA_MAX_CHARACTER_LIMIT));
            }
        }

        // Grab the extracted content from the parser and strip out all repeated whitespace characters as we don't need
        // them, if no content don't replace the existing content
        String newContent = handler.toString().replaceAll("\\s+", " ").trim();
        if (!newContent.isEmpty()) {
            document.setContent(newContent);
        }

        // If a title exists in the document metadata replace the document title with it
        if (metadata.get("title") != null) {
            document.setTitle(String.format("%s - %s", document.getTitle(), metadata.get("title")));
        }

        // Clear b64 encoded file
        document.setFileBase64(null);

        return document;
    }

    /**
     * Not currently used, designed to handle html content
     * 
     * @param document
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws TikaException
     */    
    public Document parseHTMLContentString(Document document) throws IOException, SAXException, TikaException {
        BodyContentHandler handler = new BodyContentHandler(TIKA_MAX_CHARACTER_LIMIT);
        HtmlParser parser = new HtmlParser();
        Metadata metadata = new Metadata();

        InputStream stream = new ByteArrayInputStream(document.getContent().getBytes());

        try {
            parser.parse(stream, handler, metadata, new ParseContext());
        } catch(SAXException ex) {
            if (ex.getClass().getCanonicalName() != "org.apache.tika.sax.WriteOutContentHandler$WriteLimitReachedException") {
                throw ex;
            } else {
                System.out.println(String.format("Got more characters than current Tika limit (%d), truncating to limit", TIKA_MAX_CHARACTER_LIMIT));
            }
        }

        // Grab the extracted content from the parser and strip out all repeated whitespace characters as we don't need
        // them, if no content don't replace the existing content
        String newContent = handler.toString().replaceAll("\\s+", " ").trim();
        if (!newContent.isEmpty()) {
            document.setContent(newContent);
        }

        // If a title exists in the document metadata replace the document title with it
        if (metadata.get("title") != null) {
            document.setTitle(metadata.get("title"));
        }

        return document;
    }
}
