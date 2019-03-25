package search.ingester;

import search.ingester.models.Document;

public class DocumentTweaker {
    
    static void setContentTruncatedField(Document doc) {
        
        // we populate the content_truncated field because that's what we actually show
        // in the search results (when not using highlights).

        String c = doc.getContent();
    
        c = c.replace("\n", ""); // remove newlines
        c = c.trim();            // trim leading and trailing whitespace
    
        String truncated = c.substring(0, Math.min(c.length(), 200));
    
        if (c.length() > truncated.length()) {
            c = truncated + "...";
        }
    
        doc.setContentTruncated(c);
    }
}
