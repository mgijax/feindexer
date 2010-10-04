package org.jax.mgi.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * JournalAutoCompleteIndexerSQL
 * @author mhall
 * This index is has the primary responsibility of populating the journal autocomplete index.
 * Since this is such a small index we don't do any actual chunking here.
 */

public class JournalsAutoCompleteIndexerSQL extends Indexer {

   
    public JournalsAutoCompleteIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * The main method, which calls doChunks to create the index.
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        
        JournalsAutoCompleteIndexerSQL ri = new JournalsAutoCompleteIndexerSQL("index.url.journalAC");
        ri.doChunks();

    }
    
    private void doChunks() {
        
        
                
        try {
            
            logger.info("Getting all distinct journals");
            ResultSet rs_overall = ex.executeProto("select distinct(journal) from reference where journal is not null");
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            logger.info("Parsing them");
            while (rs_overall.next()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.REF_JOURNAL, rs_overall.getString("journal"));
                String [] temp = rs_overall.getString("journal").split("\\W");
                
                // Add all possible permutations of this author into the index.
                if (temp.length > 1) {
                    for (int i = 1; i< temp.length; i++) {
                            String tempString = "";
                            for (int j = i; j < temp.length; j++) {
                                if (tempString.equals("")) {
                                    tempString += temp[j];
                                }
                                else {
                                    tempString += " " + temp[j];
                                }
                            }
                        doc.addField(IndexConstants.REF_JOURNAL, tempString);
                    }
                }
                
                doc.addField(IndexConstants.REF_JOURNAL_SORT, rs_overall.getString("journal"));
                
                docs.add(doc);                
            }
            
            logger.info("Adding the documents to the index.");
            
            server.add(docs);
            server.commit();
            
        } catch (Exception e) {
            logger.error("In the exception part.");
            e.printStackTrace();}
    }
}