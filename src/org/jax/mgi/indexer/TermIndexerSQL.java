package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.reporting.Timer;

/**
 * The template indexer
 * @author mhall
 * Copy this code to create a new indexer, and then just change the appropriate sections.
 * 
 * If you need chunking go and take the code from the sequence indexer.
 */

public class TermIndexerSQL extends Indexer {

   
    public TermIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Insert Index Name Constant


        TermIndexerSQL ri = new TermIndexerSQL("index.url.term");
        ri.doChunks();
        
  
    }
    
    private void doChunks() {
                
        try {
            
            // TODO Insert Appropriate max Count logic here, or Delete if you will not need it.
            
            ResultSet rs_tmp = ex.executeProto("select max(term_key) as maxTermKey from term");
            rs_tmp.next();
            
            System.out.println("Max Term Number: " + rs_tmp.getString("maxTermKey") + " Timing: "+ ex.getTiming());
            String start = "0";
            String end = rs_tmp.getString("maxTermKey");
            
            // TODO Setup your sub object relationships here.  Copy and paste of each additional 
            // Subobject
            
            logger.info("Seleceting all marker references");
            String markerToRefSQL = "select referenceKey, markerKey from markerToReference where referenceKey > " + start + " and referenceKey <= "+ end;
            System.out.println(markerToRefSQL);
            HashMap <String, HashSet <String>> refToMarkers = makeHash(markerToRefSQL, "referenceKey", "markerKey");

            
            // TODO Setup the main query here
            
            logger.info("Getting all references");
            ResultSet rs_overall = ex.executeProto("select term_key from term");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // TODO Parse the main query results here.
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("author", rs_overall.getString("authors"));
                doc.addField("jnumID", rs_overall.getString("jnumID"));
                doc.addField("journal", rs_overall.getString("journal"));
                doc.addField("refkey", rs_overall.getString("referenceKey"));
                doc.addField("title", rs_overall.getString("title"));
                doc.addField("abstract", rs_overall.getString("abstract"));
                doc.addField("issue", rs_overall.getString("issue"));
                doc.addField("volume", rs_overall.getString("vol"));
                if (refToMarkers.containsKey(rs_overall.getString("referenceKey"))) {
                    for (String markerKey: refToMarkers.get(rs_overall.getString("referenceKey"))) {
                        doc.addField("markerkey", markerKey);
                    }
                }

                rs_overall.next();
                
                docs.add(doc);
                
                if (docs.size() > 10000) {
                    logger.info("Adding a stack of the documents to Solr");
                    server.add(docs);
                    docs = new ArrayList<SolrInputDocument>();
                    logger.info("Done adding to solr, Moving on");
                }
            }
            server.add(docs);
            server.commit();
            
        } catch (Exception e) {e.printStackTrace();}
    }
}
