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

public class TemplateIndexerSQL extends Indexer {

   
    public TemplateIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Insert Index Name Constant


        TemplateIndexerSQL ri = new TemplateIndexerSQL("INSERT INDEX PROPERTY KEY HERE");
        ri.doChunks();
        
  
    }
    
    private void doChunks() {
                
        try {
            
            // TODO Insert Appropriate max Count logic here, or Delete if you will not need it.
            
            ResultSet rs_tmp = ex.executeProto("select max(referenceKey) as maxRefKey from reference");
            rs_tmp.next();
            
            System.out.println("Max Ref Number: " + rs_tmp.getString("maxRefKey") + " Timing: "+ ex.getTiming());
            String start = "0";
            String end = rs_tmp.getString("maxRefKey");
            
            // TODO Setup your sub object relationships here.  Copy and paste of each additional 
            // Subobject
            
            logger.info("Seleceting all marker references");
            String markerToRefSQL = "select referenceKey, markerKey from markerToReference where referenceKey > " + start + " and referenceKey <= "+ end;
            System.out.println(markerToRefSQL);
            HashMap <String, HashSet <String>> refToMarkers = makeHash(markerToRefSQL, "referenceKey", "markerKey");

            
            // TODO Setup the main query here
            
            logger.info("Getting all references");
            ResultSet rs_overall = ex.executeProto("select r.referenceKey, r.jnumID, r.authors, r.title, r.journal, r.vol, r.issue, ra.abstract from reference as r inner join referenceAbstract ra on r.referenceKey = ra.referenceKey");
            
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
