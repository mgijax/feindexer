package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * CreIndexerSQL
 * @author mhall
 * 
 * This class is responsible for populating the cre index.  This index is 
 * fairly straight forward, with only one sub object relationship.
 * 
 */

public class AlleleIndexerSQL extends Indexer {

   
    public AlleleIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * The main function, which calls doChunks and creates the index.
     * @param args
     */
    public static void main(String[] args) {

        AlleleIndexerSQL ri = new AlleleIndexerSQL("index.url.allele");
        ri.doChunks();
    }
    
    /**
     * This is the method that is responsible for populating the index for cre.  It has one sub 
     * object relationship, and an extremely large main query.
     */
    
    private void doChunks() {
                
        try {
                       
            // The system sub object relationship.  This has no 
            // chunking as it shouldn't be needed.
            
            logger.info("Seleceting all allele -> ID relationship");
            String allToIDSQL = "select allele_key, acc_id from allele_id";
            System.out.println(allToIDSQL);
            HashMap <String, HashSet <String>> allToIDs = makeHash(allToIDSQL, "allele_key", "acc_id");


            // The main sql for cre, this is a very large, but simple sql statement.
            
            logger.info("Getting all alleles");
            ResultSet rs_overall = ex.executeProto("select allele_key, symbol, name, allele_type from allele");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the results, again this is a very large query, but fairly
            // flat and straightforward. 
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.ALL_KEY, rs_overall.getString("allele_key"));
                doc.addField(IndexConstants.ALL_SYMBOL, rs_overall.getString("symbol"));
                doc.addField(IndexConstants.ALL_NAME, rs_overall.getString("name"));
                doc.addField(IndexConstants.ALL_TYPE, rs_overall.getString("allele_type"));

                
                // Bring in the multi-valued field allele system. 
                
                if (allToIDs.containsKey(rs_overall.getString("allele_key"))) {
                    for (String id: allToIDs.get(rs_overall.getString("allele_key"))) {
                        doc.addField(IndexConstants.ALL_ID, id);
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
            logger.info("Adding final stack of the documents to Solr");
            server.add(docs);
            server.commit();
            
        } catch (Exception e) {e.printStackTrace();}
    }
    
}