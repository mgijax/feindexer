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
 * CreAlleleSystemSQL
 * @author mhall
 * 
 * This class is responsible for populating the cre allele system index.  This index is 
 * fairly straight forward, with only one sub object relationship.
 * 
 */

public class CreAlleleSystemIndexerSQL extends Indexer {

   
    public CreAlleleSystemIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * The main function, which calls doChunks and creates the index.
     * @param args
     */
    public static void main(String[] args) {

        CreAlleleSystemIndexerSQL ri = new CreAlleleSystemIndexerSQL("index.url.creAlleleSystem");
        ri.doChunks();
    }
    
    /**
     * This is the method that is responsible for populating the index for cre.  It has one sub 
     * object relationship, and an extremely large main query.
     */
    
    private void doChunks() {
                
        try {
                       
            // The main sql for cre, this is a very large, but simple sql statement.
            
            logger.info("Getting all cre allele systems");
            ResultSet rs_overall = ex.executeProto("select distinct allele_system_key, allele_id, system_key" +
                    " from recombinase_allele_system");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the results, again this is a very large query, but fairly
            // flat and straightforward. 
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.ALL_ID, rs_overall.getString("allele_id"));
                doc.addField(IndexConstants.CRE_SYSTEM_KEY, rs_overall.getString("system_key"));
                doc.addField(IndexConstants.CRE_ALL_SYSTEM_KEY, rs_overall.getString("allele_system_key"));

                
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
    
    String doBit(String bit) {
        if (bit == null) {
            return "-1";
        }
        if (bit.equals("1")) {
            return "1";
        }
        return "0";
    }
}
