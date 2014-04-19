package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * indexer for mapping homology cluster IDs to their respective database keys
 * @author jsb
 */

public class HomologyIndexerSQL extends Indexer {

   
    public HomologyIndexerSQL () {
        super("index.url.homology");
    }
    
    public void index() {
                
        try {
            // Setup the main query here
            
            logger.info("Getting all homology cluster IDs and keys");
            ResultSet rs_overall = ex.executeProto(
		"select cluster_key, primary_id "
		+ "from homology_cluster "
		+ "where primary_id is not null");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs =
		new ArrayList<SolrInputDocument>();
            
            // Parse the main query results here.
            
            logger.info("Parsing homology cluster ID/key records");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.HOMOLOGY_KEY,
			rs_overall.getString("cluster_key"));
                doc.addField(IndexConstants.HOMOLOGY_ID,
			rs_overall.getString("primary_id"));

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
