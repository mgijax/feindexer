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
 * indexer for the marker/tissue relationship (data from the 
 * marker_tissue_expression_counts) table
 * @author jsb
 * 
 * Note: refactored during 5.x development
 */

public class MarkerTissueIndexerSQL extends Indexer {

   
    public MarkerTissueIndexerSQL () {
        super("index.url.markerTissue");
    }
    
    public void index() {
                
        try {
            // TODO Setup the main query here
            
            logger.info("Getting all marker/tissue records");
            ResultSet rs_overall = ex.executeProto(
		"select marker_key, unique_key, sequence_num "
		+ "from marker_tissue_expression_counts");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs =
		new ArrayList<SolrInputDocument>();
            
            // Parse the main query results here.
            
            logger.info("Parsing marker/tissue records");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.MRK_KEY,
			rs_overall.getString("marker_key"));
                doc.addField(IndexConstants.UNIQUE_KEY,
			rs_overall.getString("unique_key"));
                doc.addField(IndexConstants.SEQUENCE_NUM,
			rs_overall.getString("sequence_num"));

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
