package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * The template indexer
 * @author mhall
 * Copy this code to create a new indexer, and then just change the appropriate sections.
 * 
 * If you need chunking go and take the code from the sequence indexer.
 * 
 * Note: Refactored during 5.x development
 */

public class ImageIndexerSQL extends Indexer {

   
    public ImageIndexerSQL () {
        super("index.url.image");
    }
    
    public void index() throws Exception
    {
        	
            logger.info("Selecting all phenotype image -> markers");
            String imagesToMrkSQL = "select distinct marker_key, image_key from marker_to_phenotype_image";
            System.out.println(imagesToMrkSQL);
            HashMap <String, HashSet <String>> imagesToMarkers = makeHash(imagesToMrkSQL, "image_key", "marker_key");
	    logger.info ("Got markers for " + imagesToMarkers.size() + " images");

            logger.info("Selecting all phenotype image -> alleles");
            String imagesToAllSQL = "select distinct image_key, allele_key "
		    + "from allele_to_image "
		    + "union "
		    + "select distinct thumbnail_image_key, allele_key "
		    + "from allele_to_image a, image i "
		    + "where a.image_key = i.image_key";

            System.out.println(imagesToAllSQL);
            HashMap <String, HashSet <String>> imagesToAlleles = makeHash(imagesToAllSQL, "image_key", "allele_key");        	

	    logger.info ("Got alleles for " + imagesToAlleles.size() + " images");

            logger.info("Getting all images");
            ResultSet rs_overall = ex.executeProto("select i.mgi_id, i.image_key, isn.by_default, i.is_thumbnail, i.image_class " +   
				"from image i, image_sequence_num isn " + 
				"where isn.image_key = i.image_key");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // TODO Parse the main query results here.
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.IMAGE_KEY, rs_overall.getString("image_key"));
                doc.addField(IndexConstants.IMAGE_ID, rs_overall.getString("mgi_id"));
                doc.addField(IndexConstants.BY_DEFAULT, rs_overall.getString("by_default"));
                doc.addField(IndexConstants.IS_THUMB, rs_overall.getString("is_thumbnail"));
                doc.addField(IndexConstants.IMAGE_CLASS, rs_overall.getString("image_class"));
                
		// markers for a phenotype image
                if (imagesToMarkers.containsKey(rs_overall.getString("image_key"))) {
                    for (String markerKey: imagesToMarkers.get(rs_overall.getString("image_key"))) {
                        doc.addField(IndexConstants.MRK_KEY, markerKey);
                    }
                }
                
		// alleles for a phenotype image
                if (imagesToAlleles.containsKey(rs_overall.getString("image_key"))) {
                    for (String alleleKey: imagesToAlleles.get(rs_overall.getString("image_key"))) {
                        doc.addField(IndexConstants.ALL_KEY, alleleKey);
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
            
    }
}
