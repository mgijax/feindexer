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
 * The template indexer
 * @author mhall
 * Copy this code to create a new indexer, and then just change the appropriate sections.
 * 
 * If you need chunking go and take the code from the sequence indexer.
 */

public class PhenotypeImageIndexerSQL extends Indexer {

   
    public PhenotypeImageIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Insert Index Name Constant


        PhenotypeImageIndexerSQL ri = new PhenotypeImageIndexerSQL("index.url.phenotypeImage");
        ri.doChunks();
        
  
    }
    
    private void doChunks() {
                
        try {
        	
            logger.info("Seleceting all image -> markers");
            String imagesToMrkSQL = "select distinct marker_key, image_key from marker_to_phenotype_image";
            System.out.println(imagesToMrkSQL);
            HashMap <String, HashSet <String>> imagesToMarkers = makeHash(imagesToMrkSQL, "image_key", "marker_key");

            logger.info("Seleceting all image -> alleles");
            String imagesToAllSQL = "select distinct image_key, allele_key from image_alleles";
            System.out.println(imagesToAllSQL);
            HashMap <String, HashSet <String>> imagesToAlleles = makeHash(imagesToAllSQL, "image_key", "allele_key");
              
            // Get the phenotypic images
            
            logger.info("Getting phenotypic images");
            ResultSet rs_overall = ex.executeProto("select i.image_key, isn.by_default " + 
				"from image i, image_sequence_num isn " + 
				"where image_class = 'Phenotypes'  " +
				"and i.image_key = isn.image_key " +
				"and i.is_thumbnail = 1");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // TODO Parse the main query results here.
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.IMAGE_KEY, rs_overall.getString("image_key"));
                doc.addField(IndexConstants.BY_DEFAULT, rs_overall.getString("by_default"));
  
                
                if (imagesToMarkers.containsKey(rs_overall.getString("image_key"))) {
                    for (String markerKey: imagesToMarkers.get(rs_overall.getString("image_key"))) {
                        doc.addField(IndexConstants.MRK_KEY, markerKey);
                    }
                }
                
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
            
        } catch (Exception e) {e.printStackTrace();}
    }
}
