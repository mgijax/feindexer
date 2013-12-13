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
 * Note: Refactored during 5.x development
 */

public class AlleleIndexerSQL extends Indexer {

   
    public AlleleIndexerSQL () {
        super("index.url.allele");
    }

    /**
     * This is the method that is responsible for populating the index for cre.  It has one sub 
     * object relationship, and an extremely large main query.
     */
    
    public void index() throws Exception
    {
    	
    	logger.info("building map of allele_keys -> marker locations");
    	String locationQuery="select allele_key,ml.* " +
    			"from marker_to_allele mta join " +
    			"marker_location ml on ml.marker_key=mta.marker_key limit 100";
    	HashMap<Integer,AlleleLocation> locationMap = new HashMap<Integer,AlleleLocation>();
    	ResultSet rs = ex.executeProto(locationQuery);
    	while(rs.next())
    	{
    		Integer allKey = rs.getInt("allele_key");
    		String chromosome = rs.getString("chromosome");
    		Integer start = rs.getInt("start_coordinate");
    		Integer end = rs.getInt("end_coordinate");
    		Double cmOffset = rs.getDouble("cm_offset");
    		String cytogeneticOffset = rs.getString("cytogenetic_offset");
    		logger.debug("chr="+chromosome+",start="+start+",end="+end+",cm="+cmOffset+",cyto="+cytogeneticOffset);
    	}
    	if(true)return;
    	logger.info("done building map of allele_keys -> marker locations");

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
            
    }
    
    private class AlleleLocation
    {
    	String chromosome=null;
    	Integer startCoordinate=null;
    	Integer endCoordinate=null;
    	Double cmOffset=null;
    	String cytogeneticOffset=null;
    }
    
}
