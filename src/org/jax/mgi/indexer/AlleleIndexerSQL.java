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
    			"marker_location ml on ml.marker_key=mta.marker_key";
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
    		
    		if(!locationMap.containsKey(allKey)) locationMap.put(allKey,new AlleleLocation());
    		AlleleLocation al = locationMap.get(allKey);
    		
    		// set any non-null fields from this location row
    		if(chromosome!=null) al.chromosome=chromosome;
    		if(start>0) al.startCoordinate=start;
    		if(end>0) al.endCoordinate=end;
    		if(cmOffset>0.0) al.cmOffset=cmOffset;
    		if(cytogeneticOffset!=null) al.cytogeneticOffset=cytogeneticOffset;
    	}
    	logger.info("done building map of allele_keys -> marker locations");
    	
        // The system sub object relationship.  This has no 
        // chunking as it shouldn't be needed.
        
        logger.info("Seleceting all allele -> ID relationship");
        String allToIDSQL = "select allele_key, acc_id from allele_id";
        System.out.println(allToIDSQL);
        HashMap <String, HashSet <String>> allToIDs = makeHash(allToIDSQL, "allele_key", "acc_id");


        // The main sql for cre, this is a very large, but simple sql statement.
        
        logger.info("Getting all alleles");
        rs = ex.executeProto("select allele_key, symbol, name, allele_type, collection, is_wild_type from allele");
        
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        
        // Parse the results, again this is a very large query, but fairly
        // flat and straightforward. 
        
        logger.info("Parsing them");
        while (rs.next()) {
            SolrInputDocument doc = new SolrInputDocument();
            Integer allKey=rs.getInt("allele_key");
            doc.addField(IndexConstants.ALL_KEY, allKey.toString());
            doc.addField(IndexConstants.ALL_SYMBOL, rs.getString("symbol"));
            doc.addField(IndexConstants.ALL_NAME, rs.getString("name"));
            doc.addField(IndexConstants.ALL_TYPE, rs.getString("allele_type"));
            doc.addField(IndexConstants.ALL_IS_WILD_TYPE, rs.getString("is_wild_type"));
            doc.addField(IndexConstants.ALL_COLLECTION, rs.getString("collection"));
            
            if(locationMap.containsKey(allKey))
            {
            	AlleleLocation al = locationMap.get(allKey);
            	// add any location data for this allele
            	if(al.chromosome!=null) doc.addField(IndexConstants.CHROMOSOME, al.chromosome);
            	if(al.startCoordinate>0) doc.addField(IndexConstants.START_COORD, al.startCoordinate);
            	if(al.endCoordinate>0) doc.addField(IndexConstants.END_COORD, al.endCoordinate);
            	if(al.cmOffset>0.0) doc.addField(IndexConstants.CM_OFFSET, al.cmOffset);
            	if(al.cytogeneticOffset!=null) doc.addField(IndexConstants.CYTOGENETIC_OFFSET, al.cytogeneticOffset);
            }

            
            // Bring in the multi-valued field allele system. 
            
            if (allToIDs.containsKey(allKey.toString())) {
                for (String id: allToIDs.get(allKey.toString())) {
                    doc.addField(IndexConstants.ALL_ID, id);
                }
            }
         
            docs.add(doc);   
            if (docs.size() > 1000) {
                writeDocs(docs);
                docs = new ArrayList<SolrInputDocument>();
            }
        }
        logger.info("Adding final stack of the documents to Solr");
        server.add(docs);
        server.commit();       
    }
    
    private class AlleleLocation
    {
    	String chromosome=null;
    	Integer startCoordinate=0;
    	Integer endCoordinate=0;
    	Double cmOffset=0.0;
    	String cytogeneticOffset=null;
    }
    
}
