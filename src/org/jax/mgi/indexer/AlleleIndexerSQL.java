package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
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
        // Parse the results, again this is a very large query, but fairly
        // flat and straightforward. 
        ResultSet rs = ex.executeProto("select max(allele_key) as max_allele_key from allele");
    	rs.next();
    	
    	Integer start = 0;
        Integer end = rs.getInt("max_allele_key");
    	int chunkSize = 50000;
        
        int modValue = end.intValue() / chunkSize;
        // Perform the chunking

        logger.info("Getting all disease and MP annotations");
        
        for (int i = 0; i <= modValue; i++) 
        {
            start = i * chunkSize;
            end = start + chunkSize;
            
            processAlleles(start,end);
        }
    }
    
    /*
     * All queries in here should filter by start and end allele_key
     */
    private void processAlleles(int startKey,int endKey) throws Exception
    {
    	/*
    	 *  gather lookups
    	 */
    	// phenotypes
    	Map<String,Set<String>> alleleNotesMap = getAlleleNotesMap(startKey,endKey);
    	Map<String,Set<String>> alleleMPIdMap = getAlleleMPIdsMap(startKey,endKey);
    	
    	// locations
    	Map<Integer,AlleleLocation> locationMap = getAlleleLocations(startKey,endKey);
    	
    	// accession IDs
        Map<String,Set<String>> allIdMap = this.getAlleleIdsMap(startKey,endKey);
        
    	// The main sql for allele
        logger.info("Getting all alleles");
        ResultSet rs = ex.executeProto("select m.marker_key,m.primary_id marker_id, " +
        		"a.allele_key, " +
        		"a.symbol, a.name, " +
        		"a.allele_type,a.allele_subtype, " +
        		"a.collection, is_wild_type " +
        		"from allele a join " +
        		"marker_to_allele mta on a.allele_key=mta.allele_key join " +
        		"marker m on m.marker_key=mta.marker_key "+
        		"where mta.allele_key > "+startKey+" and mta.allele_key <= "+endKey+" ");
        
        logger.info("Parsing them");
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        while (rs.next()) {
            SolrInputDocument doc = new SolrInputDocument();
            Integer allKey=rs.getInt("allele_key");
            String allKeyString=allKey.toString();
            
            /*
             * Add marker fields
             */
            doc.addField(IndexConstants.MRK_KEY,rs.getString("marker_key"));
            doc.addField(IndexConstants.MRK_ID,rs.getString("marker_id"));

            /*
             * Add allele fields
             */
            doc.addField(IndexConstants.ALL_KEY, allKeyString);
            doc.addField(IndexConstants.ALL_SYMBOL, rs.getString("symbol"));
            doc.addField(IndexConstants.ALL_NAME, rs.getString("name"));
            doc.addField(IndexConstants.ALL_TYPE, rs.getString("allele_type"));
            doc.addField(IndexConstants.ALL_IS_WILD_TYPE, rs.getString("is_wild_type"));
            doc.addField(IndexConstants.ALL_COLLECTION, rs.getString("collection"));
            String subTypes = rs.getString("allele_subtype");
            if(notEmpty(subTypes))
            {
            	for(String subType : subTypes.split(", "))
            	{
            		doc.addField(IndexConstants.ALL_SUBTYPE,subType);
            	}
            }
            
            /*
             * Phenotype data
             */
            // add the phenotype notes
            this.addAllFromLookup(doc,IndexConstants.ALL_PHENO_TEXT,allKeyString,alleleNotesMap);
            this.addAllFromLookup(doc,IndexConstants.ALL_PHENO_ID,allKeyString,alleleMPIdMap);
            
            /*
             * Allele Location data
             */
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

            
            // add allele accession IDs
            this.addAllFromLookup(doc,IndexConstants.ALL_ID,allKeyString,allIdMap);
         
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
    
    
    /*
     * Lookup access functions
     */
    public Map<String,Set<String>> getAlleleNotesMap(int start,int end) throws Exception
    {
    	// get all phenotype notes for alleles
    	String phenoNotesSQL="select atg.allele_key, mpan.note\r\n" + 
    			"from allele_to_genotype atg join \r\n" + 
    			"	mp_system ms on ms.genotype_key=atg.genotype_key join\r\n" + 
    			"	mp_term mpt on mpt.mp_system_key=ms.mp_system_key join\r\n" + 
    			"	mp_reference mpr on mpr.mp_term_key=mpt.mp_term_key join\r\n" + 
    			"	mp_annotation_note  mpan on mpan.mp_reference_key=mpr.mp_reference_key "+
    			"and atg.allele_key > "+start+" and atg.allele_key <= "+end+" ";
    	return this.populateLookup(phenoNotesSQL,"allele_key","note","allele_key->annotation notes");
    }
    public Map<String,Set<String>> getAlleleMPIdsMap(int start,int end) throws Exception
    {
    	// get the direct MP ID associations
	    String mpIdsSQL="select atg.allele_key, mpt.term_id\r\n" + 
	    		"from allele_to_genotype atg join \r\n" + 
	    		"	mp_system ms on ms.genotype_key=atg.genotype_key join\r\n" + 
	    		"	mp_term mpt on mpt.mp_system_key=ms.mp_system_key"+
				"and atg.allele_key > "+start+" and atg.allele_key <= "+end+" ";
		Map<String,Set<String>> allelePhenoIdMap = this.populateLookup(mpIdsSQL,"allele_key","note","allele_key->MP Ids");
		
		// add the parent IDs
		String mpAncIdsSQL="select atg.allele_key, tas.ancestor_primary_id\r\n" + 
				"from allele_to_genotype atg join \r\n" + 
				"	mp_system ms on ms.genotype_key=atg.genotype_key join\r\n" + 
				"	mp_term mpt on mpt.mp_system_key=ms.mp_system_key join\r\n" + 
				"	term t on t.primary_id=mpt.term_id join\r\n" + 
				"	term_ancestor_simple tas on tas.term_key=t.term_key "+
				"and atg.allele_key > "+start+" and atg.allele_key <= "+end+" ";
		allelePhenoIdMap = this.populateLookup(mpAncIdsSQL,"allele_key","ancestor_primary_id","allele_key->ancestor IDs",allelePhenoIdMap);
		
		// add the alt IDs for all parents
		String altIdSQL="WITH\r\n" + 
				"allele_terms AS (select atg.allele_key,t.term_key\r\n" + 
				"from allele_to_genotype atg join \r\n" + 
				"	mp_system ms on ms.genotype_key=atg.genotype_key join\r\n" + 
				"	mp_term mpt on mpt.mp_system_key=ms.mp_system_key join\r\n" + 
				"	term t on t.primary_id=mpt.term_id\r\n" + 
				"and atg.allele_key > "+start+" and atg.allele_key <= "+end+" "+
				")\r\n" + 
				"select at.allele_key, ti.acc_id\r\n" + 
				"from allele_terms at join\r\n" + 
				"	term_ancestor_simple tas on tas.term_key=at.term_key join\r\n" + 
				"	term anc_t on anc_t.primary_id=tas.ancestor_primary_id join\r\n" + 
				"	term_id ti on ti.term_key=anc_t.term_key\r\n" + 
				"UNION\r\n" + 
				"select at.allele_key,ti.acc_id\r\n" + 
				"from allele_terms at join\r\n" + 
				"	term_id ti on ti.term_key=at.term_key";
		allelePhenoIdMap = this.populateLookup(altIdSQL,"allele_key","acc_id","allele_key->alt IDs",allelePhenoIdMap);
		
		return allelePhenoIdMap;
    }
    public Map<String,Set<String>> getAlleleIdsMap(int start,int end) throws Exception
    {
        String allToIDSQL = "select allele_key, acc_id from allele_id "+
        		 "where allele_key > "+start+" and allele_key <= "+end+" ";
        Map<String,Set<String>> allIdMap = this.populateLookup(allToIDSQL, "allele_key", "acc_id","allele_keys -> allele accession IDs");
        return allIdMap;
    }
    public Map<Integer,AlleleLocation> getAlleleLocations(int start,int end) throws Exception
    {
    	logger.info("building map of allele_keys -> marker locations");
    	String locationQuery="select allele_key,ml.* " +
    			"from marker_to_allele mta join " +
    			"marker_location ml on ml.marker_key=mta.marker_key " +
    			"where mta.allele_key > "+start+" and mta.allele_key <= "+end+" ";
    	Map<Integer,AlleleLocation> locationMap = new HashMap<Integer,AlleleLocation>();
    	ResultSet rs = ex.executeProto(locationQuery);
    	while(rs.next())
    	{
    		Integer allKey = rs.getInt("allele_key");
    		String chromosome = rs.getString("chromosome");
    		Integer startCoord = rs.getInt("start_coordinate");
    		Integer endCoord = rs.getInt("end_coordinate");
    		Double cmOffset = rs.getDouble("cm_offset");
    		String cytogeneticOffset = rs.getString("cytogenetic_offset");
    		
    		if(!locationMap.containsKey(allKey)) locationMap.put(allKey,new AlleleLocation());
    		AlleleLocation al = locationMap.get(allKey);
    		
    		// set any non-null fields from this location row
    		if(chromosome!=null) al.chromosome=chromosome;
    		if(startCoord>0) al.startCoordinate=startCoord;
    		if(endCoord>0) al.endCoordinate=endCoord;
    		if(cmOffset>0.0) al.cmOffset=cmOffset;
    		if(cytogeneticOffset!=null) al.cytogeneticOffset=cytogeneticOffset;
    	}
    	logger.info("done building map of allele_keys -> marker locations");
    	return locationMap;
    }
    
    // helper class for storing allele location info
    public class AlleleLocation
    {
    	String chromosome=null;
    	Integer startCoordinate=0;
    	Integer endCoordinate=0;
    	Double cmOffset=0.0;
    	String cytogeneticOffset=null;
    }
    private boolean notEmpty(String s)
    {
    	return s!=null && !s.equals("");
    }
}
