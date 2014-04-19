package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.CreFields;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;

/**
 * CreIndexerSQL
 * @author mhall
 * 
 * This class is responsible for populating the cre index.  This index is 
 * fairly straight forward, with only one sub object relationship.
 * 
 * Note: Refactored during 5.x development
 */

public class CreIndexerSQL extends Indexer {

   
    public CreIndexerSQL () {
        super("index.url.cre");
    }
    
    /**
     * This is the method that is responsible for populating the index for cre.  It has one sub 
     * object relationship, and an extremely large main query.
     */
    
    public void index() throws Exception
    {
                       
            // The system sub object relationship.  This has no 
            // chunking as it shouldn't be needed.
            
            logger.info("Selecting all allele systems relationships");
            String allToSystemSQL = "select allele_key, system from recombinase_allele_system where system != '' order by allele_key ";
            System.out.println(allToSystemSQL);
            HashMap <String, HashSet <String>> allToSystems = makeHash(allToSystemSQL, "allele_key", "system");

            // build maps to handle structure queries
            Map<String,List<CreStructure>> structureAlleleMap = new HashMap<String,List<CreStructure>>();
	        logger.info("building map of structures to allele key");
	        String structureAlleleQuery = "select distinct ras.allele_key, " +
	        		"rar.structure, " +
	        		"struct.term_key, "+
	        		"struct.primary_id, " +
	        		"ras.system "+
	        		"from recombinase_assay_result rar,  "+
	        		"recombinase_allele_system ras, "+
	        		"term struct "+
	        		"where rar.allele_system_key=ras.allele_system_key  "+
	        		"and struct.vocab_name='EMAPA' "+
	        		"and struct.term=rar.structure ";
	        ResultSet rs = ex.executeProto(structureAlleleQuery);
	        while (rs.next())
	        {
	        	String alleleKey = rs.getString("allele_key");
	        	String sId = rs.getString("primary_id");
	        	String structure = rs.getString("structure");
	        	String sKey = rs.getString("term_key");
	        	String system = rs.getString("system");

	        	CreStructure struct = new CreStructure(sKey,structure,sId,system);
	        	
	        	if(!structureAlleleMap.containsKey(alleleKey))
	        	{
	        		structureAlleleMap.put(alleleKey, new ArrayList<CreStructure>());
	        	}
	        	structureAlleleMap.get(alleleKey).add(struct);
	        }
	        logger.info("done gathering structure synonyms");
            
	        Map<String,List<String>> structureAncestorIdMap = new HashMap<String,List<String>>();
            logger.info("building map of structure ancestors to allele key");
	        String structureAncestorQuery = SharedQueries.GXD_EMAP_ANCESTOR_QUERY;
	        rs = ex.executeProto(structureAncestorQuery);

	        while (rs.next())
	        {
	        	String skey = rs.getString("structure_term_key");
	        	String ancestorId = rs.getString("ancestor_id");
	        	String structureId = rs.getString("structure_id");
	        	if(!structureAncestorIdMap.containsKey(skey))
	        	{
	        		structureAncestorIdMap.put(skey, new ArrayList<String>());
	        		// Include original term
	        		structureAncestorIdMap.get(skey).add(structureId);
	        	}
	        	structureAncestorIdMap.get(skey).add(ancestorId);
	        }
	        logger.info("done gathering structure ancestors");
	        
	        Map<String,List<String>> structureSynonymMap = new HashMap<String,List<String>>();
	        logger.info("building map of structure synonyms");
	        String structureSynonymQuery = SharedQueries.GXD_EMAP_SYNONYMS_QUERY;
	        rs = ex.executeProto(structureSynonymQuery);
	        while (rs.next())
	        {
	        	String sId = rs.getString("structure_id");
	        	String synonym = rs.getString("synonym");
	        	String structure = rs.getString("structure");
	        	if(!structureSynonymMap.containsKey(sId))
	        	{
	        		structureSynonymMap.put(sId, new ArrayList<String>());
	        		// Include original term
	        		structureSynonymMap.get(sId).add(structure);
	        	}
	        	structureSynonymMap.get(sId).add(synonym);
	        }
	        logger.info("done gathering structure synonyms");
            // The main sql for cre, this is a very large, but simple sql statement.
            
            logger.info("Getting all cre alleles");
            ResultSet rs_overall = ex.executeProto("select ars.allele_key, " +
                "ars.detected_count, ars.not_detected_count, " + 
                "asn.by_symbol, asn.by_allele_type, asn.by_driver, " +
                "ac.reference_count, a.driver, a.inducible_note, " +
                "aic.strain_count " +  
                "from allele_recombinase_systems as ars " +
                "left outer join allele_sequence_num as asn " +
                "on ars.allele_key = asn.allele_key  " +
                "left outer join allele_counts as ac " +
                "on ars.allele_key = ac.allele_key " +
                "left outer join allele_imsr_counts as aic " + 
                "on ars.allele_key = aic.allele_key  " +
                "left outer join allele as a on ars.allele_key = a.allele_key " +
                "order by ars.allele_key");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the results, again this is a very large query, but fairly
            // flat and straightforward. 
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
            	String alleleKey = rs_overall.getString("allele_key");
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.ALL_KEY, alleleKey);
                doc.addField(IndexConstants.ALL_DRIVER_SORT, rs_overall.getString("by_driver"));
                doc.addField(IndexConstants.ALL_DRIVER, rs_overall.getString("driver"));
                doc.addField(IndexConstants.ALL_INDUCIBLE, rs_overall.getString("inducible_note"));
                doc.addField(IndexConstants.ALL_IMSR_COUNT, rs_overall.getString("strain_count"));
                doc.addField(IndexConstants.ALL_REFERENCE_COUNT_SORT, rs_overall.getString("reference_count"));
                doc.addField(IndexConstants.ALL_TYPE_SORT, rs_overall.getString("by_allele_type"));
                doc.addField(IndexConstants.ALL_SYMBOL_SORT, rs_overall.getString("by_symbol"));
                doc.addField(IndexConstants.CRE_NOT_DETECTED_COUNT, rs_overall.getString("not_detected_count"));
                doc.addField(IndexConstants.CRE_DETECTED_COUNT, rs_overall.getString("detected_count"));
                doc.addField(IndexConstants.CRE_DETECTED_TOTAL_COUNT, rs_overall.getInt("not_detected_count") + rs_overall.getInt("detected_count"));
                
                // Bring in the multi-valued field allele system. 
                
                if (allToSystems.containsKey(alleleKey)) {
                    for (String system: allToSystems.get(rs_overall.getString("allele_key"))) {
                        doc.addField(IndexConstants.CRE_ALL_SYSTEM, system);
                    }
                }
                
                // Bring in the allele structures
                // We only technically need STRUCTURE_ANCESTORS at the moment, but the others will be handy for links in the future
                
                if (structureAlleleMap.containsKey(alleleKey))
                {
                	for(CreStructure struct : structureAlleleMap.get(alleleKey))
                	{
                		// Add various fields for structures
                		String strcutureField = mapCreField(struct.system);
                		this.addFieldNoDup(doc,strcutureField,struct.structureName);
                		this.addFieldNoDup(doc,GxdResultFields.STRUCTURE_ID, struct.structureID);
                		if(structureAncestorIdMap.containsKey(struct.structureKey))
		                {
		                	// get ancestors
		                	List<String> structure_ancestor_ids = structureAncestorIdMap.get(struct.structureKey);
		                	for (String structure_ancestor_id : structure_ancestor_ids)
		                	{
		                		// get synonyms for each ancestor/term
		                		if(structureSynonymMap.containsKey(structure_ancestor_id))
		                		{
		                			//also add structure MGI ID
		                			this.addFieldNoDup(doc,GxdResultFields.STRUCTURE_ID, structure_ancestor_id);
			                		List<String> structureSynonyms = structureSynonymMap.get(structure_ancestor_id);
			                		for (String structureSynonym : structureSynonyms)
			                		{
			                			this.addFieldNoDup(doc,strcutureField,structureSynonym);
			                		}
		                		}
		                	}
                		}
                	}
                	this.resetDupTracking();
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
    
    private String mapCreField(String system) {
		if(CreFields.SYSTEM_FIELDS.containsKey(system))
		{
			return CreFields.SYSTEM_FIELDS.get(system);
		}
		return GxdResultFields.STRUCTURE_ANCESTORS;
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
    private class CreStructure
    {
    	public String structureKey;
    	public String structureName;
    	public String structureID;
    	public String system;
    	public CreStructure(String structureKey,String structureName,String structureID,String system)
    	{
    		this.structureKey=structureKey;
    		this.structureName=structureName;
    		this.structureID=structureID;
    		this.system=system;
    	}
    }
}
