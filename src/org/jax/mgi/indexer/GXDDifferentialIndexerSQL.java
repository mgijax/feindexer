package org.jax.mgi.indexer;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;

/**
 * GXDDifferentialIndexerSQL
 * @author kstone
 * This index is used primarily for performing GXD differential searches.
 * Each document represents a marker with a marker key,
 * 	which will then be used to filter results from the gxdResult index in a two step process.
 * 
 */

public class GXDDifferentialIndexerSQL extends Indexer 
{   
    public GXDDifferentialIndexerSQL () 
    { super("index.url.gxdDifferentialMarker"); }
    
    public void index() throws Exception
    {    
        Map<String,List<String>> structureAncestorIdMap = new HashMap<String,List<String>>();
        logger.info("building map of structure ancestors");
        String structureAncestorQuery = SharedQueries.GXD_EMAP_ANCESTOR_QUERY;
        ResultSet rs = ex.executeProto(structureAncestorQuery);

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
        
        Map<String,List<Structure>> structureSynonymMap = new HashMap<String,List<Structure>>();
        logger.info("building map of structure synonyms");
	String structureSynonymQuery = "select ts.synonym, "
	    + "  t.term as structure, "
	    + "  t.primary_id as structure_id, "
	    + "  e.stage as theiler_stage "
	    + "from term t "
	    + "inner join term_emap e on (t.term_key = e.term_key) "
	    + "left outer join term_synonym ts on (t.term_key=ts.term_key) "
	    + "where t.vocab_name in ('EMAPA', 'EMAPS')";

        rs = ex.executeProto(structureSynonymQuery);

        while (rs.next())
        {
        	String sId = rs.getString("structure_id");
        	String synonym = rs.getString("synonym");
        	String structure = rs.getString("structure");
        	String ts = rs.getString("theiler_stage");
        	if(!structureSynonymMap.containsKey(sId))
        	{
        		structureSynonymMap.put(sId, new ArrayList<Structure>());
        		// Include original term
        		structureSynonymMap.get(sId).add(new Structure(structure,ts));
        	}
        	if(synonym != null && !synonym.equals("null"))
        	{
        		structureSynonymMap.get(sId).add(new Structure(synonym,ts));
        	}
        }
        logger.info("done gathering structure synonyms");
        
        ResultSet rs_tmp = ex.executeProto("select count(distinct marker_key) as cnt from expression_result_summary");
    	rs_tmp.next();
    	
    	int start = 0;
        int end = rs_tmp.getInt("cnt");
    	int chunkSize = 2000;
        
        int modValue = end / chunkSize;

        logger.info("Getting "+end+" marker keys + differential data");
        
        for (int i = 0; i <= (modValue+1); i++) 
        {
            start = i * chunkSize;
            //end = start + chunkSize;
            
            logger.info ("Processing markers keys (ignoring ambiguous results) " + start + " to " + (start+chunkSize));
            String query = "WITH expr_markers AS (select distinct  marker_key from expression_result_summary ers "+
            			" order by marker_key limit "+chunkSize+" offset "+start+") " +
            		"select ers.is_expressed, " +
            		"ers.structure_key, " +
            		"ers.structure_printname, " +
            		"ers.theiler_stage," +
            		"ers.marker_key "+
            		"from expression_result_summary ers,expr_markers em "+
            		"where em.marker_key=ers.marker_key " +
            		"and ers.is_expressed != 'Unknown/Ambiguous' " +
                    "and ers.assay_type != 'Recombinase reporter' "+
                    "and ers.assay_type != 'In situ reporter (transgenic)' " +
                    "and (ers.is_wild_type = 1 or ers.genotype_key=-1) ";
                   // "order by is_expressed desc ";
            rs = ex.executeProto(query);
            Map<Integer,List<Result>> markerResults = new HashMap<Integer,List<Result>>();
            
            logger.info("Organising them");
           // Set<String> uniqueStageResults = new HashSet<String>();
            while (rs.next()) 
            {           
            	int marker_key = rs.getInt("marker_key");
            	boolean is_expressed = rs.getString("is_expressed").equals("Yes");
            	String structure_key = rs.getString("structure_key");
            	String structure = rs.getString("structure_printname");
            	String stage = rs.getString("theiler_stage");
            	if(!markerResults.containsKey(marker_key))
            	{
            		markerResults.put(marker_key,new ArrayList<Result>());
            	}
            	String uniqueStageResult = "TS"+stage+":"+structure;
            	//TODO: find a way to roll up conflicting results for same structure and stage
//            	if(uniqueStageResults.contains(uniqueStageResult)) continue;
//            	else
//            	{
            	//	uniqueStageResults.add(uniqueStageResult);
            		markerResults.get(marker_key).add(new Result(structure_key,structure,stage,is_expressed));
            	//}
            }
            //uniqueStageResults = null;
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            logger.info("Creating Solr Documents");
            for(int markerKey : markerResults.keySet())
            {   
                SolrInputDocument doc = new SolrInputDocument();
            	// Add the single value fields
                doc.addField(GxdResultFields.KEY, ""+markerKey);
                doc.addField(GxdResultFields.MARKER_KEY, markerKey);
	           
                Map<String,ExclusiveStageTracker> exclusiveTrackers = new HashMap<String,ExclusiveStageTracker>();
                
                // iterate this marker's results to build various search fields.
            	
            	Set<String> posAncestors = new HashSet<String>();
                for(Result result : markerResults.get(markerKey))
                {
                	// init the objects related to exclusive structure searching
                	ExclusiveStageTracker et;
                	if(!exclusiveTrackers.containsKey(result.stage))
                	{
                		et = new ExclusiveStageTracker(result.stage);
                		exclusiveTrackers.put(result.stage,et);
                	}
                	else et = exclusiveTrackers.get(result.stage);
                	
                	ExclusiveResultTracker ert = new ExclusiveResultTracker();

	                // add the differential ancestor search fields for this marker (for positive results only)
	                if(result.expressed)
	                {
	                	// get term ID of ancestors
	                	if(structureAncestorIdMap.containsKey(result.structureKey))
	                	{
		                	List<String> structureAncestorIds = structureAncestorIdMap.get(result.structureKey);
		                	for (String structureAncestorId : structureAncestorIds)
		                	{
		                		// find all the terms + synonyms for each ancestorID
		                		if(structureSynonymMap.containsKey(structureAncestorId))
		                		{
			                		List<Structure> structureSynonyms = structureSynonymMap.get(structureAncestorId);
			                		for (Structure structureSynonym : structureSynonyms)
			                		{
			                			posAncestors.add(structureSynonym.toString());
			                			ert.addAncestor(structureSynonym.structure);
			                		}
		                		}
		                	}
	                	}
	                	posAncestors.add(new Structure(result.structure,result.stage).toString());
            			ert.addAncestor(result.structure);
            	
            			et.addResultTracker(result.structure,ert);
	                }
	                
                }
                // add the unique positive ancestors (including original structure)
            	for(String posAncestor : posAncestors)
            	{
            		doc.addField(GxdResultFields.DIFF_POS_ANCESTORS, posAncestor);
            	}
            	
            	// go through the exclusive stage trackers and find the exclusive structures
            	for(ExclusiveStageTracker et : exclusiveTrackers.values())
            	{
            		et.calculateExclusiveStructures();
            		for(String structure : et.getExclusiveStructures())
            		{
            			doc.addField(GxdResultFields.DIFF_EXC_ANCESTORS,(new Structure(structure,et.stage)).toString());
            		}
            	}
            	
            	// further go through the calculated "by stage" exclusive structures 
            	// and apply special rules to get exclusive structures for all annotated stages
            	Set<String> exclusiveAllStageStructures = new HashSet<String>();
            	for(ExclusiveStageTracker et : exclusiveTrackers.values())
            	{
            		// I know some folks don't like loop labels, but I'm sorry, this particular logic calls for it.
            		structure1Loop:
            		for(String structure : et.getExclusiveStructures())
            		{
            			// only include this term if this exact term exists at least once in every stage
            			// OR is a substring of at least one term in every stage (Got that?)
            			// Ok, good. Let's begin...
            			
            			stage2Loop:
            			for(ExclusiveStageTracker et2 : exclusiveTrackers.values())
                    	{
            				//we must search every other stage's exclusive structures
            				if(et.stage!=et2.stage)
            				{
	                    		for(String structure2 : et2.getExclusiveStructures())
	                    		{
	                    			if(structure.equals(structure2) || structure2.contains(structure))
	                    			{
	                    				// found match! continue to next stage
	                    				continue stage2Loop;
	                    			}
	                    		}
	                    		// we did not find a match... have to continue to next structure
	                    		continue structure1Loop;
                    		}
                    	}
            			// Congratulations. You made it this far!
            			exclusiveAllStageStructures.add(structure);
            		}
            	}
            	
            	//add the unique exclusive all stage structures
            	for(String exclusiveAllStageStructure : exclusiveAllStageStructures)
            	{
        			doc.addField(GxdResultFields.DIFF_EXC_ANCESTORS_ALL_STAGES,exclusiveAllStageStructure);
            	}
            	
                docs.add(doc);
                if (docs.size() > 1000) 
                {
                    //logger.info("Adding a stack of the documents to Solr");
                	startTime();
                    writeDocs(docs);
                    long endTime = stopTime();
                    if(endTime > 500)
                    {
                    	logger.info("time to call writeDocs() "+stopTime());
                    }
                    docs = new ArrayList<SolrInputDocument>();
                }
            }
            if (! docs.isEmpty()) {
                server.add(docs);
            }
            
            server.commit();
        }
    }
    
    /*
     * For debugging purposes only
     */
    private long startTime = 0;
    public void startTime()
    {
    	startTime = System.nanoTime();
    }
    public long stopTime()
    {
    	long endTime = System.nanoTime();
    	return (endTime - startTime)/1000000;
    	
    }
    
    // helper classes
    public class Result
    {
    	public String structureKey;
    	public String structure;
    	public String stage;
    	public boolean expressed;
    	
    	public Result(String structureKey,String structure,String stage,boolean expressed)
    	{
    		this.structureKey=structureKey;
    		this.structure=structure;
    		this.stage=stage;
    		this.expressed=expressed;
    	}
    }
    public class Structure
    {
    	public String structure;
    	public String stage;
    	public Structure(String structure,String stage)
    	{
    		this.structure=structure;
    		this.stage=stage;
    	}
		@Override
		public String toString() {
			return "TS"+stage+":"+structure;
		}
    }
    
    // classes to help calculate exclusive structure fields
    /*
     * ExclusiveResultTracker
     * tracks structure + ancestors for a given positive result
     */
    public class ExclusiveResultTracker
    {
    	public Set<String> ancestorStructures = new HashSet<String>();
    	public ExclusiveResultTracker(){}
    	public void addAncestor(String structure)
    	{
    		ancestorStructures.add(structure);
    	}
    }
    
    /*
     * ExclusiveStageTracker
     * tracks exclusive structures within a theiler stage
     */
    public class ExclusiveStageTracker
    {
    	public String stage;
    	
    	private Map<String,ExclusiveResultTracker> resultTrackers = new HashMap<String,ExclusiveResultTracker>();
    	private Set<String> exclusiveStructures = new HashSet<String>();
    	
    	ExclusiveStageTracker(String stage)
    	{
    		this.stage=stage;
    	}
    	public void addResultTracker(String structure,ExclusiveResultTracker ert)
    	{
    		if(!resultTrackers.containsKey(structure))
    		{
    			resultTrackers.put(structure,ert);
    		}
    	}
    	
    	// determine from the current list of structures, which ones are exclusive
    	public void calculateExclusiveStructures()
    	{
    		// do set difference of all ancestors for each result tracker
    		for(ExclusiveResultTracker ert : resultTrackers.values())
    		{
    			if(exclusiveStructures.size()==0) exclusiveStructures = ert.ancestorStructures;
    			else exclusiveStructures.retainAll(ert.ancestorStructures);
    		}
    	}
    	public Set<String> getExclusiveStructures()
    	{
    		return exclusiveStructures;
    	}
    }
    
}
