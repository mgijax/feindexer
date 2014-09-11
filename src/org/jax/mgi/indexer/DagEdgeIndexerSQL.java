package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.DagEdgeFields;

/**
 * DagEdgeIndexerSQL
 * @author kstone
 * This index is intended to be a lookup for all vocab term edges (i.e. parent->child relationships)
 *  
 *  For now it just has the EMAPA DAG (with additional data for EMAPA IDs)
 * 
 */

public class DagEdgeIndexerSQL extends Indexer 
{   
    public DagEdgeIndexerSQL () 
    { super("index.url.dagEdge"); }
    
    public void index() throws Exception
    {    
    	ResultSet rs = ex.executeProto("select max(t.term_key) as max_term_key, min(t.term_key) as min_term_key from term t join " +
    			"term_child tc on tc.term_key=t.term_key where t.vocab_name='EMAPA'");
     	rs.next();
     	
     	Integer start = rs.getInt("min_term_key") - 1 ;
        Integer stop = rs.getInt("max_term_key");
        logger.info("min term key = "+start+", max term key = "+stop);
     	int chunkSize = 5000;
         
        int modValue = (stop-start) / chunkSize;
        // Perform the chunking
        for (int i = 0; i <= modValue; i++) 
        {
             start = start + (i * chunkSize);
             stop = start + chunkSize;
             
             logger.info("Loading direct edges for terms "+start+" to "+stop);
             processDirectEdges(start,stop);
             
             logger.info("Loading descendent edges for terms "+start+" to "+stop);
             processDescendentEdges(start,stop);
        }
        logger.info("load completed");
    }
    
    private void processDirectEdges(int start,int stop) throws Exception
    {
    	Map<String,EMAPAInfo> emapaInfoMap = getEmapaInfo(start,stop);
    	
    	Map<String,Set<String>> edgeAncestorMap = getEdgeAncestors(start,stop);
    	
    	Map<String,Set<String>> edgeDescendentMap = getEdgeDescendents(start,stop);
    	
    	String query = "select tc.unique_key, p.term_key parent_term_key,\n" + 
    			"p.term parent_term,\n" + 
    			"p.primary_id parent_id,\n" + 
    			"tc.child_term_key,\n" + 
    			"tc.child_term,\n" + 
    			"tc.child_primary_id child_id,\n" + 
    			"p.vocab_name vocab " +
    			"from term p join\n" + 
    			"term_child tc on tc.term_key=p.term_key \n" + 
    			"where p.vocab_name='EMAPA' "
    			+ "AND tc.term_key>"+start+" AND tc.term_key<="+stop;
    	
    	ResultSet rs = ex.executeProto(query);
    	List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    	while (rs.next()) 
        {
        	SolrInputDocument doc = new SolrInputDocument();
            String uniqueKey = rs.getString("vocab")+"_direct_"+rs.getString("unique_key");
            Integer parentKey = rs.getInt("parent_term_key");
            String parentKeyString = parentKey.toString();
            Integer childKey = rs.getInt("child_term_key");
            String childKeyString = childKey.toString();
            
            doc.addField(IndexConstants.UNIQUE_KEY,uniqueKey);
            doc.addField(DagEdgeFields.CHILD_TERM_KEY,childKey);
            doc.addField(DagEdgeFields.CHILD_TERM,rs.getString("child_term"));
            doc.addField(DagEdgeFields.CHILD_ID,rs.getString("child_id"));
            doc.addField(DagEdgeFields.VOCAB,rs.getString("vocab"));
            doc.addField(DagEdgeFields.PARENT_TERM_KEY,parentKey);
            doc.addField(DagEdgeFields.PARENT_TERM,rs.getString("parent_term"));
            doc.addField(DagEdgeFields.PARENT_ID,rs.getString("parent_id"));
            doc.addField(DagEdgeFields.EDGE_TYPE,DagEdgeFields.DIRECT_EDGE_TYPE);
            
            this.addAllFromLookup(doc,DagEdgeFields.RELATED_ANCESTOR,parentKeyString,edgeAncestorMap);
            this.addAllFromLookup(doc,DagEdgeFields.RELATED_DESCENDENT,childKeyString,edgeDescendentMap);
            
            if(emapaInfoMap.containsKey(parentKeyString))
            {
            	EMAPAInfo ei = emapaInfoMap.get(parentKeyString);
            	doc.addField(DagEdgeFields.PARENT_START_STAGE,ei.startStage);
            	doc.addField(DagEdgeFields.PARENT_END_STAGE,ei.endStage);
            }
            if(emapaInfoMap.containsKey(childKeyString))
            {
            	EMAPAInfo ei = emapaInfoMap.get(childKeyString);
            	doc.addField(DagEdgeFields.CHILD_START_STAGE,ei.startStage);
            	doc.addField(DagEdgeFields.CHILD_END_STAGE,ei.endStage);
            }
            
            docs.add(doc);
        }
        if(docs.size()>0) server.add(docs);
        server.commit();
    }
    
    private void processDescendentEdges(int start,int stop) throws Exception
    {	
    	String query = "select td.unique_key, p.term_key parent_term_key,\n" + 
    			"p.term parent_term,\n" + 
    			"p.primary_id parent_id,\n" + 
    			"td.descendent_term_key,\n" + 
    			"td.descendent_term,\n" + 
    			"td.descendent_primary_id descendent_id,\n" + 
    			"p.vocab_name vocab " +
    			"from term p join\n" + 
    			"term_descendent td on td.term_key=p.term_key \n" + 
    			"where p.vocab_name='EMAPA' "
    			+ "AND td.term_key>"+start+" AND td.term_key<="+stop;
    	
    	ResultSet rs = ex.executeProto(query);
    	List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    	while (rs.next()) 
        {
        	SolrInputDocument doc = new SolrInputDocument();
            String uniqueKey = rs.getString("vocab")+"_descendent_"+rs.getString("unique_key");
            Integer parentKey = rs.getInt("parent_term_key");
            Integer childKey = rs.getInt("descendent_term_key");
            
            doc.addField(IndexConstants.UNIQUE_KEY,uniqueKey);
            doc.addField(DagEdgeFields.CHILD_TERM_KEY,childKey);
            doc.addField(DagEdgeFields.CHILD_TERM,rs.getString("descendent_term"));
            doc.addField(DagEdgeFields.CHILD_ID,rs.getString("descendent_id"));
            doc.addField(DagEdgeFields.VOCAB,rs.getString("vocab"));
            doc.addField(DagEdgeFields.PARENT_TERM_KEY,parentKey);
            doc.addField(DagEdgeFields.PARENT_TERM,rs.getString("parent_term"));
            doc.addField(DagEdgeFields.PARENT_ID,rs.getString("parent_id"));
            doc.addField(DagEdgeFields.EDGE_TYPE,DagEdgeFields.DESCENDENT_EDGE_TYPE);
            
            docs.add(doc);
        }
        if(docs.size()>0) server.add(docs);
        server.commit();
    }
    
    /*
     * Returns map of term-key to EMAPAInfo
     */
    private Map<String,EMAPAInfo> getEmapaInfo(int start,int stop) throws Exception
    {
    	logger.debug("building map of term_key to EMAPAInfo");
    	Map<String,EMAPAInfo> emapaInfoMap = new HashMap<String,EMAPAInfo>();
    	String emapaInfoQuery = "WITH\n" + 
    			"emapa_tc AS (select term_key,child_term_key\n" + 
    			"	from term_child\n" + 
    			"	where ((term_key>"+start+" and term_key<="+stop+") \n" + 
    			"		OR (child_term_key>"+start+" and child_term_key<="+stop+"))\n" + 
    			"	)\n" + 
    			"select te.term_key,te.start_stage,te.end_stage\n" + 
    			"from emapa_tc tc join \n" + 
    			"term_emap te on te.term_key=tc.term_key\n" + 
    			"UNION\n" + 
    			"select te.term_key,te.start_stage,te.end_stage\n" + 
    			"from emapa_tc tc join \n" + 
    			"term_emap te on te.term_key=tc.child_term_key\n";
    	ResultSet rs = ex.executeProto(emapaInfoQuery);
    	while(rs.next())
    	{
    		emapaInfoMap.put(rs.getString("term_key"),
    				new EMAPAInfo(rs.getInt("start_stage"),rs.getInt("end_stage"))
    		);
    	}
    	logger.debug("done building map of term_key to EMAPAInfo");
    	return emapaInfoMap;
    }
    
    /*
     * Returns map of parent_term_key to Ancestor term IDs
     */
    private Map<String,Set<String>> getEdgeAncestors(int start,int stop) throws Exception
    {
    	logger.debug("building map of parent_term_key to ancestor terms");
    	String query = "select tc.term_key parent_key,\n" + 
    			"tas.ancestor_primary_id ancestor_id\n" + 
    			"from term_child tc join\n" + 
    			"term_ancestor_simple tas on tas.term_key=tc.term_key\n" + 
    			"where tc.term_key>"+start+" and tc.term_key<="+stop;
    	logger.debug("done building map of term_child.unique_key to ancestor terms");
    	return this.populateLookup(query,"parent_key","ancestor_id","parent_term_key->ancestor_id");
    }
    
    /*
     * Returns map of child_term_key to descendent term IDs
     */
    private Map<String,Set<String>> getEdgeDescendents(int start,int stop) throws Exception
    {
    	logger.debug("building map of child_term_key to descendent terms");
    	String query = "select tc.child_term_key child_key,\n" + 
    			"td.descendent_primary_id descendent_id\n" + 
    			"from term_child tc join\n" + 
    			"term_descendent td on td.term_key=tc.child_term_key\n" + 
    			"where tc.term_key>"+start+" and tc.term_key<="+stop;
    	logger.debug("done building map of term_child.unique_key to ancestor terms");
    	return this.populateLookup(query,"child_key","descendent_id","child_term_key->descendent_id");
    }
    
    private class EMAPAInfo {
    	public int startStage;
    	public int endStage;
    	
    	public EMAPAInfo(int startStage, int endStage) {
			this.startStage=startStage;
			this.endStage=endStage;
		}
    }
}
