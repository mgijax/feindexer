package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.indexer.AlleleIndexerSQL.AlleleLocation;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * MarkerIndexerSQL
 * @author kstone
 * This class populates the marker index, which powers both marker ID lookups
 * 	as well as the marker summary.
 */

public class MarkerIndexerSQL extends Indexer 
{  
    public MarkerIndexerSQL () 
    {
        super("index.url.marker");
    }
    
    /**
     * The main worker of this class, it starts by gathering up all the 1->N 
     * subobject relationships that a reference can have, and then runs the 
     * main query.  We then enter a parsing phase where we place the data into
     * solr documents, and put them into the index.
     */
    public void index() throws Exception
    {
            // How many markers are there total?
            ResultSet rs = ex.executeProto("select max(marker_key) as maxMarkerKey from marker");
            rs.next();
            
            Integer start = 0;
            Integer end = rs.getInt("maxMarkerKey");
        	int chunkSize = 5000;
            
            int modValue = end.intValue() / chunkSize;
            
            // Perform the chunking
            logger.info("Loading markers up to marker_key="+end);
            for (int i = 0; i <= modValue; i++) 
            {
                start = i * chunkSize;
                end = start + chunkSize;
                
                processMarkers(start,end);
            }
            
           logger.info("Done loading markers");
    }
    
    private void processMarkers(int start, int end) throws Exception
    {
    	logger.info("Processing marker keys "+start+" to "+end);
    	
    	 // Get all marker id -> marker relationships
        String markerToIDSQL = "select distinct marker_key, acc_id from marker_id where marker_key > " + start + " and marker_key <= "+ end + " and private = 0";
        Map<String,Set<String>> idToMarkers = this.populateLookup(markerToIDSQL, "marker_key", "acc_id","marker to IDs");

        // Get all marker -> reference relationships, by marker key
        String markerToReferenceSQL = "select distinct marker_key, reference_key from marker_to_reference where marker_key > " + start + " and marker_key <= "+ end;
        Map<String,Set<String>> referenceToMarkers = this.populateLookup(markerToReferenceSQL, "marker_key", "reference_key","marker to ref keys");
                    
        // Get all marker -> vocab relationships, by marker key
        String markerToTermSQL = "select distinct m.marker_key, a.term, a.annotation_type, a.term_id from marker_to_annotation m, annotation a where m.marker_key > " + start + " and m.marker_key <= "+ end + " and m.annotation_key = a.annotation_key";
        Map<String,Set<String>> termToMarkers = makeVocabHash(markerToTermSQL, "marker_key", "term");

        // Get all marker terms and their IDs
        String markerToTermIDSQL = "select distinct m.marker_key, a.term_id from marker_to_annotation m, annotation a where m.marker_key > " + start + " and m.marker_key <= "+ end + " and m.annotation_key = a.annotation_key";
        Map<String,Set <String>> termToMarkersID = this.populateLookup(markerToTermIDSQL, "marker_key", "term_id","marker to Terms/IDs");
        
        // Get all marker location information
        Map<Integer,MarkerLocation> locationMap = getMarkerLocations(start,end);
        
        logger.info("Getting all mouse markers");
        String markerSQL = "select distinct marker_key, primary_id marker_id,symbol, " +
        			" name, marker_type, marker_subtype, status, organism from marker " +
        		"where organism = 'mouse' " +
        		"	and marker_key > "+start+" and marker_key <= "+end;
        ResultSet rs = ex.executeProto(markerSQL);
        
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
      
        // Parse the base query, adding its contents into solr  
        logger.info("Parsing them");
        while (rs.next()) 
        {
        	String mrkKey = rs.getString("marker_key");
            SolrInputDocument doc = new SolrInputDocument();
            
            doc.addField(IndexConstants.MRK_KEY, rs.getString("marker_key"));
            doc.addField(IndexConstants.MRK_SYMBOL, rs.getString("symbol"));
            doc.addField(IndexConstants.MRK_NAME, rs.getString("name"));
            doc.addField(IndexConstants.MRK_TYPE, rs.getString("marker_type"));
            doc.addField("featureType", rs.getString("marker_subtype"));
            doc.addField(IndexConstants.MRK_STATUS, rs.getString("status"));
            doc.addField(IndexConstants.MRK_ORGANISM, rs.getString("organism"));
            String markerID = rs.getString("marker_id");
            if (markerID==null) markerID = "";
            doc.addField(IndexConstants.MRK_PRIMARY_ID,markerID);
            
            // Parse the 1->N marker relationships here
            this.addAllFromLookup(doc,IndexConstants.REF_KEY,mrkKey,referenceToMarkers);
            this.addAllFromLookup(doc,IndexConstants.MRK_TERM,mrkKey,termToMarkers);
            this.addAllFromLookup(doc,IndexConstants.MRK_ID,mrkKey,idToMarkers);
            this.addAllFromLookup(doc,IndexConstants.MRK_TERM_ID,mrkKey,termToMarkersID);
            
            /*
             * Marker Location data
             */
            if(locationMap.containsKey(mrkKey))
            {
            	MarkerLocation ml = locationMap.get(mrkKey);
            	// add any location data for this marker
            	if(ml.chromosome!=null) doc.addField(IndexConstants.CHROMOSOME, ml.chromosome);
            	if(ml.startCoordinate>0) doc.addField(IndexConstants.START_COORD, ml.startCoordinate);
            	if(ml.endCoordinate>0) doc.addField(IndexConstants.END_COORD, ml.endCoordinate);
            	if(ml.cmOffset>0.0) doc.addField(IndexConstants.CM_OFFSET, ml.cmOffset);
            }
            docs.add(doc);
            
            if (docs.size() > 1000) 
            {
                writeDocs(docs);
                docs = new ArrayList<SolrInputDocument>();
            }
        }
        if(docs.size()>0) server.add(docs);
        server.commit();
    }
    
    public Map<Integer,MarkerLocation> getMarkerLocations(int start,int end) throws Exception
    {
    	logger.info("building map of marker_keys -> marker locations");
    	String locationQuery="select ml.* " +
    			"from marker_location ml " +
    			"where ml.marker_key > "+start+" and ml.marker_key <= "+end+" ";
    	Map<Integer,MarkerLocation> locationMap = new HashMap<Integer,MarkerLocation>();
    	ResultSet rs = ex.executeProto(locationQuery);
    	while(rs.next())
    	{
    		Integer mrkKey = rs.getInt("marker_key");
    		String chromosome = rs.getString("chromosome");
    		Integer startCoord = rs.getInt("start_coordinate");
    		Integer endCoord = rs.getInt("end_coordinate");
    		Double cmOffset = rs.getDouble("cm_offset");
    		
    		if(!locationMap.containsKey(mrkKey)) locationMap.put(mrkKey,new MarkerLocation());
    		MarkerLocation al = locationMap.get(mrkKey);
    		
    		// set any non-null fields from this location row
    		if(chromosome!=null) al.chromosome=chromosome;
    		if(startCoord>0) al.startCoordinate=startCoord;
    		if(endCoord>0) al.endCoordinate=endCoord;
    		if(cmOffset>0.0) al.cmOffset=cmOffset;
    	}
    	logger.info("done building map of marker_keys -> marker locations");
    	return locationMap;
    }
    
    private Map<String,Set<String>> makeVocabHash(String sql, String keyString, String valueString) throws Exception
    {   
        Map <String,Set <String>> tempMap = new HashMap <String,Set<String>>();
        
        ResultSet rs = ex.executeProto(sql); 
        while (rs.next()) 
        {
            String key = rs.getString(keyString);
            String value = rs.getString(valueString);
            String vocab = rs.getString("annotation_type");
            if (tempMap.containsKey(key)) 
            {
                tempMap.get(key).add(translateVocab(value, vocab));
            }
            else 
            {
                HashSet <String> temp = new HashSet <String> ();
                temp.add(translateVocab(value, vocab));
                tempMap.put(key, temp);
            }
        }
        return tempMap;
    }
    
    protected String translateVocab(String value, String vocab) 
    {
        if (vocab.equals("GO/Marker"))   return "Function: " + value;
        if (vocab.equals("InterPro/Marker"))   return "Protein Domain: " + value;
        if (vocab.equals("PIRSF/Marker"))  return "Protein Family: " + value;
        if (vocab.equals("OMIM/Human Marker"))  return "Disease Model: " + value;
        
        return "";
    }
    
    // helper class for storing marker location info
    public class MarkerLocation
    {
    	String chromosome=null;
    	Integer startCoordinate=0;
    	Integer endCoordinate=0;
    	Double cmOffset=0.0;
    }
    private boolean notEmpty(String s)
    {
    	return s!=null && !s.equals("");
    }
}
