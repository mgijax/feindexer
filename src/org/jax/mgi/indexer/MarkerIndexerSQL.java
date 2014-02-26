package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
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
    	this.tempTables();
    	
        // How many markers are there total?
        ResultSet rs = ex.executeProto("select max(marker_key) as maxMarkerKey from marker");
        rs.next();
        
        Integer start = 0;
        Integer end = rs.getInt("maxMarkerKey");
    	int chunkSize = 20000;
        
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
    	
    	 // Get marker id -> marker relationships
        String markerToIDSQL = "select distinct marker_key, acc_id from marker_id where marker_key > " + start + " and marker_key <= "+ end + " and private = 0";
        Map<String,Set<String>> idToMarkers = this.populateLookup(markerToIDSQL, "marker_key", "acc_id","marker to IDs");

        // Get marker -> reference relationships, by marker key
        String markerToReferenceSQL = "select distinct marker_key, reference_key from marker_to_reference where marker_key > " + start + " and marker_key <= "+ end;
        Map<String,Set<String>> referenceToMarkers = this.populateLookup(markerToReferenceSQL, "marker_key", "reference_key","marker to ref keys");
                    
        // Get marker -> vocab relationships, by marker key
        String markerToTermSQL = "select distinct m.marker_key, a.term, a.annotation_type, a.term_id from marker_to_annotation m, annotation a where m.marker_key > " + start + " and m.marker_key <= "+ end + " and m.annotation_key = a.annotation_key";
        Map<String,Set<String>> termToMarkers = makeVocabHash(markerToTermSQL, "marker_key", "term");

        // Get marker terms and their IDs
        String markerToTermIDSQL = "select distinct m.marker_key, a.term_id from marker_to_annotation m, annotation a where m.marker_key > " + start + " and m.marker_key <= "+ end + " and m.annotation_key = a.annotation_key";
        Map<String,Set <String>> termToMarkersID = this.populateLookup(markerToTermIDSQL, "marker_key", "term_id","marker to Terms/IDs");
        
        // Get marker location information
        Map<Integer,MarkerLocation> locationMap = getMarkerLocations(start,end);
        
        // Get marker nomen information
        Map<Integer,List<MarkerNomen>> nomenMap = getMarkerNomen(start,end);
        
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
        	int mrkKeyInt = rs.getInt("marker_key");
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
            if(locationMap.containsKey(mrkKeyInt))
            {
            	MarkerLocation ml = locationMap.get(mrkKeyInt);
            	// add any location data for this marker
            	if(ml.chromosome!=null) doc.addField(IndexConstants.CHROMOSOME, ml.chromosome);
            	if(ml.startCoordinate>0) doc.addField(IndexConstants.START_COORD, ml.startCoordinate);
            	if(ml.endCoordinate>0) doc.addField(IndexConstants.END_COORD, ml.endCoordinate);
            	if(ml.cmOffset>0.0) doc.addField(IndexConstants.CM_OFFSET, ml.cmOffset);
            }
            
            /*
             * Marker nomen
             */
            if(nomenMap.containsKey(mrkKeyInt))
            {
            	for(MarkerNomen mn : nomenMap.get(mrkKeyInt))
            	{
            		doc.addField(mapNomenField(mn.termType),mn.term);
            	}
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
    
    private Map<Integer,MarkerLocation> getMarkerLocations(int start,int end) throws Exception
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
    
    private Map<Integer,List<MarkerNomen>> getMarkerNomen(int start,int end) throws Exception
    {
    	logger.info("building map of marker_keys -> marker nomen");
    	String mrkNomenQuery = "select marker_key, nomen, term_type " +
    			"tmp_marker_nomen mn " +
    			"where mn.marker_key > "+start+" and mn.marker_key <= "+end;

    	Map<Integer,List<MarkerNomen>> nomenMap = new HashMap<Integer,List<MarkerNomen>>();
    	ResultSet rs = ex.executeProto(mrkNomenQuery);
    	while(rs.next())
    	{
    		Integer mrkKey = rs.getInt("marker_key");
    		
    		MarkerNomen mn = new MarkerNomen();
    		mn.term = rs.getString("term");
    		mn.termType = rs.getString("term_type");
    		
    		if(!nomenMap.containsKey(mrkKey)) nomenMap.put(mrkKey,new ArrayList<MarkerNomen>(Arrays.asList(mn)));
    		else nomenMap.get(mrkKey).add(mn);
    	}
    	logger.info("done building map of marker_keys -> marker nomen");
    	return nomenMap;
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
    
    private void tempTables() throws Exception
    {
    	// create marker to nomenclature
    	logger.info("creating temp table of marker_key to nomenclature");
    	String mrkNomenQuery = "select msn.marker_key, msn.term nomen, msn.term_type " +
    			"into temp tmp_marker_nomen " +
    			"from marker_searchable_nomenclature msn " +
    			"where msn.term_type in ('human name','human synonym','human symbol'," +
        				"'current symbol','current name','old symbol','synonym','related synonym','old name' ) ";
    	this.ex.executeVoid(mrkNomenQuery);
    	createTempIndex("tmp_marker_nomen","marker_key");
    	logger.info("done creating temp table of marker_key to nomenclature");
    }
    
    protected String translateVocab(String value, String vocab) 
    {
        if (vocab.equals("GO/Marker"))   return "Function: " + value;
        if (vocab.equals("InterPro/Marker"))   return "Protein Domain: " + value;
        if (vocab.equals("PIRSF/Marker"))  return "Protein Family: " + value;
        if (vocab.equals("OMIM/Human Marker"))  return "Disease Model: " + value;
        
        return "";
    }
    
    private String mapNomenField(String termType)
    {
    	if("human name".equals(termType)) return "humanName";
    	if("human synonym".equals(termType)) return "humanSynonym";
    	if("human symbol".equals(termType)) return "humanSymbol";
    	if("current symbol".equals(termType)) return "currentSymbol";
    	if("current name".equals(termType)) return "currentName";
    	if("old symbol".equals(termType)) return "oldSymbol";
    	if("synonym".equals(termType)) return "synonym";
    	if("related synonym".equals(termType)) return "relatedSynonym";
    	if("old name".equals(termType)) return "oldName";

    	return termType;
    }
    
    // helper class for storing marker location info
    public class MarkerLocation
    {
    	String chromosome=null;
    	Integer startCoordinate=0;
    	Integer endCoordinate=0;
    	Double cmOffset=0.0;
    }
    
    // helper class for storing marker nomen info
    public class MarkerNomen
    {
    	String term;
    	String termType;
    }
    
    private boolean notEmpty(String s)
    {
    	return s!=null && !s.equals("");
    }
}
