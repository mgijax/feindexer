package org.jax.mgi.indexer;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;
import org.jax.mgi.shr.fe.indexconstants.ImagePaneFields;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

/**
 * GXDImagePaneIndexerSQL
 * @author kstone
 * This index is has the primary responsibility of populating the GXD Result solr index.
 * Each document in this index represents an assay result. This index can/will have fields to group by
 * assayKey and markerKey
 * 
 */

public class GXDImagePaneIndexerSQL extends Indexer 
{   
	// class variables
	public static Map<String,Integer> ASSAY_TYPE_SEQ_MAP = new HashMap<String,Integer>();
	static
	{
		ASSAY_TYPE_SEQ_MAP.put("Immunohistochemistry", 1);
		ASSAY_TYPE_SEQ_MAP.put("RNA in situ", 2);
		ASSAY_TYPE_SEQ_MAP.put("In situ reporter (knock in)", 3);
		ASSAY_TYPE_SEQ_MAP.put("Northern blot", 4);
		ASSAY_TYPE_SEQ_MAP.put("Western blot", 5);
		ASSAY_TYPE_SEQ_MAP.put("RT-PCR", 6);
		ASSAY_TYPE_SEQ_MAP.put("RNase protection", 7);
		ASSAY_TYPE_SEQ_MAP.put("Nuclease S1", 8);
	}
	public static SmartAlphaComparator sac = new SmartAlphaComparator();
	
    public GXDImagePaneIndexerSQL () 
    { super("index.url.gxdImagePane"); }
    
    public void index() throws Exception
    {    
        	String imageQuery="select ei.result_key,ei.imagepane_key,ers.assay_id "+
        			"from expression_result_to_imagepane ei,expression_result_summary ers "+
        			"where ei.result_key=ers.result_key";
        	Map<Integer,List<Integer>> imagePaneResultMap = new HashMap<Integer,List<Integer>>();
        	Map<Integer,String> assayIDMap = new HashMap<Integer,String>();
        	logger.info("building map of image pane keys to result keys");
        	
            ResultSet rs = ex.executeProto(imageQuery);

	        while (rs.next())
	        {
	        	int ipKey = rs.getInt("imagepane_key");
	        	int resultKey = rs.getInt("result_key");
	        	if(!imagePaneResultMap.containsKey(ipKey))
	        	{
	        		imagePaneResultMap.put(ipKey, new ArrayList<Integer>());
	        	}
	        	imagePaneResultMap.get(ipKey).add(resultKey);
	        	
	        	assayIDMap.put(ipKey, rs.getString("assay_id"));
	        }
	        logger.info("done building map of image pane keys to result keys");
	        
        	ResultSet rs_tmp = ex.executeProto("select max(imagepane_key) as max_ip_key from expression_imagepane");
        	rs_tmp.next();
        	
        	Integer start = 0;
            Integer end = rs_tmp.getInt("max_ip_key");
        	int chunkSize = 150000;
            
            int modValue = end.intValue() / chunkSize;
            
            // Perform the chunking, this might become a configurable value later on

            logger.info("Getting all image panes");
            
            for (int i = 0; i <= modValue; i++) {
            
	            start = i * chunkSize;
	            end = start + chunkSize;
	            
	            String geneQuery="select eri.imagepane_key,ers.assay_type,ers.marker_symbol, s.specimen_label,ers.assay_id "+
		        		"from expression_result_to_imagepane eri,  " +
		        		"expression_result_summary ers LEFT OUTER JOIN " +
		        		"assay_specimen s ON ers.specimen_key=s.specimen_key "+
		        		"where eri.result_key=ers.result_key " +
		        		"and eri.imagepane_key > "+start+" and eri.imagepane_key <= "+end+" ";
	        	Map<Integer,Map<String,MetaData>> imagePaneMetaMap = new HashMap<Integer,Map<String,MetaData>>();
	        	logger.info("building map of image pane keys to meta data, ie. gene symbols + assay types + specimen labels");
	        	
	            rs = ex.executeProto(geneQuery);

		        while (rs.next())
		        {
		        	int ipKey = rs.getInt("imagepane_key");
		        	String assayType = rs.getString("assay_type");
		        	String markerSymbol = rs.getString("marker_symbol");
		        	MetaData md = new MetaData(assayType,markerSymbol);
		        	
		        	// init the meta data map for this pane
		        	if(!imagePaneMetaMap.containsKey(ipKey))
		        	{
		        		imagePaneMetaMap.put(ipKey, new HashMap<String,MetaData>());
		        	}
		        	Map<String,MetaData> metaMap = imagePaneMetaMap.get(ipKey);
		        	
		        	//init the meta data for this gene/assay type combo
		        	if(!metaMap.containsKey(md.toKey()))
		        	{
		        		metaMap.put(md.toKey(), md);
		        	}
		        	else md = metaMap.get(md.toKey());
		        	
		        	// append and specimen labels
		        	String specLabel = rs.getString("specimen_label");
		        	String assayID = rs.getString("assay_id");
		        	md.addSpecimenLabel(specLabel,assayID);
		        }
		        logger.info("done building map of image pane keys to meta data");
		        
		        logger.info("sorting map of image pane keys to meta data");
		        Map<Integer,List<MetaData>> imagePaneSortedMetaMap = new HashMap<Integer,List<MetaData>>();
		        for(Integer key : imagePaneMetaMap.keySet())
		        {
		        	List<MetaData> sortedMeta = new ArrayList<MetaData>(imagePaneMetaMap.get(key).values());
		        	// actually sort the meta
		        	Collections.sort(sortedMeta);
		        	imagePaneSortedMetaMap.put(key,sortedMeta);
		        }
		        imagePaneMetaMap = null; // mark for garbage collection
		        logger.info("done sorting map of image pane keys to meta data");
		        
		        
	            logger.info ("Processing imagepane key > " + start + " and <= " + end);
	            String query = "select i.mgi_id,ip.imagepane_key, " +
	            		"i.figure_label, i.pixeldb_numeric_id, ip.pane_label, " +
	            		"ip.x,ip.y,ip.width,ip.height, " +
	            		"ip.by_default pane_sort_seq, " +
	            		"i.width image_width, i.height image_height " +
	            		" from image i,expression_imagepane ip where i.image_key=ip.image_key " +
	            		"and i.pixeldb_numeric_id is not null "+
	                    "and ip.imagepane_key > "+start+" and ip.imagepane_key <= "+end+" ";
	            rs = ex.executeProto(query);
	            
	            //Map<String,SolrInputDocument> docs = new HashMap<String,SolrInputDocument>();
	            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	            
	            logger.info("Parsing them");
	            while (rs.next()) 
	            {           
	            	int imagepane_key = rs.getInt("imagepane_key");
	            	String imageID = rs.getString("mgi_id");
	            	String assayID = assayIDMap.containsKey(imagepane_key) ? assayIDMap.get(imagepane_key): "";

	            	SolrInputDocument doc = new SolrInputDocument();
	            	doc.addField(ImagePaneFields.IMAGE_PANE_KEY, imagepane_key);
	            	doc.addField(IndexConstants.UNIQUE_KEY, ""+imagepane_key);
	            	doc.addField(IndexConstants.IMAGE_ID, imageID);
	            	doc.addField(GxdResultFields.ASSAY_MGIID,assayID);
	            	
	            	doc.addField(ImagePaneFields.IMAGE_PIXELDBID, rs.getString("pixeldb_numeric_id"));
	            	String paneLabel = rs.getString("pane_label")!=null ? rs.getString("pane_label") : "";
	            	doc.addField(ImagePaneFields.IMAGE_LABEL, rs.getString("figure_label")+paneLabel);
	            	
	            	doc.addField(ImagePaneFields.IMAGE_WIDTH, rs.getInt("image_width"));
	            	doc.addField(ImagePaneFields.IMAGE_HEIGHT, rs.getInt("image_height"));
	            	doc.addField(ImagePaneFields.PANE_WIDTH, rs.getInt("width"));
	            	doc.addField(ImagePaneFields.PANE_HEIGHT, rs.getInt("height"));
	            	doc.addField(ImagePaneFields.PANE_X, rs.getInt("x"));
	            	doc.addField(ImagePaneFields.PANE_Y, rs.getInt("y"));
	            	
	            	// add the default sort field
	            	doc.addField(IndexConstants.BY_DEFAULT, rs.getInt("pane_sort_seq"));

	            	//get results
	            	// if this lookup fails, then there is probably a data inconsistency
	            	// we need all the expression_gatherers run at the same time to get the db keys in line
	            	List<Integer> expressionResultKeys = imagePaneResultMap.get(imagepane_key);
	            	if(expressionResultKeys == null)
	            	{
	            		// these keys are the whole point of this index. Without them, we can't join to it to get images.
	            		// This may happen in a case with inconsistent image data.
	            		continue;
	            	}
	            	
	            	for(Integer result_key : expressionResultKeys)
	            	{
	            		doc.addField(GxdResultFields.RESULT_KEY,result_key);
	            	}
	            	
	            	if(imagePaneSortedMetaMap.containsKey(imagepane_key))
	            	{
	            		for(MetaData md : imagePaneSortedMetaMap.get(imagepane_key))
	            		{
	            			doc.addField(ImagePaneFields.IMAGE_META, md.toString());
	            		}
	            	}
		                
                    docs.add(doc);
	                if (docs.size() > 1000) {
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
    
    // Meta data for an image pane
    public class MetaData implements Comparable<MetaData>
    {	
    	// instance variables
    	public String assayType;
    	public String markerSymbol;
    	public List<String> specimenLabels=new ArrayList<String>();
    	
    	public MetaData(String assayType,String markerSymbol)
    	{
    		this.assayType=assayType;
    		this.markerSymbol=markerSymbol;
    	}

		public void addSpecimenLabel(String specimen,String assayID)
    	{
			if (specimen == null) specimen = "null";
			// need to attach the assayID for linking
			String label = specimen+"|"+assayID;
    		// keep the labels unique
    		if(!specimenLabels.contains(label)) specimenLabels.add(label);
    	}
    	
    	// a key to be used in maps that represents the uniqueness of this object
    	public String toKey()
    	{
    		return assayType+"||"+markerSymbol;
    	}
    	
    	public String toString()
    	{
    		return assayType+"||"+markerSymbol+"||"+StringUtils.join(specimenLabels,"\t\t");
    	}

		@Override
		public int compareTo(MetaData arg0)
		{
			// sort assayType first
			int assayTypeSeq1 = ASSAY_TYPE_SEQ_MAP.containsKey(this.assayType) ? ASSAY_TYPE_SEQ_MAP.get(this.assayType): 99;
			int assayTypeSeq2 = ASSAY_TYPE_SEQ_MAP.containsKey(arg0.assayType) ? ASSAY_TYPE_SEQ_MAP.get(arg0.assayType): 99;
			if(assayTypeSeq1 > assayTypeSeq2) return 1;
			else if (assayTypeSeq1 < assayTypeSeq2) return -1;
			
			// assay types are equal, try sorting by markerSymbol
			return sac.compare(this.markerSymbol, arg0.markerSymbol);
		}
    	
    }
}
