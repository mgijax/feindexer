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
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;

/**
 * GXDImagePaneIndexerSQL
 * @author kstone
 * This index is to be joined with the diseasePortal index to facilitate building the grid
 * 	It loads annotations that have been rolled up and grouped by genotype cluster (AKA genocluster)
 * 
 */

public class DiseasePortalAnnotationIndexerSQL extends Indexer 
{   

    public DiseasePortalAnnotationIndexerSQL () 
    { super("index.url.diseasePortalAnnotation"); }
    
    public void index() throws Exception
    {    
        	String clusterToClusterQuery="select gcm.hdp_gridcluster_key, genoC.hdp_genocluster_key "+
				"from hdp_gridcluster_marker gcm, "+
					"hdp_genocluster genoC "+
				"where gcm.marker_key=genoC.marker_key ";
        	logger.info("building map of grid cluster keys to geno cluster keys");
        	Map<Integer,Integer> clusterToClusterMap = new HashMap<Integer,Integer>();
            ResultSet rs = ex.executeProto(clusterToClusterQuery);

	        while (rs.next())
	        {
	        	int gridKey = rs.getInt("hdp_gridcluster_key");
	        	int genoKey = rs.getInt("hdp_genocluster_key");
	        	clusterToClusterMap.put(genoKey,gridKey);
	        }
	        logger.info("done building map of grid cluster keys to geno cluster keys");
	        

            logger.info("Getting all hdp_genocluster_annotation rows");

            String query = "select gca.hdp_genocluster_key, " +
            		"gca.annotation_type, " +
            		"gca.term_type, " +
            		"gca.term, " +
            		"gca.term_id, " +
            		"gca.qualifier_type "+
            		"from hdp_genocluster_annotation gca ";
            
            rs = ex.executeProto(query);
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            logger.info("Parsing them");
            int uniqueKey = 0;
            while (rs.next()) 
            {           
            	uniqueKey += 1;
            	int genoClusterKey = rs.getInt("hdp_genocluster_key");
            	int gridClusterKey = clusterToClusterMap.containsKey(genoClusterKey) ? clusterToClusterMap.get(genoClusterKey) : 0;
            	String vocabName = getVocabName(rs.getString("annotation_type"));
            	String qualifier = rs.getString("qualifier_type");
            	if(qualifier==null) qualifier = "";
            	
            	SolrInputDocument doc = new SolrInputDocument();
            	doc.addField(DiseasePortalFields.UNIQUE_KEY,uniqueKey);
            	doc.addField(DiseasePortalFields.GRID_CLUSTER_KEY,gridClusterKey);
            	doc.addField(DiseasePortalFields.GENO_CLUSTER_KEY,genoClusterKey);
            	doc.addField(DiseasePortalFields.TERM_TYPE,rs.getString("term_type")); // either term or header
            	doc.addField(DiseasePortalFields.VOCAB_NAME,vocabName);
            	doc.addField(DiseasePortalFields.TERM,rs.getString("term"));
            	doc.addField(DiseasePortalFields.TERM_ID,rs.getString("term_id"));
            	doc.addField(DiseasePortalFields.TERM_QUALIFIER,qualifier);
            	
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
            
            logger.info("finished first set of data");
            
            // Now add the information needed for Human marker diseases

            logger.info("loading OMIM data for human markers");
            query = "(select ha.marker_key, " +
            		"ha.header, " +
            		"ha.vocab_name, " +
            		"ha.term, " +
            		"ha.term_id, " +
            		"ha.qualifier_type, " +
            		"gcm.hdp_gridcluster_key "+
            		"from hdp_annotation ha, " +
            		"hdp_gridcluster_marker gcm " +
            		"where ha.organism_key=2 " +
            		"and ha.marker_key=gcm.marker_key " +
            		"and vocab_name='OMIM') ";
            
            rs = ex.executeProto(query);
            
            docs = new ArrayList<SolrInputDocument>();
            logger.info("parsing OMIM data for human markers");
            
            // don't add duplicate headers for each marker
            Map<String,Set<String>> markerHeaderAnnotationIdMap = new HashMap<String,Set<String>>();
            
            String delim = "||";
            String delimRegex = "\\|\\|";
            
            while (rs.next()) 
            {           
            	if(rs.getString("term") == null || rs.getString("term").equals("")) continue;
            	String qualifier = rs.getString("qualifier_type");
            	if(qualifier==null) qualifier = "";
            	
            	uniqueKey += 1;
            	int markerKey = rs.getInt("marker_key");
            	int gridClusterKey = rs.getInt("hdp_gridcluster_key");
            	
            	String humanJoinKey = DiseasePortalIndexerSQL.makeHumanDiseaseKey(markerKey,rs.getString("term_id"));
            	
            	SolrInputDocument doc = new SolrInputDocument();
            	doc.addField(DiseasePortalFields.UNIQUE_KEY,uniqueKey);
            	doc.addField(DiseasePortalFields.GRID_CLUSTER_KEY,gridClusterKey);
            	doc.addField(DiseasePortalFields.MARKER_KEY,markerKey);
            	doc.addField(DiseasePortalFields.TERM_TYPE,"term");
            	doc.addField(DiseasePortalFields.VOCAB_NAME,rs.getString("vocab_name"));
            	doc.addField(DiseasePortalFields.TERM,rs.getString("term"));
            	doc.addField(DiseasePortalFields.TERM_ID,rs.getString("term_id"));
            	doc.addField(DiseasePortalFields.HUMAN_DISEASE_JOIN_KEY,humanJoinKey);
            	doc.addField(DiseasePortalFields.TERM_QUALIFIER,qualifier);

                docs.add(doc);
                
                // Now add a header if one is set
            	String header = rs.getString("header");
            	if(header != null && !header.equals(""))
            	{
            		String markerHeaderKey = gridClusterKey + delim + markerKey + delim + header + delim + qualifier;
            		if(!markerHeaderAnnotationIdMap.containsKey(markerHeaderKey))
            		{
            			markerHeaderAnnotationIdMap.put(markerHeaderKey,new HashSet<String>());
            		}
            		markerHeaderAnnotationIdMap.get(markerHeaderKey).add(humanJoinKey);
            	}
            	
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
            // add all the header documents for human->disease clumps
    		for(String markerHeaderKey : markerHeaderAnnotationIdMap.keySet())
    		{
        		String qualifier = "";
        		// parse the values out of our map key
        		String[] tokens = markerHeaderKey.split(delimRegex);
        		if(tokens.length<3) continue;
        		int gridClusterKey = Integer.parseInt(tokens[0]);
        		int markerKey = Integer.parseInt(tokens[1]);
        		String header = tokens[2];
        		if(tokens.length>3) qualifier = tokens[3];
        		
        		uniqueKey += 1;
        		SolrInputDocument doc = new SolrInputDocument();
            	doc.addField(DiseasePortalFields.UNIQUE_KEY,uniqueKey);
            	doc.addField(DiseasePortalFields.GRID_CLUSTER_KEY,gridClusterKey);
            	doc.addField(DiseasePortalFields.MARKER_KEY,markerKey);
            	doc.addField(DiseasePortalFields.TERM_TYPE,"header"); 
            	doc.addField(DiseasePortalFields.VOCAB_NAME,"OMIM");
            	doc.addField(DiseasePortalFields.TERM,header);
            	doc.addField(DiseasePortalFields.TERM_ID,header);
            	doc.addField(DiseasePortalFields.TERM_QUALIFIER,qualifier);

            	// add all the annotation termIds that we can join on
            	this.addAllFromLookup(doc,DiseasePortalFields.HUMAN_DISEASE_JOIN_KEY,markerHeaderKey,markerHeaderAnnotationIdMap);
            	
                docs.add(doc);
    		
    		}
    		
            if (! docs.isEmpty()) {
                server.add(docs);
            }
            server.commit();
    }
    
    private String getVocabName(String annotationTypeKey)
    {
    	List<String> mpTypes = Arrays.asList("1002");
    	List<String> omimTypes = Arrays.asList("1005","1006");
    	if(mpTypes.contains(annotationTypeKey)) return "Mammalian Phenotype";
    	if(omimTypes.contains(annotationTypeKey)) return "OMIM";
    	return "";
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
}
