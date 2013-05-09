package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * RefIndexerSQL
 * @author mhall
 * This class has the primary responsibility for populating the reference index.
 * It has a fairly large number of sub object relationships, but since its a 
 * fairly small dataset there is no real need to do actual chunking.
 * 
 * Note: Refactored during 5.x development
 */

public class MarkerIndexerSQL extends Indexer {

   
    public MarkerIndexerSQL () {
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
            
            ResultSet rs_tmp = ex.executeProto("select max(marker_key) as maxMarkerKey from marker");
            rs_tmp.next();
            
            logger.info("Max Marker Number: " + rs_tmp.getString("maxMarkerKey") + " Timing: "+ ex.getTiming());
            String start = "0";
            String end = rs_tmp.getString("maxMarkerKey");
            
            // Get all marker id -> marker relationships
            logger.info("Seleceting all Marker ID's -> marker");
            String markerToIDSQL = "select distinct marker_key, acc_id from marker_id where marker_key > " + start + " and marker_key <= "+ end + " and private = 0";
            logger.info(markerToIDSQL);
            HashMap <String, HashSet <String>> idToMarkers = makeHash(markerToIDSQL, "marker_key", "acc_id");

            // Get all marker -> reference relationships, by marker key
            
            logger.info("Seleceting all reference keys -> marker");
            String markerToReferenceSQL = "select distinct marker_key, reference_key from marker_to_reference where marker_key > " + start + " and marker_key <= "+ end;
            logger.info(markerToReferenceSQL);
            HashMap <String, HashSet <String>> referenceToMarkers = makeHash(markerToReferenceSQL, "marker_key", "reference_key");
                        
            // Get all marker -> vocab relationships, by marker key
            
            logger.info("Seleceting all vocab terms/ID's -> marker");
            String markerToTermSQL = "select distinct m.marker_key, a.term, a.annotation_type, a.term_id from marker_to_annotation m, annotation a where m.marker_key > " + start + " and m.marker_key <= "+ end + " and m.annotation_key = a.annotation_key";
            logger.info(markerToTermSQL);
            HashMap <String, HashSet <String>> termToMarkers = makeVocabHash(markerToTermSQL, "marker_key", "term");

            
            logger.info("Seleceting all vocab terms/ID's -> marker");
            String markerToTermIDSQL = "select distinct m.marker_key, a.term_id from marker_to_annotation m, annotation a where m.marker_key > " + start + " and m.marker_key <= "+ end + " and m.annotation_key = a.annotation_key";
            logger.info(markerToTermIDSQL);
            HashMap <String, HashSet <String>> termToMarkersID = makeHash(markerToTermIDSQL, "marker_key", "term_id");
            
            logger.info("Selecting all vocab term/IDs -> marker (excluding NOTs) for GXD");
            String markerToTermIDForGXDSQL = SharedQueries.GXD_VOCAB_QUERY +" and mta.marker_key > " + start + " and mta.marker_key <= "+ end + " and mta.annotation_key = a.annotation_key";
            logger.info(markerToTermIDForGXDSQL);
            HashMap <String, HashSet <String>> termToMarkersIDForGXD = makeHash(markerToTermIDForGXDSQL, "marker_key", "term_id");
            
            logger.info("Selecting all vocab ancestor term/IDs for GXD");
            String markerToTermIDAncestorsForGXDSQL = SharedQueries.GXD_VOCAB_ANCESTOR_QUERY;
            logger.info(markerToTermIDAncestorsForGXDSQL);
            HashMap <String, HashSet <String>> termAncestorsToMarkersIDForGXD = makeHash(markerToTermIDAncestorsForGXDSQL, "primary_id", "ancestor_primary_id");
            
            
            logger.info("Getting all markers");
            String markerSQL = "select distinct marker_key, primary_id marker_id,symbol," +
            		" name, marker_type, status, organism from marker" +
			" where organism = 'mouse'";
            logger.info(markerSQL);
            ResultSet rs_overall = ex.executeProto(markerSQL);
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the base query, adding its contents into solr
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                
                doc.addField(IndexConstants.MRK_KEY, rs_overall.getString("marker_key"));
                doc.addField(IndexConstants.MRK_SYMBOL, rs_overall.getString("symbol"));
                doc.addField(IndexConstants.MRK_NAME, rs_overall.getString("name"));
                doc.addField(IndexConstants.MRK_TYPE, rs_overall.getString("marker_type"));
                doc.addField(IndexConstants.MRK_STATUS, rs_overall.getString("status"));
                doc.addField(IndexConstants.MRK_ORGANISM, rs_overall.getString("organism"));
                String markerID = rs_overall.getString("marker_id");
                if (markerID==null) markerID = "";
                doc.addField(IndexConstants.MRK_PRIMARY_ID,markerID);
                
                // Parse the 1->N marker relationship here, adding in the marker keys
                
                if (referenceToMarkers.containsKey(rs_overall.getString("marker_key"))) {
                    for (String key: referenceToMarkers.get(rs_overall.getString("marker_key"))) {
                        doc.addField(IndexConstants.REF_KEY, key);
                    }
                }
                
                if (termToMarkers.containsKey(rs_overall.getString("marker_key"))) {
                    for (String markerKey: termToMarkers.get(rs_overall.getString("marker_key"))) {
                        doc.addField(IndexConstants.MRK_TERM, markerKey);
                    }
                }
                
                if (idToMarkers.containsKey(rs_overall.getString("marker_key"))) {
                    for (String id: idToMarkers.get(rs_overall.getString("marker_key"))) {
                        doc.addField(IndexConstants.MRK_ID, id);
                    }
                }

                
                if (termToMarkersID.containsKey(rs_overall.getString("marker_key"))) {
                    for (String markerKey: termToMarkersID.get(rs_overall.getString("marker_key"))) {
                        doc.addField(IndexConstants.MRK_TERM_ID, markerKey);
                    }
                }
                
                if (termToMarkersIDForGXD.containsKey(rs_overall.getString("marker_key"))) {
                    for (String termID: termToMarkersIDForGXD.get(rs_overall.getString("marker_key"))) {
                        doc.addField(IndexConstants.MRK_TERM_ID_FOR_GXD, termID);
                        if(termAncestorsToMarkersIDForGXD.containsKey(termID))
                        {
                        	for(String ancestorID : termAncestorsToMarkersIDForGXD.get(termID))
                        	{
                        		doc.addField(IndexConstants.MRK_TERM_ID_FOR_GXD, ancestorID);
                        	}
                        }
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
            server.add(docs);
            server.commit();
    }
    
    protected HashMap <String, HashSet <String>> makeVocabHash(String sql, String keyString, String valueString) throws Exception
    {
        
        HashMap <String, HashSet <String>> tempMap = new HashMap <String, HashSet <String>> ();
        
            ResultSet rs = ex.executeProto(sql);         

            String key = null;
            String value = null;
            String vocab = null;
            String vocabID = null;
            
            while (rs.next()) {
                key = rs.getString(keyString);
                value = rs.getString(valueString);
                vocab = rs.getString("annotation_type");
                vocabID = rs.getString("term_id");
                if (tempMap.containsKey(key)) {
                    tempMap.get(key).add(translateVocab(value, vocab));
/*                    System.out.println(value);
                    System.out.println(vocab);
                    System.out.println(translateVocab(value, vocab));
                    tempMap.get(key).add(vocabID);
                    System.out.println(vocabID);*/
                }
                else {
                    HashSet <String> temp = new HashSet <String> ();
                    temp.add(translateVocab(value, vocab));
                    tempMap.put(key, temp);
/*                    System.out.println(value);
                    System.out.println(vocab);
                    System.out.println(translateVocab(value, vocab));
                    tempMap.get(key).add(vocabID);
                    System.out.println(vocabID);*/
                }
            }
        return tempMap;
        
    }
    
    protected String translateVocab(String value, String vocab) {
        if (vocab.equals("GO/Marker")) {
            return "Function: " + value;
        }
        if (vocab.equals("InterPro/Marker")) {
            return "Protein Domain: " + value;
        }
        if (vocab.equals("PIRSF/Marker")) {
            return "Protein Family: " + value;
        }
        if (vocab.equals("OMIM/Human Marker")) {
            return "Disease Model: " + value;
        }
        return "";
    }
}
