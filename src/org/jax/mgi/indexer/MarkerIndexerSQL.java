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
 */

public class MarkerIndexerSQL extends Indexer {

   
   
    public MarkerIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * The main method, called from the command line in order to start the indexing.
     * @param args
     */
    public static void main(String[] args) {

        MarkerIndexerSQL ri = new MarkerIndexerSQL("index.url.marker");
        ri.doChunks();
         
    }
    
    /**
     * The main worker of this class, it starts by gathering up all the 1->N 
     * subobject relationships that a reference can have, and then runs the 
     * main query.  We then enter a parsing phase where we place the data into
     * solr documents, and put them into the index.
     */
    
    private void doChunks() {
                
        try {
            
            // How many references are there total?
            
            ResultSet rs_tmp = ex.executeProto("select max(marker_key) as maxMarkerKey from marker");
            rs_tmp.next();
            
            logger.info("Max Marker Number: " + rs_tmp.getString("maxMarkerKey") + " Timing: "+ ex.getTiming());
            String start = "0";
            String end = rs_tmp.getString("maxMarkerKey");
               
            // Get all reference -> marker relationships, by marker key
            
            logger.info("Seleceting all vocab terms/ID's -> marker");
            String markerToTermSQL = "select distinct marker_key, term, annotation_type, term_id from marker_annotation where marker_key > " + start + " and marker_key <= "+ end;
            logger.info(markerToTermSQL);
            HashMap <String, HashSet <String>> termToMarkers = makeVocabHash(markerToTermSQL, "marker_key", "term");

            
            logger.info("Seleceting all vocab terms/ID's -> marker");
            String markerToTermIDSQL = "select distinct marker_key, term_id from marker_annotation where marker_key > " + start + " and marker_key <= "+ end;
            logger.info(markerToTermIDSQL);
            HashMap <String, HashSet <String>> termToMarkersID = makeHash(markerToTermSQL, "marker_key", "term_id");
            /*// Get all reference -> sequence relationships, by sequence key
            
            logger.info("Seleceting all reference -> sequence associations");
            String referenceToSeqSQL = "select reference_key, sequence_key from reference_to_sequence where reference_key > " + start + " and reference_key <= "+ end;
            logger.info(referenceToSeqSQL);
            HashMap <String, HashSet <String>> refToSequences = makeHash(referenceToSeqSQL,"reference_key","sequence_key");
            
            // Author information, for the formatted authors.
            logger.info("Seleceting all reference -> sequence associations");
            String referenceAuthorSQL = "select reference_key, author from reference_individual_authors where reference_key > " + start + " and reference_key <= "+ end;
            logger.info(referenceAuthorSQL);
            HashMap <String, HashSet <String>> refAuthors = makeHash(referenceAuthorSQL,"reference_key","author");

            // last Author information, for the formatted authors.
            logger.info("Seleceting all reference -> sequence associations");
            String referenceAuthorLastSQL = "select reference_key, author from reference_individual_authors where is_last = 1 and reference_key > " + start + " and reference_key <= "+ end;
            logger.info(referenceAuthorLastSQL);
            HashMap <String, HashSet <String>> refAuthorsLast = makeHash(referenceAuthorLastSQL,"reference_key","author");
            
            // first Author information, for the formatted authors.
            logger.info("Seleceting all reference -> sequence associations");
            String referenceAuthorFirstSQL = "select reference_key, author from reference_individual_authors where sequence_num = 1 and reference_key > " + start + " and reference_key <= "+ end;
            logger.info(referenceAuthorFirstSQL);
            HashMap <String, HashSet <String>> refAuthorsFirst = makeHash(referenceAuthorFirstSQL,"reference_key","author");
            */
            // The main reference query.
            
            logger.info("Getting all markers");
            String markerSQL = "select distinct marker_key, symbol," +
            		" name, marker_type, status, organism from marker";
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
                
                // Parse the 1->N marker relationship here, adding in the marker keys
                
                if (termToMarkers.containsKey(rs_overall.getString("marker_key"))) {
                    for (String markerKey: termToMarkers.get(rs_overall.getString("marker_key"))) {
                        doc.addField(IndexConstants.MRK_TERM, markerKey);
                    }
                }
                
                if (termToMarkersID.containsKey(rs_overall.getString("marker_key"))) {
                    for (String markerKey: termToMarkersID.get(rs_overall.getString("marker_key"))) {
                        doc.addField(IndexConstants.MRK_TERM_ID, markerKey);
                    }
                }
/*                doc.addField(IndexConstants.MRK_COUNT, convertCount(rs_overall.getInt("marker_count")));
                if (convertCount(rs_overall.getInt("marker_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Genome features");
                }
                doc.addField(IndexConstants.PRB_COUNT, convertCount(rs_overall.getInt("probe_count")));
                if (convertCount(rs_overall.getInt("probe_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Molecular probes and clones");
                }
                
                doc.addField(IndexConstants.MAP_EXPT_COUNT, convertCount(rs_overall.getInt("mapping_expt_count")));
                if (convertCount(rs_overall.getInt("mapping_expt_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Mapping data");
                }
                
                doc.addField(IndexConstants.GXD_INDEX_COUNT, convertCount(rs_overall.getInt("gxd_index_count")));
                if (convertCount(rs_overall.getInt("gxd_index_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Gene expression literature content records");
                }
                
                doc.addField(IndexConstants.GXD_RESULT_COUNT, convertCount(rs_overall.getInt("gxd_result_count")));
                if (convertCount(rs_overall.getInt("gxd_result_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Expression: assays, results, tissues");
                }

                doc.addField(IndexConstants.GXD_STRUCT_COUNT, convertCount(rs_overall.getInt("gxd_structure_count")));
                if (convertCount(rs_overall.getInt("gxd_result_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Expression: assays, results, tissues");
                }
                
                doc.addField(IndexConstants.GXD_ASSAY_COUNT, convertCount(rs_overall.getInt("gxd_assay_count")));
                if (convertCount(rs_overall.getInt("gxd_assay_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Expression: assays, results, tissues");
                }

                doc.addField(IndexConstants.ALL_COUNT, convertCount(rs_overall.getInt("allele_count")));
                if (convertCount(rs_overall.getInt("allele_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Phenotypic alleles");
                }

                doc.addField(IndexConstants.SEQ_COUNT, convertCount(rs_overall.getInt("sequence_count")));
                if (convertCount(rs_overall.getInt("sequence_count")) > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Sequences");
                }*/
                // Count of orthologs isn't implemented yet.
/*                doc.addField(IndexConstants.ORTHO_COUNT, 0);
                
                // Add in the 1->n marker relationships
                
                if (refToMarkers.containsKey(rs_overall.getString("reference_key"))) {
                    for (String markerKey: refToMarkers.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.MRK_KEY, markerKey);
                    }
                }
                
                // Add in the 1->n sequence relationships
                
                if (refToSequences.containsKey(rs_overall.getString("reference_key"))) {
                    for (String sequenceKey: refToSequences.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.SEQ_KEY, sequenceKey);
                    }
                }*/
                
                // Add in all of the indivudual authors, specifically formatted for 
                // searching.
                
                // In a nutshell we split on whitespace, and then add in each resulting
                // token into the database, as well as the entirety of the author string.
                
/*                if (refAuthors.containsKey(rs_overall.getString("reference_key"))) {
                    for (String author: refAuthors.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.REF_AUTHOR_FORMATTED, author);
                        
                        // Add in a single untouched version of the formatted authors
                        doc.addField(IndexConstants.REF_AUTHOR_FACET, author);
                        
                        if (author != null) {
                        String [] temp = author.split("[\\W-&&[^']]");
                        
                        // Add all possible permutations of this author into the index.
                        if (temp.length > 1) {
                            String tempString = "";
                            for (int i = 0; i< temp.length; i++) {
                                    if (i == 0) {
                                        tempString = temp[i];
                                    }
                                    else {
                                        tempString = tempString + " " + temp[i];
                                    }
                                    doc.addField(IndexConstants.REF_AUTHOR_FORMATTED, tempString);
                                }
                            }
                        }
                    }
                }*/

                // Add all possible prefixes for the first author only
                
/*                if (refAuthorsFirst.containsKey(rs_overall.getString("reference_key"))) {
                    for (String author: refAuthorsFirst.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.REF_FIRST_AUTHOR, author);
                        
                        if (author != null) {
                            String [] temp = author.split(" ");
                            
                            // Add all possible permutations of this author into the index.
                            if (temp.length > 1) {
                                String tempString = "";
                                for (int i = 0; i< temp.length -1; i++) {
                                        if (i == 0) {
                                            tempString = temp[i];
                                        }
                                        else {
                                            tempString = tempString + " " + temp[i];
                                        }
                                        doc.addField(IndexConstants.REF_FIRST_AUTHOR, tempString);
                                    }
                                }
                            }
                    }
                }*/
                
                // Add all possible prefixes for the last author only
                
/*                if (refAuthorsLast.containsKey(rs_overall.getString("reference_key"))) {
                    for (String author: refAuthorsLast.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.REF_LAST_AUTHOR, author);
                        
                        if (author != null) {
                            String [] temp = author.split(" ");
                            
                            // Add all possible permutations of this author into the index.
                            if (temp.length > 1) {
                                String tempString = "";
                                for (int i = 0; i< temp.length -1; i++) {
                                        if (i == 0) {
                                            tempString = temp[i];
                                        }
                                        else {
                                            tempString = tempString + " " + temp[i];
                                        }
                                        doc.addField(IndexConstants.REF_LAST_AUTHOR, tempString);
                                    }
                                }
                            }
                    }
                }*/
                
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
            
        } catch (Exception e) {
            logger.error("In the exception part.");
            e.printStackTrace();}
    }
    
    protected HashMap <String, HashSet <String>> makeVocabHash(String sql, String keyString, String valueString) {
        
        HashMap <String, HashSet <String>> tempMap = new HashMap <String, HashSet <String>> ();
        
        try {
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
        } catch (Exception e) {e.printStackTrace();}
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
