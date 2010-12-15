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

public class RefIndexerSQL extends Indexer {

   
   
    public RefIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * The main method, called from the command line in order to start the indexing.
     * @param args
     */
    public static void main(String[] args) {

        RefIndexerSQL ri = new RefIndexerSQL("index.url.reference");
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
            
            ResultSet rs_tmp = ex.executeProto("select max(reference_Key) as maxRefKey from reference");
            rs_tmp.next();
            
            logger.info("Max Ref Number: " + rs_tmp.getString("maxRefKey") + " Timing: "+ ex.getTiming());
            String start = "0";
            String end = rs_tmp.getString("maxRefKey");
               
            // Get all reference -> marker relationships, by marker key
            
            logger.info("Seleceting all reference -> marker");
            String markerToRefSQL = "select reference_key, marker_key from marker_to_reference where reference_key > " + start + " and reference_key <= "+ end;
            logger.info(markerToRefSQL);
            HashMap <String, HashSet <String>> refToMarkers = makeHash(markerToRefSQL, "reference_key", "marker_key");

            // Get all reference -> allele relationships, by allele key
            
            logger.info("Seleceting all reference -> allele");
            String alleleToRefSQL = "select reference_key, allele_key from allele_to_reference where reference_key > " + start + " and reference_key <= "+ end;
            logger.info(alleleToRefSQL);
            HashMap <String, HashSet <String>> refToAlleles = makeHash(alleleToRefSQL, "reference_key", "allele_key");
            
            // Get all reference -> sequence relationships, by sequence key
            
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
            logger.info("Seleceting all reference -> first author");
            String referenceAuthorFirstSQL = "select reference_key, author from reference_individual_authors where sequence_num = 1 and reference_key > " + start + " and reference_key <= "+ end;
            logger.info(referenceAuthorFirstSQL);
            HashMap <String, HashSet <String>> refAuthorsFirst = makeHash(referenceAuthorFirstSQL,"reference_key","author");
            
            // The main reference query.
            
            logger.info("Getting all references");
            String referenceSQL = "select r.reference_key, r.year, r.jnum_id, r.pubmed_id, r.authors, r.title," +
                " r.journal, r.vol, r.issue, ra.abstract," +
                " rc.marker_count, rc.probe_count, rc.mapping_expt_count, rc.gxd_index_count, rc.gxd_result_count," +
                " rc.gxd_structure_count, rc.gxd_assay_count, rc.allele_count, rc.sequence_count " +
                "from reference as r " +
                "inner join reference_abstract ra on r.reference_key = ra.reference_key inner join reference_counts as rc on r.reference_key = rc.reference_key";
            logger.info(referenceSQL);
            ResultSet rs_overall = ex.executeProto(referenceSQL);
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the base query, adding its contents into solr
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.REF_AUTHOR, rs_overall.getString("authors"));
                doc.addField(IndexConstants.JNUM_ID, rs_overall.getString("jnum_id"));
                String jnumID [] = rs_overall.getString("jnum_id").split(":");
                doc.addField(IndexConstants.JNUM_ID, jnumID[1]);
                doc.addField(IndexConstants.PUBMED_ID, rs_overall.getString("pubMed_id"));
                doc.addField(IndexConstants.REF_JOURNAL, rs_overall.getString("journal"));
                doc.addField(IndexConstants.REF_JOURNAL_FACET, rs_overall.getString("journal"));
                doc.addField(IndexConstants.REF_KEY, rs_overall.getString("reference_key"));
                doc.addField(IndexConstants.REF_TITLE, rs_overall.getString("title"));
                
                // Temporary new way to put in the title
                
                // This string is used for the overall smushed title and abstract field.
                String titleAndAbstract = "";
                
                if (rs_overall.getString("title") != null) {
                    String tempTitle = rs_overall.getString("title").replaceAll("\\p{Punct}", " ");
                    
                    doc.addField(IndexConstants.REF_TITLE_STEMMED, tempTitle);
                    doc.addField(IndexConstants.REF_TITLE_UNSTEMMED, tempTitle);
                    titleAndAbstract = tempTitle;
                }
                
                doc.addField(IndexConstants.REF_YEAR, rs_overall.getString("year"));
                doc.addField(IndexConstants.REF_ABSTRACT, rs_overall.getString("abstract"));
                 
                
                if (rs_overall.getString("abstract") != null) {                
                    String tempAbstract = rs_overall.getString("abstract").replaceAll("\\p{Punct}", " ");
    
                    doc.addField(IndexConstants.REF_ABSTRACT_STEMMED, tempAbstract);
                    doc.addField(IndexConstants.REF_ABSTRACT_UNSTEMMED, tempAbstract);
                
                    // Put together the second part of the smushed title and abstract
                    
                    if (titleAndAbstract.equals("")) {
                        titleAndAbstract = tempAbstract;
                    } else {
                        titleAndAbstract = titleAndAbstract + " WORDTHATCANTEXIST " + tempAbstract;
                    }
                    
                }
                
                // Add the smushed title and abstract into the document
                
                doc.addField(IndexConstants.REF_TITLE_ABSTRACT_STEMMED, titleAndAbstract);
                doc.addField(IndexConstants.REF_TITLE_ABSTRACT_UNSTEMMED, titleAndAbstract);
                                
                doc.addField(IndexConstants.REF_ISSUE, rs_overall.getString("issue"));
                doc.addField(IndexConstants.REF_VOLUME, rs_overall.getString("vol"));
                
                doc.addField(IndexConstants.MRK_COUNT, convertCount(rs_overall.getInt("marker_count")));
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
                }
                // Count of orthologs isn't implemented yet.
                doc.addField(IndexConstants.ORTHO_COUNT, 0);
                
                // Add in the 1->n marker relationships
                
                if (refToMarkers.containsKey(rs_overall.getString("reference_key"))) {
                    for (String markerKey: refToMarkers.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.MRK_KEY, markerKey);
                    }
                }

                // Add in the 1->n allele relationships
                
                if (refToAlleles.containsKey(rs_overall.getString("reference_key"))) {
                    for (String alleleKey: refToAlleles.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.ALL_KEY, alleleKey);
                    }
                }
                
                // Add in the 1->n sequence relationships
                
                if (refToSequences.containsKey(rs_overall.getString("reference_key"))) {
                    for (String sequenceKey: refToSequences.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.SEQ_KEY, sequenceKey);
                    }
                }
                
                // Add in all of the indivudual authors, specifically formatted for 
                // searching.
                
                // In a nutshell we split on whitespace, and then add in each resulting
                // token into the database, as well as the entirety of the author string.
                
                if (refAuthors.containsKey(rs_overall.getString("reference_key"))) {
                    for (String author: refAuthors.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.REF_AUTHOR_FORMATTED, author);
                        
                        // Add in a single untouched version of the formatted authors
                        doc.addField(IndexConstants.REF_AUTHOR_FACET, author);
                        
                        if (author != null) {
                        String [] temp = author.split("[\\W-&&[^']]");
                        
                        // Add all possible permutations of this author into the index.
                        if (temp.length > 1) {
                            String tempString = "";
                            for (int i = 0; i< temp.length && i <= 3; i++) {
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
                }

                // Add all possible prefixes for the first author only
                
                if (refAuthorsFirst.containsKey(rs_overall.getString("reference_key"))) {
                    for (String author: refAuthorsFirst.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.REF_FIRST_AUTHOR, author);
                        
                        if (author != null) {
                            String [] temp = author.split("[\\W-&&[^']]");
                            
                            // Add all possible permutations of this author into the index.
                            if (temp.length > 1) {
                                String tempString = "";
                                for (int i = 0; i< temp.length && i <= 3; i++) {
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
                }
                
                // Add all possible prefixes for the last author only
                
                if (refAuthorsLast.containsKey(rs_overall.getString("reference_key"))) {
                    for (String author: refAuthorsLast.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.REF_LAST_AUTHOR, author);
                        
                        if (author != null) {
                            String [] temp = author.split("[\\W-&&[^']]");
                            
                            // Add all possible permutations of this author into the index.
                            if (temp.length > 1) {
                                String tempString = "";
                                for (int i = 0; i< temp.length && i <= 3; i++) {
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
            
        } catch (Exception e) {
            logger.error("In the exception part.");
            e.printStackTrace();}
    }
}
