package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

public class RefIndexerSQL extends Indexer {

   
   
    public RefIndexerSQL () {
        super("index.url.reference");
    }
    
    
    /**
     * The main worker of this class, it starts by gathering up all the 1->N 
     * subobject relationships that a reference can have, and then runs the 
     * main query.  We then enter a parsing phase where we place the data into
     * solr documents, and put them into the index.
     */
    public void index() throws Exception
    {
    	String diseaseRelevantMarkerQuery = "select mtr.reference_key, m.primary_id marker_id " +
    			"from hdp_marker_to_reference mtr,marker m " +
    			"where m.marker_key=mtr.marker_key ";
    	Map<String,Set<String>> diseaseRelevantMarkerMap = populateLookup(diseaseRelevantMarkerQuery,"reference_key","marker_id",
    				"disease relevant marker IDs (for linking from disease portal)");
    	
    	String diseaseRelevantRefQuery = "select trt.reference_key, ha.term_id disease_id " +
    			"from hdp_term_to_reference trt,hdp_annotation ha " +
    			"where ha.term_key=trt.term_key " +
    			"and ha.vocab_name='OMIM' ";
    	Map<String,Set<String>> diseaseRelevantRefMap = populateLookup(diseaseRelevantRefQuery,"reference_key","disease_id",
    				"disease IDs to references (for linking from disease portal)");
    	
	// populate lookup from reference key to set of marker IDs which have
	// GO annotations from that reference
	String goMarkerSQL = "select distinct m.primary_id, r.reference_key "
		+ "from marker_to_annotation mta, "
		+ "    annotation a, "
		+ "    annotation_reference r, "
		+ "    marker m "
		+ "where mta.annotation_key = a.annotation_key "
		+ "    and a.annotation_key = r.annotation_key "
		+ "    and mta.marker_key = m.marker_key "
		+ "    and a.evidence_code != 'ND' "
		+ "    and m.organism = 'mouse' "
		+ "    and a.vocab_name = 'GO' ";		
	Map<String, Set<String>> goMarkerMap = populateLookup(goMarkerSQL,
		"reference_key", "primary_id", "GO/Marker annotations");

	// populate lookup from reference key to set of marker IDs which have
	// alleles associated with that reference
	String phenoMarkerSQL = "with pairs as ("
		+ "select marker_key, allele_key "
		+ "from marker_to_allele "
		+ "union "
		+ "select related_marker_key, allele_key "
		+ "from allele_related_marker "
		+ "where relationship_category in "
		+ "  ('mutation_involves', 'expresses_component')"
		+ ") "
		+ "select distinct m.primary_id, r.reference_key "
		+ "from marker m, pairs mta, allele a, "
		+ "  allele_to_reference atr, reference r "
		+ "where m.marker_key = mta.marker_key "
		+ "  and mta.allele_key = a.allele_key "
		+ "  and a.is_wild_type = 0 "
		+ "  and a.allele_key = atr.allele_key "
		+ "  and atr.reference_key = r.reference_key ";
	Map<String,Set<String>> phenoMarkerMap = populateLookup(phenoMarkerSQL,
		"reference_key", "primary_id", "MP/Marker associations");

            // How many references are there total?
            ResultSet rs_tmp = ex.executeProto("select max(reference_Key) as maxRefKey from reference");
            rs_tmp.next();
            
            logger.info("Max Ref Number: " + rs_tmp.getString("maxRefKey") + " Timing: "+ ex.getTiming());
            String start = "0";
            String end = rs_tmp.getString("maxRefKey");
               
            // Get all reference -> marker relationships, by marker key
            
            logger.info("Selecting all reference -> marker");
            String markerToRefSQL = "select reference_key, marker_key from marker_to_reference where reference_key > " + start + " and reference_key <= "+ end;
            logger.info(markerToRefSQL);
            HashMap <String, HashSet <String>> refToMarkers = makeHash(markerToRefSQL, "reference_key", "marker_key");

            // Get all reference -> book publisher relationships, by publisher
            
            logger.info("Selecting all reference -> publisher");
            String pubToRefSQL = "select reference_key, publisher from reference_book where reference_key > " + start + " and reference_key <= "+ end;
            logger.info(pubToRefSQL);
            HashMap <String, HashSet <String>> pubToRefs = makeHash(pubToRefSQL, "reference_key", "publisher");            
            
            // Get all reference -> allele relationships, by allele key
            
            logger.info("Selecting all reference -> allele");
            String alleleToRefSQL = "select reference_key, allele_key from allele_to_reference where reference_key > " + start + " and reference_key <= "+ end;
            logger.info(alleleToRefSQL);
            HashMap <String, HashSet <String>> refToAlleles = makeHash(alleleToRefSQL, "reference_key", "allele_key");
            
            // Get all reference -> sequence relationships, by sequence key
            
//            logger.info("Selecting all reference -> sequence associations");
//            String referenceToSeqSQL = "select reference_key, sequence_key from reference_to_sequence where reference_key > " + start + " and reference_key <= "+ end;
//            logger.info(referenceToSeqSQL);
//            //HashMap <String, HashSet <String>> refToSequences = makeHash(referenceToSeqSQL,"reference_key","sequence_key");
            
            // Author information, for the formatted authors.
            logger.info("Selecting all reference -> authors");
            String referenceAuthorSQL = "select reference_key, author from reference_individual_authors where reference_key > " + start + " and reference_key <= "+ end;
            logger.info(referenceAuthorSQL);
            HashMap <String, HashSet <String>> refAuthors = makeHash(referenceAuthorSQL,"reference_key","author");

            // last Author information, for the formatted authors.
            logger.info("Selecting all reference -> author last names");
            String referenceAuthorLastSQL = "select reference_key, author from reference_individual_authors where is_last = 1 and reference_key > " + start + " and reference_key <= "+ end;
            logger.info(referenceAuthorLastSQL);
            HashMap <String, HashSet <String>> refAuthorsLast = makeHash(referenceAuthorLastSQL,"reference_key","author");
            
            // first Author information, for the formatted authors.
            logger.info("Selecting all reference -> first author");
            String referenceAuthorFirstSQL = "select reference_key, author from reference_individual_authors where sequence_num = 1 and reference_key > " + start + " and reference_key <= "+ end;
            logger.info(referenceAuthorFirstSQL);
            HashMap <String, HashSet <String>> refAuthorsFirst = makeHash(referenceAuthorFirstSQL,"reference_key","author");
            
	    // MGI IDs (not J: numbers) for each reference
	    logger.info("Selecting references -> IDs");
	    String referenceIDsSQL = "select reference_key, acc_id from reference_id where reference_key > " + start + " and reference_key <= " + end;
	    logger.info (referenceIDsSQL);
	    HashMap<String, HashSet<String>> refIDs = makeHash(referenceIDsSQL, "reference_key", "acc_id");

            // The main reference query.
            
            logger.info("Getting all references");
            String referenceSQL = "select r.reference_key, r.year, r.jnum_id, r.pubmed_id, r.authors, r.title," +
                " r.journal, r.vol, r.issue, ra.abstract," +
                " rc.marker_count, rc.probe_count, rc.mapping_expt_count, rc.gxd_index_count, rc.gxd_result_count," +
                " rc.gxd_structure_count, rc.gxd_assay_count, rc.allele_count, rc.sequence_count, rc.go_annotation_count, r.reference_group " +
                "from reference as r " +
                "inner join reference_abstract ra on r.reference_key = ra.reference_key inner join reference_counts as rc on r.reference_key = rc.reference_key";
            logger.info(referenceSQL);
            ResultSet rs_overall = ex.executeProto(referenceSQL);
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            // Parse the base query, adding its contents into solr
            
            logger.info("Parsing them");
            int count=0;
            while (!rs_overall.isAfterLast()) {
            	count++;
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.REF_AUTHOR, rs_overall.getString("authors"));
                String jnumID [] = rs_overall.getString("jnum_id").split(":");
                doc.addField(IndexConstants.REF_ID, jnumID[1]);
                
                doc.addField(IndexConstants.REF_JOURNAL, rs_overall.getString("journal"));
                doc.addField(IndexConstants.REF_JOURNAL_FACET, rs_overall.getString("journal"));
                doc.addField(IndexConstants.REF_GROUPING, rs_overall.getString("reference_group"));
                
                String refKey = rs_overall.getString("reference_key");
                doc.addField(IndexConstants.REF_KEY, refKey);
                
                // add all the marker IDs for disease relevant markers
                addAllFromLookup(doc,IndexConstants.REF_DISEASE_RELEVANT_MARKER_ID,refKey,diseaseRelevantMarkerMap);
                addAllFromLookup(doc,IndexConstants.REF_DISEASE_ID,refKey,diseaseRelevantRefMap);

		// add all marker IDs where this reference has GO data
		addAllFromLookup(doc, IndexConstants.REF_GO_MARKER_ID, refKey,
		    goMarkerMap); 
                
		// add all marker IDs where this reference is associated with
		// alleles of the marker
		addAllFromLookup(doc, IndexConstants.REF_PHENO_MARKER_ID, refKey,
		    phenoMarkerMap); 
                
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
                
                Boolean foundACount = Boolean.FALSE;
                
                // good samaritan: The following 50ish lines of code here hurt me on such an emotional level that I could not let it go unchanged.
                // I will leave this code as an example of the madness.
//                doc.addField(IndexConstants.MRK_COUNT, convertCount(rs_overall.getInt("marker_count")));
//                if (convertCount(rs_overall.getInt("marker_count")) > 0) {
//                    doc.addField(IndexConstants.REF_HAS_DATA, "Genome features");
//                    foundACount = Boolean.TRUE;
//                }
                int marker_count = rs_overall.getInt("marker_count");
                // It is pretty questionable why we would convert a count to 1 or 0, but it's not my code and I don't want to break anything.
                // so I'm leaving this little gem here.
	            doc.addField(IndexConstants.MRK_COUNT, marker_count>0 ? 1 : 0);
	            if (marker_count > 0) {
	                doc.addField(IndexConstants.REF_HAS_DATA, "Genome features");
	                foundACount = Boolean.TRUE;
	            }
	            int probe_count = rs_overall.getInt("probe_count");
                doc.addField(IndexConstants.PRB_COUNT, probe_count>0 ? 1 : 0);
                if (probe_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Molecular probes and clones");
                    foundACount = Boolean.TRUE;
                }
                
                int map_expt_count = rs_overall.getInt("mapping_expt_count");
                doc.addField(IndexConstants.MAP_EXPT_COUNT, map_expt_count>0 ? 1 : 0);
                if (map_expt_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Mapping data");
                    foundACount = Boolean.TRUE;
                }
                int gxd_index_count = rs_overall.getInt("gxd_index_count");
                doc.addField(IndexConstants.GXD_INDEX_COUNT, gxd_index_count>0 ? 1 : 0);
                if (gxd_index_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Expression literature records");
                    foundACount = Boolean.TRUE;
                }
                
                int gxd_result_count = rs_overall.getInt("gxd_result_count");
                doc.addField(IndexConstants.GXD_RESULT_COUNT, gxd_result_count>0 ? 1 : 0);
                if (gxd_result_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Expression: assays results");
                    foundACount = Boolean.TRUE;
                }
                
                int gxd_structure_count = rs_overall.getInt("gxd_structure_count");
                doc.addField(IndexConstants.GXD_STRUCT_COUNT, gxd_structure_count>0 ? 1 : 0);
                if (gxd_structure_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Expression: assays results");
                    foundACount = Boolean.TRUE;
                }
                
                int gxd_assay_count = rs_overall.getInt("gxd_assay_count");
                doc.addField(IndexConstants.GXD_ASSAY_COUNT, gxd_assay_count>0 ? 1 : 0);
                if (gxd_assay_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Expression: assays results");
                    foundACount = Boolean.TRUE;
                }

                int allele_count = rs_overall.getInt("allele_count");
                doc.addField(IndexConstants.ALL_COUNT, allele_count>0 ? 1 : 0);
                if (allele_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Phenotypic alleles");
                    foundACount = Boolean.TRUE;
                }

                int sequence_count = rs_overall.getInt("sequence_count");
                doc.addField(IndexConstants.SEQ_COUNT, sequence_count>0 ? 1 : 0);
                if (sequence_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Sequences");
                    foundACount = Boolean.TRUE;
                }
                
                int go_annot_count = rs_overall.getInt("go_annotation_count");
                doc.addField(IndexConstants.GO_ANNOT_COUNT, go_annot_count>0 ? 1 : 0);
                if (go_annot_count > 0) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "Functional annotations (GO)");
                    foundACount = Boolean.TRUE;
                }
                
                if (!foundACount) {
                    doc.addField(IndexConstants.REF_HAS_DATA, "No curated data");
                }
                // Count of orthologs isn't implemented yet.
                doc.addField(IndexConstants.ORTHO_COUNT, 0);
                
                // Add in the 1->n marker relationships
                
                if (refToMarkers.containsKey(rs_overall.getString("reference_key"))) {
                    for (String markerKey: refToMarkers.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.MRK_KEY, markerKey);
                    }
                }
                
                // Add in the 1->1 publisher relationships, this only applies to books.
                
                if (pubToRefs.containsKey(rs_overall.getString("reference_key"))) {
                    for (String publisher: pubToRefs.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.REF_JOURNAL_FACET, publisher);
                    }
                }                

                // Add in the 1->n allele relationships
                
                if (refToAlleles.containsKey(rs_overall.getString("reference_key"))) {
                    for (String alleleKey: refToAlleles.get(rs_overall.getString("reference_key"))) {
                        doc.addField(IndexConstants.ALL_KEY, alleleKey);
                    }
                }
                
                // Add in the 1->n sequence relationships
                
//                if (refToSequences.containsKey(rs_overall.getString("reference_key"))) {
//                    for (String sequenceKey: refToSequences.get(rs_overall.getString("reference_key"))) {
//                        doc.addField(IndexConstants.SEQ_KEY, sequenceKey);
//                    }
//                }
                
		// add in the MGI ID(s) for each reference (not J: numbers)

		if (refIDs.containsKey(rs_overall.getString("reference_key"))) {
			for (String refID: refIDs.get(rs_overall.getString("reference_key"))) {
				doc.addField(IndexConstants.REF_ID, refID);
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
                        if (author == null || author.equals(" ")) {
                            doc.addField(IndexConstants.REF_AUTHOR_FACET, "No author listed");
                        }
                        else {
                            doc.addField(IndexConstants.REF_AUTHOR_FACET, author);
                        }
                        
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
                
                if (docs.size() > 1000) {
                    //logger.info("Adding a stack of the documents to Solr");
                    writeDocs(docs);
                    docs = new ArrayList<SolrInputDocument>();
                    //logger.info("Done adding to solr, Moving on");
                }
                if(count % 20000 == 0)
                {
                	// commit regularly to keep Solr from blowing out of memory
                	logger.info("committing docs "+(count-20000)+" through "+count);
                	server.commit();
                }
            }
	    if (docs.size() > 0) {
            	server.add(docs);
	    }
            server.commit();
    }
    
    // Convert the counts from actual values to something like a bit.
    // anonymous: WTF is this for anyway?
//    protected Integer convertCount(Integer count) {
//        if (count > 0) {
//            return 1;
//        }
//        else {
//            return 0; 
//        }
//    }
}
