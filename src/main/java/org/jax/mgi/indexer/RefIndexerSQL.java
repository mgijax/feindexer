package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * RefIndexerSQL
 * This class has the primary responsibility for populating the reference index.
 */
public class RefIndexerSQL extends Indexer {
	public RefIndexerSQL () {
		super("reference");
	}

	// pre-compute some data in temp tables to aid efficiency
	public void createTempTables() {
		// closure table for DO terms and their ancestors
		
		String cmd0 = "select ha.term_key, t.term_key as ancestor_key, s.ancestor_primary_id "
				+ "into temp table closure "
				+ "from term ha, term_ancestor s, term t "
				+ "where ha.term_key = s.term_key "
				+ " and ha.vocab_name = 'Disease Ontology' "
				+ " and s.ancestor_primary_id = t.primary_id "
				+ " and t.vocab_name = 'Disease Ontology' "
				+ "union "
				+ "select ha.term_key, ha.term_key, ha.primary_id "
				+ "from term ha "
				+ "where ha.vocab_name = 'Disease Ontology' ";
		ex.executeVoid(cmd0);
		this.createTempIndex("closure", "term_key");
		this.createTempIndex("closure", "ancestor_key"); 
		logger.info("created temp table closure");
		
		// mapping from a marker to its alleles (including traversing relationship tables)

		String cmd1 = "with pair_union as ("
			+ "select marker_key, allele_key "
			+ "from marker_to_allele "
			+ "union "
			+ "select related_marker_key, allele_key "
			+ "from allele_related_marker "
			+ "where relationship_category in "
			+ "  ('mutation_involves', 'expresses_component')"
			+ ") "
			+ "select marker_key, allele_key "
			+ "into temp table pairs "
			+ "from pair_union";
		ex.executeVoid(cmd1);
		this.createTempIndex("pairs", "marker_key");
		this.createTempIndex("pairs", "allele_key"); 
		logger.info("created temp table pairs");
	}
	
	/** get a mapping from reference keys (with strain data) to list of the Strain IDs for that reference.
	 */
	public Map<String,Set<String>> getStrainIDs(int startKey, int endKey) throws Exception {
		String cmd = "select r.reference_key, s.primary_id "
			+ "from strain_to_reference r, strain s "
			+ "where s.strain_key = r.strain_key "
			+ "  and r.reference_key >= " + startKey
			+ "  and r.reference_key < " + endKey;
		return populateLookup(cmd, "reference_key", "primary_id", "strain IDs");
	}

	// get a mapping from reference key to IDs for its associated disease-relevant markers
	public Map<String,Set<String>> getDiseaseRelevantMarkerMap(int startKey, int endKey) throws Exception {
		String diseaseRelevantMarkerQuery = "select mtr.reference_key, m.primary_id marker_id "
			+ "from hdp_marker_to_reference mtr,marker m "
			+ "where m.marker_key = mtr.marker_key "
			+ " and mtr.reference_key >= " + startKey
			+ " and mtr.reference_key < " + endKey;

		return populateLookup(diseaseRelevantMarkerQuery,"reference_key","marker_id",
				"disease relevant marker IDs (for linking from disease portal)");
	}

	// get a mapping from reference key to disease IDs associated with it
	public Map<String,Set<String>> getDiseaseRelevantReferenceMap(int startKey, int endKey) throws Exception {
		String diseaseRelevantRefQuery = "select distinct trt.reference_key, c.ancestor_primary_id as disease_id "
			+ "from hdp_term_to_reference trt, term ha, closure c "
			+ "where ha.term_key = c.term_key "
			+ " and c.term_key = trt.term_key "
			+ " and trt.reference_key >= " + startKey
			+ " and trt.reference_key < " + endKey
			+ " and ha.vocab_name = 'Disease Ontology' ";

		return populateLookup(diseaseRelevantRefQuery,"reference_key","disease_id",
				"disease IDs to references (for linking from disease portal)");
	}

	// get a mapping from reference keys to the marker IDs associated with them via GO data
	public Map<String,Set<String>> getGoMarkerMap (int startKey, int endKey) throws Exception {
		String goMarkerSQL = "select distinct m.primary_id, r.reference_key "
			+ "from marker_to_annotation mta, "
			+ "    annotation a, "
			+ "    annotation_reference r, "
			+ "    marker m "
			+ "where mta.annotation_key = a.annotation_key "
			+ "    and a.annotation_key = r.annotation_key "
			+ "    and mta.marker_key = m.marker_key "
			+ "    and r.reference_key >= " + startKey
			+ "    and r.reference_key < " + endKey
			+ "    and a.evidence_code != 'ND' "
			+ "    and m.organism = 'mouse' "
			+ "    and a.vocab_name = 'GO' ";		

		return populateLookup(goMarkerSQL, "reference_key", "primary_id", "GO/Marker annotations");
	}

	// get a mapping from reference keys to the marker IDs associated with them via phenotype data
	public Map<String,Set<String>> getPhenoMarkerMap (int startKey, int endKey) throws Exception {
		String phenoMarkerSQL = "select distinct m.primary_id, r.reference_key "
			+ "from marker m, pairs mta, allele a, "
			+ "  allele_to_reference atr, reference r "
			+ "where m.marker_key = mta.marker_key "
			+ "  and mta.allele_key = a.allele_key "
			+ "  and a.is_wild_type = 0 "
			+ "  and a.allele_key = atr.allele_key "
			+ "  and atr.reference_key >= " + startKey
			+ "  and atr.reference_key < " + endKey
			+ "  and atr.reference_key = r.reference_key ";

		return populateLookup(phenoMarkerSQL, "reference_key", "primary_id", "MP/Marker associations");
	}
	
	// get a mapping from reference keys to the marker keys associated with them
	public Map<String,Set<String>> getMarkerMap (int startKey, int endKey) throws Exception {
		String markerToRefSQL = "select reference_key, marker_key from marker_to_reference where reference_key >= " + startKey + " and reference_key < "+ endKey;
		return populateLookup(markerToRefSQL, "reference_key", "marker_key", "associated markers");
	}

	// Get all reference -> book publisher relationships (for books)
	public Map<String,Set<String>> getPublisherMap (int startKey, int endKey) throws Exception {
		String pubToRefSQL = "select reference_key, publisher from reference_book where reference_key >= " + startKey + " and reference_key < "+ endKey;
		return populateLookup(pubToRefSQL, "reference_key", "publisher", "book publishers");            
	}

	// get a mapping from reference key to the allele keys associated with it
	public Map<String,Set<String>> getAlleleMap (int startKey, int endKey) throws Exception {
		String alleleToRefSQL = "select reference_key, allele_key from allele_to_reference where reference_key >= " + startKey + " and reference_key < "+ endKey;
		return populateLookup(alleleToRefSQL, "reference_key", "allele_key", "allele keys");
	}

	// get a mapping from reference key to the authors associated with it
	public Map<String,Set<String>> getAuthorMap (int startKey, int endKey) throws Exception {
		String referenceAuthorSQL = "select reference_key, author from reference_individual_authors where reference_key >= " + startKey + " and reference_key < "+ endKey;
		return populateLookup(referenceAuthorSQL, "reference_key", "author", "authors");
	}

	// get a mapping from reference key to the last author associated with it
	public Map<String,Set<String>> getLastAuthorMap (int startKey, int endKey) throws Exception {
		String referenceAuthorLastSQL = "select reference_key, author from reference_individual_authors where is_last = 1 and reference_key >= " + startKey + " and reference_key < "+ endKey;
		return populateLookup(referenceAuthorLastSQL, "reference_key", "author", "last authors");
	}

	// get a mapping from reference key to the first author associated with it
	public Map<String,Set<String>> getFirstAuthorMap (int startKey, int endKey) throws Exception {
		String referenceAuthorFirstSQL = "select reference_key, author from reference_individual_authors where sequence_num = 1 and reference_key >= " + startKey + " and reference_key < "+ endKey;
		return populateLookup(referenceAuthorFirstSQL, "reference_key", "author", "first authors");
	}

	// get a mapping from reference key to the IDs associated with it
	public Map<String,Set<String>> getReferenceIDMap (int startKey, int endKey) throws Exception {
		String referenceIDsSQL = "select reference_key, acc_id from reference_id where reference_key >= " + startKey + " and reference_key < " + endKey;
		return populateLookup(referenceIDsSQL, "reference_key", "acc_id", "reference IDs");
	}

	// add the count for the given solrField to the document, add the has-data flag if the count is
	// greater than zero, and return true if the count is non-zero (false if zero count).
	public boolean handleCount(SolrInputDocument doc, String solrField, int count, String flag) {
		doc.addField(solrField, count>0 ? 1 : 0);
		if (count > 0) {
			doc.addField(IndexConstants.REF_HAS_DATA, flag);
			return true;
		}
		return false;
	}
	
	// add all permutations of the given 'author' as values for 'solrField' in the given 'doc'
	public void addAuthorPermutations(SolrInputDocument doc, String solrField, String author) {
		if (author != null) {
			String [] temp = author.split("[\\W-&&[^']]");
			if (temp.length > 1) {
				String tempString = "";
				for (int i = 0; i< temp.length && i <= 3; i++) {
					if (i == 0) {
						tempString = temp[i];
					}
					else {
						tempString = tempString + " " + temp[i];
					}
					doc.addField(solrField, tempString);
				}
			}
		}
	}

	// Add in all of the indivudual authors, specifically formatted for searching.
	// In a nutshell we split on whitespace, and then add in each resulting token into the database,
	// as well as the entirety of the author string.  We optionally include them in the author facet field.
	public void addAuthorData(SolrInputDocument doc, String solrField, Map<String,Set<String>> authorMap, String refKey, boolean includeInFacetField) {
		if (authorMap.containsKey(refKey)) {
			for (String author: authorMap.get(refKey)) {
				doc.addField(solrField, author);
				addAuthorPermutations(doc, solrField, author);

				if (includeInFacetField) {
					// Add in a single untouched version of the formatted authors
					if (author == null || author.equals(" ")) {
						doc.addField(IndexConstants.REF_AUTHOR_FACET, "No author listed");
					}
					else {
						doc.addField(IndexConstants.REF_AUTHOR_FACET, author);
					}
				}
			}
		}
	}
	
	/**
	 * The main worker of this class, it identifies the min & max reference keys, breaks the set of
	 * references into chunks, caches in memory some mappings for the current batch, iterates over
	 * the references in the batch to produce solr documents, and periodically ships them to the
	 * Solr server.
	 */
	public void index() throws Exception
	{
		// find the lowest and highest reference keys, for use later in iteration
		
		String minMaxCmd = "select min(reference_key) as min_key, max(reference_key) as max_key from reference";
		ResultSet minMaxRs = ex.executeProto(minMaxCmd);
		minMaxRs.next();
		int minKey = minMaxRs.getInt("min_key");	// lowest reference key in database
		int maxKey = minMaxRs.getInt("max_key");	// highest reference key in database
		int batchSize = 50000;						// number of reference keys per batch
		logger.info("Processing references " + minKey + " to " + maxKey);
		
		createTempTables();
		
		// collection of solr documents waiting to be sent to the server
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		// iterate through batches of references (inclusive of startKey, exclusive of endKey in each batch)
		int startKey = minKey;
		while (startKey <= maxKey) {
			int endKey = startKey + batchSize;
			logger.info("Starting batch >= " + startKey + " and < " + endKey);
			
			// populate caches of data for this batch
			Map<String,Set<String>> strainIDs = getStrainIDs(startKey, endKey);
			Map<String,Set<String>> diseaseRelevantMarkerMap = getDiseaseRelevantMarkerMap(startKey, endKey);
			Map<String,Set<String>> diseaseRelevantRefMap = getDiseaseRelevantReferenceMap(startKey, endKey);
			Map<String,Set<String>> goMarkerMap = getGoMarkerMap(startKey, endKey);
			Map<String,Set<String>> phenoMarkerMap = getPhenoMarkerMap(startKey, endKey);
			Map<String,Set<String>> markerMap = getMarkerMap(startKey, endKey);
			Map<String,Set<String>> publisherMap = getPublisherMap(startKey, endKey);
			Map<String,Set<String>> alleleMap = getAlleleMap(startKey, endKey);
			Map<String,Set<String>> authorMap = getAuthorMap(startKey, endKey);
			Map<String,Set<String>> lastAuthorMap = getLastAuthorMap(startKey, endKey);
			Map<String,Set<String>> firstAuthorMap = getFirstAuthorMap(startKey, endKey);
			Map<String,Set<String>> referenceIDMap = getReferenceIDMap(startKey, endKey);
		
			logger.info("Getting basic references data");
			String referenceSQL = "select r.reference_key, r.year, r.jnum_id, r.pubmed_id, r.authors, r.title,"
				+ " r.journal, r.vol, r.issue, ra.abstract, rc.marker_count, rc.disease_model_count, rc.probe_count, rc.mapping_expt_count, "
				+ " rc.gxd_index_count, rc.gxd_result_count, rc.gxd_structure_count, rc.gxd_assay_count, "
				+ " rc.allele_count, rc.sequence_count, rc.go_annotation_count, r.reference_group "
				+ "from reference as r "
				+ "inner join reference_abstract ra on r.reference_key = ra.reference_key "
				+ "inner join reference_counts as rc on r.reference_key = rc.reference_key "
				+ "where r.reference_key >= " + startKey
				+ "  and r.reference_key < " + endKey;

			ResultSet rs_overall = ex.executeProto(referenceSQL);
			while (rs_overall.next()) {
				SolrInputDocument doc = new SolrInputDocument();
				
				String refKey = rs_overall.getString("reference_key");

				// add simple data from the query
				
				doc.addField(IndexConstants.REF_KEY, refKey);
				doc.addField(IndexConstants.REF_AUTHOR, rs_overall.getString("authors"));
				doc.addField(IndexConstants.REF_JOURNAL, rs_overall.getString("journal"));
				doc.addField(IndexConstants.REF_JOURNAL_FACET, rs_overall.getString("journal"));
				doc.addField(IndexConstants.REF_GROUPING, rs_overall.getString("reference_group"));
				doc.addField(IndexConstants.REF_TITLE, rs_overall.getString("title"));
				doc.addField(IndexConstants.REF_YEAR, rs_overall.getString("year"));
				doc.addField(IndexConstants.REF_ISSUE, rs_overall.getString("issue"));
				doc.addField(IndexConstants.REF_VOLUME, rs_overall.getString("vol"));
				doc.addField(IndexConstants.REF_ABSTRACT, rs_overall.getString("abstract"));

				// add data from the various mappings we collected for this batch
				
				addAllFromLookup(doc, IndexConstants.STRAIN_ID, refKey, strainIDs);
				addAllFromLookup(doc, IndexConstants.REF_DISEASE_RELEVANT_MARKER_ID, refKey, diseaseRelevantMarkerMap);
				addAllFromLookup(doc, IndexConstants.REF_DISEASE_ID, refKey, diseaseRelevantRefMap);
				addAllFromLookup(doc, IndexConstants.REF_GO_MARKER_ID, refKey, goMarkerMap); 
				addAllFromLookup(doc, IndexConstants.REF_PHENO_MARKER_ID, refKey, phenoMarkerMap); 
				addAllFromLookup(doc, IndexConstants.MRK_KEY, refKey, markerMap);
				addAllFromLookup(doc, IndexConstants.REF_JOURNAL_FACET, refKey, publisherMap);
				addAllFromLookup(doc, IndexConstants.ALL_KEY, refKey, alleleMap);
				addAllFromLookup(doc, IndexConstants.REF_ID, refKey, referenceIDMap);
				addAuthorData(doc, IndexConstants.REF_AUTHOR_FORMATTED, authorMap, refKey, true);
				addAuthorData(doc, IndexConstants.REF_FIRST_AUTHOR, firstAuthorMap, refKey, false);
				addAuthorData(doc, IndexConstants.REF_LAST_AUTHOR, lastAuthorMap, refKey, false);
				
				// special handling for title and abstract in joined fields

				String titleAndAbstract = "";	// overall joined together title + abstract fields
				
				if (rs_overall.getString("title") != null) {
					String tempTitle = rs_overall.getString("title").replaceAll("\\p{Punct}", " ");

					doc.addField(IndexConstants.REF_TITLE_STEMMED, tempTitle);
					doc.addField(IndexConstants.REF_TITLE_UNSTEMMED, tempTitle);
					titleAndAbstract = tempTitle;
				}

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

				doc.addField(IndexConstants.REF_TITLE_ABSTRACT_STEMMED, titleAndAbstract);
				doc.addField(IndexConstants.REF_TITLE_ABSTRACT_UNSTEMMED, titleAndAbstract);
				
				// add just the numeric part of the J: number
				
				if (rs_overall.getString("jnum_id") != null) {
					String jnumID [] = rs_overall.getString("jnum_id").split(":");
					doc.addField(IndexConstants.REF_ID, jnumID[1]); 
				}
				
				// now deal with all the counts, tracking if we've found a non-zero one
				
				boolean foundACount = handleCount(doc, IndexConstants.MRK_COUNT, rs_overall.getInt("marker_count"), "Genome features");
				foundACount = handleCount(doc, IndexConstants.DO_MODEL_COUNT, rs_overall.getInt("disease_model_count"), "Disease models") || foundACount;
				foundACount = handleCount(doc, IndexConstants.PRB_COUNT, rs_overall.getInt("probe_count"), "Molecular probes and clones") || foundACount;
				foundACount = handleCount(doc, IndexConstants.MAP_EXPT_COUNT, rs_overall.getInt("mapping_expt_count"), "Mapping data") || foundACount;
				foundACount = handleCount(doc, IndexConstants.GXD_INDEX_COUNT, rs_overall.getInt("gxd_index_count"), "Expression literature records") || foundACount;
				foundACount = handleCount(doc, IndexConstants.GXD_RESULT_COUNT, rs_overall.getInt("gxd_result_count"), "Expression: assays results") || foundACount;
				foundACount = handleCount(doc, IndexConstants.GXD_STRUCT_COUNT, rs_overall.getInt("gxd_structure_count"), "Expression: assays results") || foundACount;
				foundACount = handleCount(doc, IndexConstants.GXD_ASSAY_COUNT, rs_overall.getInt("gxd_assay_count"), "Expression: assays results") || foundACount;
				foundACount = handleCount(doc, IndexConstants.ALL_COUNT, rs_overall.getInt("allele_count"), "Phenotypic alleles") || foundACount;
				foundACount = handleCount(doc, IndexConstants.SEQ_COUNT, rs_overall.getInt("sequence_count"), "Sequences") || foundACount;
				foundACount = handleCount(doc, IndexConstants.GO_ANNOT_COUNT, rs_overall.getInt("go_annotation_count"), "Functional annotations (GO)") || foundACount;

				if (!foundACount) {
					doc.addField(IndexConstants.REF_HAS_DATA, "No curated data");
				}
				
				// Count of orthologs isn't implemented yet.
				doc.addField(IndexConstants.ORTHO_COUNT, 0);

				docs.add(doc);
				if (docs.size() > 1000) {
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>();
				}
			}
			rs_overall.close();
			
			// commit with each batch to help Solr's memory usage
			logger.info("Committing docs >= " + startKey + " and < " + endKey);
			commit();

			logger.info("Finished batch >= " + startKey + " and < " + endKey);
			startKey = endKey;
		}
		
		// final push & commit in case anything's hanging around
		writeDocs(docs);
		commit();
	}
}
