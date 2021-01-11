package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.jax.mgi.shr.VocabTerm;
import org.jax.mgi.shr.VocabTermCache;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.util.EasyStemmer;
import org.jax.mgi.shr.fe.util.StopwordRemover;

/* Is: an indexer that builds the index supporting the quick search's feature (marker + allele) bucket (aka- bucket 1).
 * 		Each document in the index represents a single searchable data element (e.g.- symbol, name, synonym, annotated
 * 		term, etc.) for a marker or allele.
 */
public class QSFeatureBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	private static String MARKER = "marker";	// used to indicate we are currently working with markers
	private static String ALLELE = "allele";	// used to indicate we are currently working with alleles

	// weights to prioritize different types of search terms / IDs
	private static int PRIMARY_ID_WEIGHT = 100;
	private static int SECONDARY_ID_WEIGHT = 97;
	private static int STRAIN_GENE_ID_WEIGHT = 94;
	private static int SYMBOL_WEIGHT = 91;
	private static int HUMAN_ID_WEIGHT = 88;
	private static int NAME_WEIGHT = 85;
	private static int MARKER_SYMBOL_WEIGHT = 82;
	private static int MARKER_NAME_WEIGHT = 79;
	private static int SYNONYM_WEIGHT = 76;
	private static int MARKER_SYNONYM_WEIGHT = 73;
	private static int TRANSGENE_PART_WEIGHT = 72;
	private static int PROTEOFORM_ID_WEIGHT = 70;
	private static int PROTEIN_DOMAIN_WEIGHT = 67;
	private static int PROTEIN_FAMILY_WEIGHT = 64;
	private static int ORTHOLOG_SYMBOL_WEIGHT = 61;
	private static int ORTHOLOG_NAME_WEIGHT = 58;
	private static int ORTHOLOG_SYNONYM_WEIGHT = 55;
	private static int DISEASE_ID_WEIGHT = 52;
	private static int DISEASE_NAME_WEIGHT = 49;
	private static int DISEASE_SYNONYM_WEIGHT = 46;
	private static int DISEASE_ORTHOLOG_WEIGHT = 43;
	private static int DISEASE_DEFINITION_WEIGHT = 40;
	private static int GO_ID_WEIGHT = 37;
	private static int GO_NAME_WEIGHT = 34;
	private static int GO_SYNONYM_WEIGHT = 31;
	private static int GO_DEFINITION_WEIGHT = 28;
	private static int MP_ID_WEIGHT = 25;
	private static int MP_NAME_WEIGHT = 22;
	private static int MP_SYNONYM_WEIGHT = 19;
	private static int MP_DEFINITION_WEIGHT = 16;
	private static int EMAP_ID_WEIGHT = 25;
	private static int EMAP_NAME_WEIGHT = 22;
	private static int EMAP_SYNONYM_WEIGHT = 19;
	private static int EMAP_DEFINITION_WEIGHT = 16;
	private static int HPO_ID_WEIGHT = 13;
	private static int HPO_NAME_WEIGHT = 10;
	private static int HPO_SYNONYM_WEIGHT = 7;
	private static int HPO_DEFINITION_WEIGHT = 4;
	
	// what to add between the base fewi URL and marker or allele ID, to link directly to a detail page
	private static Map<String,String> uriPrefixes;			
	static {
		uriPrefixes = new HashMap<String,String>();
		uriPrefixes.put(MARKER, "/marker/");
		uriPrefixes.put(ALLELE, "/allele/");
	}

	public static Map<Integer, QSFeature> features;			// marker or allele key : QSFeature object

	public static Map<Integer,String> chromosome;			// marker or allele key : chromosome
	public static Map<Integer,String> startCoord;			// marker or allele key : start coordinate
	public static Map<Integer,String> endCoord;				// marker or allele key : end coordinate
	public static Map<Integer,String> strand;				// marker or allele key : strand
	
	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

	private long uniqueKey = 0;						// ascending counter of documents created
	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch
	protected int uncommittedBatchLimit = 1000;		// number of batches to allow before doing a Solr commit
	private int uncommittedBatches = 0;				// number of batches sent to Solr and not yet committed

	private VocabTermCache goProcessCache;			// caches of data for various GO DAGs
	private VocabTermCache goFunctionCache;
	private VocabTermCache goComponentCache;
	private VocabTermCache diseaseOntologyCache;	// cache of data for DO DAG
	private VocabTermCache mpOntologyCache;			// cache of data for MP DAG

	private Map<Integer,Set<Integer>> highLevelTerms;		// maps from a term key to the keys of its high-level ancestors
	
	private EasyStemmer stemmer = new EasyStemmer();
	private StopwordRemover stopwordRemover = new StopwordRemover();
	
	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSFeatureBucketIndexerSQL() {
		super("qsFeatureBucket");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	// To minimize the number of documents created for a given feature, we assume that all queries are ordered
	// by marker/allele, and we keep track of each featureID seen and all the terms indexed for it.  Then
	// in addDoc() we check that we haven't already indexed the current term for the current ID.  This should
	//	prevent duplicates within each data type with one point of change (except for query ordering). 
	Map<String,Set<String>> indexedTerms = new HashMap<String,Set<String>>();
	
	// Reset the cache of indexed terms.
	private void clearIndexedTermCache(String featureType) {
		logger.info("Clearing cache for " + indexedTerms.size() + " " + featureType + "s");
		indexedTerms = new HashMap<String,Set<String>>();
	}

	// Add this doc to the batch we're collecting.  If the stack hits our threshold, send it to the server and reset it.
	private void addDoc(SolrInputDocument doc) {
		// See comments above definition of indexedTerms for explanation of logic.
		
		String primaryID = (String) doc.getFieldValue(IndexConstants.QS_PRIMARY_ID);
		if (!indexedTerms.containsKey(primaryID)) {
			indexedTerms.put(primaryID, new HashSet<String>());
		}
		
		String term = null;
		
		if (doc.containsKey(IndexConstants.QS_SEARCH_TERM_EXACT)) {
			term = ((String) doc.getFieldValue(IndexConstants.QS_SEARCH_TERM_EXACT)).toLowerCase();
		}
		if ((term == null) && (doc.containsKey(IndexConstants.QS_SEARCH_TERM_STEMMED))) {
			term = ((String) doc.getFieldValue(IndexConstants.QS_SEARCH_TERM_STEMMED)).toLowerCase();
		}
		
		if ((term != null) && !indexedTerms.get(primaryID).contains(term)) {
			docs.add(doc);
			if (docs.size() >= solrBatchSize)  {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
				uncommittedBatches++;
				if (uncommittedBatches >= uncommittedBatchLimit) {
					commit();
					uncommittedBatches = 0;
				}
			}
			indexedTerms.get(primaryID).add(term);
		}
	}
	
	// Build and return a new SolrInputDocument with the given fields filled in.
	private SolrInputDocument buildDoc(QSFeature feature, String exactTerm, String stemmedTerm, String searchTermDisplay,
			String searchTermType, Integer searchTermWeight) {

		SolrInputDocument doc = feature.getNewDocument();
		if (exactTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM_EXACT, exactTerm); }
		if (stemmedTerm != null) {
			doc.addField(IndexConstants.QS_SEARCH_TERM_STEMMED, stemmer.stemAll(stopwordRemover.remove(stemmedTerm)));
		}
		doc.addField(IndexConstants.QS_SEARCH_TERM_DISPLAY, searchTermDisplay);
		doc.addField(IndexConstants.QS_SEARCH_TERM_TYPE, searchTermType);
		doc.addField(IndexConstants.QS_SEARCH_TERM_WEIGHT, searchTermWeight);
		doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
		return doc;
	}
	
	// get the maximum sequence number assigned to markers
	private long getMaxMarkerSequenceNum() throws Exception {
		String cmd = "select max(by_symbol) as max_seq_num from marker_sequence_num";
		ResultSet rs = ex.executeProto(cmd);
		long maxSeqNum = 0;
		if (rs.next()) {  
			maxSeqNum = rs.getLong("max_seq_num");
		}
		rs.close();
		logger.info("Got max marker seq num = " + maxSeqNum);
		return maxSeqNum;
	}
	
	// Use the given SQL 'cmd' to retrieve a set of data for caching.  Cache keys are in the given 'keyField',
	// and values in the given 'valueField'.  Object type is specified by 'objectType'.
	private Map<String,Set<String>> buildCache(String cmd, String keyField, String valueField, String objectType) throws Exception {
		logger.debug("Beginning to cache " + objectType + "s");
		Map<String,Set<String>> cache = new HashMap<String,Set<String>>();
		
		int ct = 0;
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			ct++;
			String key = rs.getString(keyField);
			if (!cache.containsKey(key)) {
				cache.put(key, new HashSet<String>());
			}
			cache.get(key).add(rs.getString(valueField));
		}
		rs.close();
		
		logger.debug(" - Finished caching " + ct + " " + objectType + "(s)");
		return cache;
	}
	
	// For each vocabulary term, we need to know what its high-level ancestors (aka- slim terms) are, so
	// look them up and cache them.
	private void cacheHighLevelTerms() throws SQLException {
		this.highLevelTerms = new HashMap<Integer,Set<Integer>>();
		
		String cmd = "select term_key, header_term_key from term_to_header"; 

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next() ) {
			Integer termKey = rs.getInt("term_key");
			if (!highLevelTerms.containsKey(termKey)) {
				this.highLevelTerms.put(termKey, new HashSet<Integer>());
			}
			this.highLevelTerms.get(termKey).add(rs.getInt("header_term_key"));
		}
		rs.close();
		
		logger.info("Cached ancestors of " + this.highLevelTerms.size() + " terms");
	}
	
	/* cache the chromosomes, coordinates, and strands for objects of the given feature type
	 */
	private void cacheLocations(String featureType) throws Exception {
		chromosome = new HashMap<Integer,String>();
		startCoord = new HashMap<Integer,String>();
		endCoord = new HashMap<Integer,String>();
		strand = new HashMap<Integer,String>();
	
		String cmd;
		if (MARKER.equals(featureType)) {
			// may need to pick up chromosome from either coordinates, centimorgans, or cytogenetic band
			// (in order of preference)
			cmd = "select m.marker_key as feature_key, coord.chromosome, coord.start_coordinate, coord.end_coordinate, "
				+ " coord.strand, cm.chromosome as cm_chromosome, cyto.chromosome as cyto_chromosome " + 
				"from marker m " + 
				"left outer join marker_location coord on (m.marker_key = coord.marker_key "
				+ " and coord.location_type = 'coordinates') " + 
				"left outer join marker_location cyto on (m.marker_key = cyto.marker_key "
				+ " and cyto.location_type = 'cytogenetic') " + 
				"left outer join marker_location cm on (m.marker_key = cm.marker_key "
				+ " and cm.location_type = 'centimorgans') " + 
				"where m.organism = 'mouse' " + 
				"and m.status = 'official'";
		} else {
			cmd = "select a.allele_key as feature_key, coord.chromosome, coord.start_coordinate, coord.end_coordinate, "
				+ " coord.strand, cm.chromosome as cm_chromosome, cyto.chromosome as cyto_chromosome " + 
				"from allele a " +
				"inner join marker_to_allele m on (a.allele_key = m.allele_key) " + 
				"left outer join marker_location coord on (m.marker_key = coord.marker_key "
				+ " and coord.location_type = 'coordinates') " + 
				"left outer join marker_location cyto on (m.marker_key = cyto.marker_key "
				+ " and cyto.location_type = 'cytogenetic') " +
				"left outer join marker_location cm on (m.marker_key = cm.marker_key "
				+ " and cm.location_type = 'centimorgans')" +
				"where a.is_wild_type = 0";
		}

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of locations processed
		while (rs.next()) {
			ct++;

			Integer featureKey = rs.getInt("feature_key");
			String chrom = rs.getString("chromosome");
			
			if (chrom == null) {
				chrom = rs.getString("cm_chromosome");
				if (chrom == null) {
					chrom = rs.getString("cyto_chromosome");
				}
			}
			
			chromosome.put(featureKey, chrom);
			startCoord.put(featureKey, rs.getString("start_coordinate"));
			endCoord.put(featureKey, rs.getString("end_coordinate"));
			strand.put(featureKey, rs.getString("strand"));
		}
		rs.close();

		logger.info(" - cached " + ct + " locations for " + chromosome.size() + " " + featureType + "s");
	}
	
	// Load accession IDs for the given feature type (marker or allele) and create Solr documents for them.
	private void indexIDs(String featureType) throws Exception {
		logger.info(" - indexing IDs for " + featureType);
		if ((features == null) || (features.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }

		String cmd;
		
		if (MARKER.equals(featureType)) {
			// include both marker IDs and their related sequence IDs -- but exclude strain gene IDs, so
			// we can do those in a separate method and format their Best Match output differently
			cmd = "select i.marker_key as feature_key, i.acc_id, i.logical_db " + 
				"from marker m, marker_id i " + 
				"where m.marker_key = i.marker_key " + 
				"and m.status = 'official' " + 
				"and m.organism = 'mouse' " + 
				"and m.primary_id != i.acc_id " +
				"and i.private = 0 " +
				"union " +
				"select mts.marker_key, i.acc_id, i.logical_db " + 
				"from marker_to_sequence mts, sequence_id i " + 
				"where mts.sequence_key = i.sequence_key" +
				" and i.logical_db not in ('MGI Strain Gene', 'Mouse Genome Project') " +
				"order by 1";
		} else {
			cmd = "select i.allele_key as feature_key, i.acc_id, i.logical_db " + 
					"from allele_id i, allele a " + 
					"where i.private = 0 " +
					"and i.allele_key = a.allele_key " +
					"and a.primary_id != i.acc_id " +
					"and a.is_wild_type = 0 " +
					"order by 1";
		}

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("feature_key");
			String id = rs.getString("acc_id");
			String logicalDB = rs.getString("logical_db");
			
			if (id.startsWith(logicalDB)) {
				logicalDB = "ID";
			}

			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDoc(buildDoc(feature, id, null, id, logicalDB, SECONDARY_ID_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " IDs for " + featureType + "s");
	}
	
	// Index human ortholog IDs for markers.  If feature type is for alleles, just skip this.
	private void indexHumanOrthologIDs(String featureType) throws Exception {
		if (ALLELE.equals(featureType)) { return; }
		logger.info(" - human ortholog IDs for " + featureType);

		String cmd = "select distinct m.marker_key, ha.logical_db, ha.acc_id " + 
				"from marker h, marker_id ha, " + 
				"  homology_cluster_organism_to_marker hm, " + 
				"  homology_cluster_organism ho, " + 
				"  homology_cluster_organism mo, " + 
				"  homology_cluster_organism_to_marker mm, " + 
				"  marker m " + 
				"where h.organism = 'human' " + 
				"and h.marker_key = ha.marker_key " + 
				"and h.marker_key = hm.marker_key " + 
				"and hm.cluster_organism_key = ho.cluster_organism_key " + 
				"and ho.cluster_key = mo.cluster_key " + 
				"and mo.cluster_organism_key = mm.cluster_organism_key " + 
				"and mm.marker_key= m.marker_key " + 
				"and m.organism = 'mouse' " + 
				"and m.status = 'official' " +
				"order by m.marker_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("marker_key");
			String orthologID = rs.getString("acc_id");
			String logicalDB = rs.getString("logical_db");
			
			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDoc(buildDoc(feature, orthologID, null, orthologID + " (" + logicalDB + " - Human)", "ID", HUMAN_ID_WEIGHT));

				// For OMIM IDs we also need to index them without the prefix.
				if (orthologID.startsWith("OMIM:")) {
					String noPrefix = orthologID.replaceAll("OMIM:", "");
					addDoc(buildDoc(feature, noPrefix, null, noPrefix + " (" + logicalDB + " - Human)", "ID", HUMAN_ID_WEIGHT));
				}
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " human ortholog IDs for markers");
	}

	// Index mouse DO (Disease Ontology) annotations.
	private void indexMouseDiseaseAnnotations (String featureType) throws Exception {
		String cmd = null;
		
		// need to check that we're considering the roll-up rules (to exclude Gt(ROSA), etc.)
		if (ALLELE.equals(featureType)) {
			cmd = "with causative_markers as ( " + 
					"select distinct m.marker_key, d.primary_id as disease_id " + 
					"from disease d, disease_group g, disease_row r, disease_row_to_marker tm, marker m " + 
					"where tm.marker_key = m.marker_key " + 
					"and d.disease_key = g.disease_key " + 
					"and g.disease_group_key = r.disease_group_key " + 
					"and r.disease_row_key = tm.disease_row_key " + 
					"and m.organism = 'mouse' " + 
					"and tm.is_causative = 1 " + 
					") " + 
					"select distinct m.symbol, m.allele_key as feature_key, r.primary_id  " + 
					"from allele m, allele_to_genotype mtg, genotype t, disease_model dm, term r, causative_markers cm, marker_to_allele am " + 
					"where m.allele_key = mtg.allele_key  " + 
					"and mtg.genotype_key = t.genotype_key  " + 
					"and t.is_disease_model = 1  " + 
					"and t.genotype_key = dm.genotype_key  " + 
					"and dm.is_not_model = 0  " + 
					"and dm.disease_id = r.primary_id  " + 
					"and cm.marker_key = am.marker_key " + 
					"and m.allele_key = am.allele_key " + 
					"and r.primary_id = cm.disease_id " +
					"order by m.allele_key";
		} else {
			cmd = "select distinct m.symbol, m.marker_key as feature_key, d.primary_id " + 
				"from disease d, disease_group g, disease_row r, disease_row_to_marker tm, marker m " + 
				"where tm.marker_key = m.marker_key " + 
				"and d.disease_key = g.disease_key " + 
				"and g.disease_group_key = r.disease_group_key " + 
				"and r.disease_row_key = tm.disease_row_key " + 
				"and m.organism = 'mouse' " + 
				"and tm.is_causative = 1 " +
				"order by m.marker_key";
		}

		logger.info(" - indexing mouse disease annotations for " + featureType);

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of annotations processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("feature_key");
			VocabTerm vt = diseaseOntologyCache.getTerm(rs.getString("primary_id"));
			
			if (features.containsKey(featureKey) && (vt != null)) {
				QSFeature feature = features.get(featureKey);
				String term = vt.getTerm();
				
				// For each annotation, we need to index:
				// 1. term name, primary ID, secondary IDs, definition, and synonyms for that term.
				// 2. And for each of its ancestors, we also need to index:
				//    a. term name, primary ID, secondary IDs, definition, and synonyms.

				addDoc(buildDoc(feature, null, term, term, "Disease Model", DISEASE_NAME_WEIGHT));

				if (vt.getAllIDs() != null) {
					for (String accID : vt.getAllIDs()) {
						addDoc(buildDoc(feature, accID, null, term + " (" + accID + ")", "Disease Model", DISEASE_ID_WEIGHT));
					}
				}
				
				if (vt.getDefinition() != null) {
					addDoc(buildDoc(feature, null, vt.getDefinition(), term, "Disease Model", DISEASE_DEFINITION_WEIGHT));
				}

				if (vt.getSynonyms() != null) {
					for (String synonym : vt.getSynonyms()) {
						addDoc(buildDoc(feature, null, synonym, term + " (synonym: " + synonym +")", "Disease Model", DISEASE_SYNONYM_WEIGHT));
					}
				}
				
				// ancestors of this vocab term
				for (Integer ancestorKey : vt.getAncestorKeys()) {
					VocabTerm ancestor = diseaseOntologyCache.getTerm(ancestorKey);
					String ancTerm = ancestor.getTerm();
					
					addDoc(buildDoc(feature, null, ancTerm, term + " (subterm of " + ancTerm + ")", "Disease Model", DISEASE_NAME_WEIGHT));

					if (ancestor.getAllIDs() != null) {
						for (String accID : ancestor.getAllIDs()) {
							addDoc(buildDoc(feature, accID, null, term + " (subterm of " + ancestor.getTerm() + ", with ID "+ accID + ")", "Disease Model", DISEASE_ID_WEIGHT));
						}
					}
				
					if (ancestor.getDefinition() != null) {
						addDoc(buildDoc(feature, null, ancestor.getDefinition(), term + "(subterm of " + ancestor.getTerm() + ")", "Disease Model", DISEASE_DEFINITION_WEIGHT));
					}

					if (ancestor.getSynonyms() != null) {
						for (String synonym : ancestor.getSynonyms()) {
							addDoc(buildDoc(feature, null, synonym, term + " (subterm of " + ancestor.getTerm() + ", with synonym " + synonym +")", "Disease Model", DISEASE_SYNONYM_WEIGHT));
						}
					}
				}
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " mouse disease annotations for " + featureType);
	}

	// Index human ortholog DO (Disease Ontology) annotations.  No-op for alleles.
	private void indexHumanDiseaseAnnotations (String featureType) throws Exception {
		if (ALLELE.equals(featureType)) { return; }

		// from mouse marker through orthology tables to human marker, then to human DO annotations
		String cmd = "select mm.marker_key as mouse_marker_key, a.term_id " + 
				"from marker mm, homology_cluster_organism_to_marker cm, " + 
				"  homology_cluster_organism om, homology_cluster hc, " + 
				"  homology_cluster_organism oh, homology_cluster_organism_to_marker ch, " + 
				"  marker mh, marker_to_annotation mta, annotation a " + 
				"where mm.organism = 'mouse' " + 
				"  and mm.marker_key = cm.marker_key " + 
				"  and cm.cluster_organism_key = om.cluster_organism_key " + 
				"  and om.cluster_key = hc.cluster_key " + 
				"  and hc.source = 'HomoloGene and HGNC' " + 
				"  and hc.cluster_key = oh.cluster_key " + 
				"  and oh.cluster_organism_key = ch.cluster_organism_key " + 
				"  and ch.marker_key = mh.marker_key " + 
				"  and mh.organism = 'human' " + 
				"  and mh.marker_key = mta.marker_key " + 
				"  and mta.annotation_key = a.annotation_key " + 
				"  and a.annotation_type = 'DO/Human Marker' " +
				"order by mm.marker_key";

		logger.info(" - indexing human ortholog disease annotations");

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of annotations processed
		while (rs.next()) {
			ct++;
			Integer mouseMarkerKey = rs.getInt("mouse_marker_key");
			VocabTerm vt = diseaseOntologyCache.getTerm(rs.getString("term_id"));

			if (features.containsKey(mouseMarkerKey)) {
				QSFeature feature = features.get(mouseMarkerKey);
				String term = vt.getTerm();

				addDoc(buildDoc(feature, null, term, term, "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
				if (vt.getAllIDs() != null) {
					for (String accID : vt.getAllIDs()) {
						addDoc(buildDoc(feature, accID, null, term + " (" + accID + ")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
					}
				}
				
				if (vt.getSynonyms() != null) {
					for (String synonym : vt.getSynonyms()) {
						addDoc(buildDoc(feature, null, synonym, term + " (synonym: " + synonym +")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
					}
				}
				
				// ancestors of this vocab term
				for (Integer ancestorKey : vt.getAncestorKeys()) {
					VocabTerm ancestor = diseaseOntologyCache.getTerm(ancestorKey);
					String ancTerm = ancestor.getTerm();
					
					addDoc(buildDoc(feature, null, ancTerm, term + " (subterm of " + ancTerm + ")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));

					if (ancestor.getAllIDs() != null) {
						for (String accID : ancestor.getAllIDs()) {
							addDoc(buildDoc(feature, accID, null, term + " (subterm of " + ancTerm + ", with ID " + accID + ")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
						}
					}
				
					if (ancestor.getDefinition() != null) {
						addDoc(buildDoc(feature, null, ancestor.getDefinition(), term + " (subterm of " + ancTerm + ")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
					}

					if (ancestor.getSynonyms() != null) {
						for (String synonym : ancestor.getSynonyms()) {
							addDoc(buildDoc(feature, null, synonym, term + " (subterm of " + ancTerm + ", with synonym " + synonym +")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
						}
					}
				}
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " human ortholog disease annotations");
	}

	// Index proteoform IDs for markers.  If feature type is for alleles, just skip this.
	private void indexProteoformIDs (String featureType) throws Exception {
		if (ALLELE.equals(featureType)) { return; }
		logger.info(" - indexing proteoform IDs for " + featureType);

		String cmd = "select mta.marker_key, a.term, a.term_id " + 
			"from annotation a, marker_to_annotation mta, marker m " + 
			"where a.annotation_key = mta.annotation_key " + 
			"and mta.annotation_type = 'Proteoform/Marker' " + 
			"and mta.marker_key = m.marker_key " + 
			"and m.organism = 'mouse' " +
			"order by mta.marker_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("marker_key");
			String accID = rs.getString("term_id");
			String term = rs.getString("term");
			
			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDoc(buildDoc(feature, accID, null, term + " (" + accID + ")", "Proteoform", PROTEOFORM_ID_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " proteoform IDs for markers");
	}

	// Index strain gene IDs for markers.  If feature type is for alleles, just skip this.
	private void indexStrainGenes(String featureType) throws Exception {
		if (ALLELE.equals(featureType)) { return; }
		logger.info(" - indexing strain gene IDs for " + featureType);

		String cmd = "select m.marker_key, gm.logical_db, gm.gene_model_id " + 
			"from marker m, strain_marker sm, strain_marker_gene_model gm " + 
			"where m.marker_key = sm.canonical_marker_key " + 
			"and sm.strain_marker_key = gm.strain_marker_key " +
			"order by m.marker_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("marker_key");
			String sgID = rs.getString("gene_model_id");
			String logicalDB = rs.getString("logical_db");
			
			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDoc(buildDoc(feature, sgID, null, sgID + " (" + logicalDB + ")", "ID", STRAIN_GENE_ID_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " strain gene IDs for markers");
	}

	// Index protein family annotations for markers.  If feature type is for alleles, just skip this.
	private void indexProteinFamilies(String featureType) throws Exception {
		if (ALLELE.equals(featureType)) { return; }
		logger.info(" - indexing protein families for " + featureType);

		String cmd = "select mta.marker_key, a.term, a.term_id " + 
			"from annotation a, marker_to_annotation mta, marker m " + 
			"where a.annotation_key = mta.annotation_key " + 
			"and mta.annotation_type = 'PIRSF/Marker' " + 
			"and mta.marker_key = m.marker_key " + 
			"and m.organism = 'mouse' " +
			"order by mta.marker_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of terms processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("marker_key");
			String termID = rs.getString("term_id");
			String term = rs.getString("term");
			
			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDoc(buildDoc(feature, termID, null, term + " (" + termID + ")", "Protein Family", PROTEIN_FAMILY_WEIGHT));
				addDoc(buildDoc(feature, null, term, term, "Protein Domain", PROTEIN_FAMILY_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " protein domains for markers");
	}

	// Index protein domain annotations for markers.  If feature type is for alleles, just skip this.
	private void indexProteinDomains(String featureType) throws Exception {
		if (ALLELE.equals(featureType)) { return; }
		logger.info(" - indexing protein domains for " + featureType);

		String cmd = "select distinct m.marker_key as feature_key, a.term_id as primary_id, a.term "
			+ "from annotation a, marker_to_annotation mta, marker m "
			+ "where a.annotation_key = mta.annotation_key "
			+ "and mta.marker_key = m.marker_key "
			+ "and a.vocab_name = 'InterPro Domains' "
			+ "order by m.marker_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of terms processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("feature_key");
			String termID = rs.getString("primary_id");
			String term = rs.getString("term");
			
			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDoc(buildDoc(feature, termID, null, term + " (" + termID + ")", "Protein Domain", PROTEIN_DOMAIN_WEIGHT));
				addDoc(buildDoc(feature, null, term, term, "Protein Domain", PROTEIN_DOMAIN_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " protein domains for markers");
	}

	/* Cache and return all synonyms for the given feature type (markers or alleles), populating the synonyms object.
	 * Note that this caching method is different from the others in that it does not directly modify the object's
	 * cache.  Instead, the map produced is returned.  This lets us also easily cache marker synonyms when dealing
	 * with alleles.
	 */
	private Map<Integer,List<String>> cacheSynonyms(String featureType) throws Exception {
		logger.info(" - caching synonyms for " + featureType);
		
		HashMap<Integer,List<String>> mySynonyms = new HashMap<Integer,List<String>>();
		
		String cmd;
		if (MARKER.equals(featureType)) {
			// Do not index synonyms for transgene markers, as their Tg alleles already get done.
			cmd = "select s.marker_key as feature_key, s.synonym " + 
					"from marker_synonym s, marker m " +
					"where s.marker_key = m.marker_key " +
					"and m.marker_subtype != 'transgene' " +
					"order by s.marker_key";
		} else {
			cmd = "select s.allele_key as feature_key, s.synonym " + 
					"from allele_synonym s, allele a " +
					"where s.allele_key = a.allele_key " +
					"and a.is_wild_type = 0 " +
					"order by s.allele_key";
		}
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of synonyms processed
		while (rs.next()) {
			ct++;

			Integer featureKey = rs.getInt("feature_key");
			String synonym = rs.getString("synonym");
			
			if (!mySynonyms.containsKey(featureKey)) {
				mySynonyms.put(featureKey, new ArrayList<String>());
			}
			mySynonyms.get(featureKey).add(synonym);
		}
		rs.close();

		logger.info(" - cached " + ct + " synonyms for " + mySynonyms.size() + " " + featureType + "s");
		return mySynonyms;
	}

	/* Index searchable nomenclature (symbol, name, synonyms) for orthologous markers in non-mouse organisms.
	 */
	private void indexOrthologNomenclature(String featureType) throws Exception {
		// For now, bail out if looking for allele data.
		if (!MARKER.equals(featureType)) { return; }

		// Retrieve terms to be indexed with ortholog nomenclature.  Prefer old mouse nomen, then human,
		// then rat before all the others.  We can thus prioritize which organisms are shown for which
		// terms in the "best match" column in the fewi.
		String cmd = "select m.primary_id, n.term, n.term_type, case " + 
				"  when n.term_type like 'old%' then 1 " + 
				"  when n.term_type like 'human%' then 2 " + 
				"  when n.term_type like 'rat%' then 3 " + 
				"  else 4 " + 
				"  end as preference, m.marker_key " + 
				"from marker_searchable_nomenclature n, marker m " + 
				"where n.term_type in ( " + 
				"    'old symbol', 'human name', 'human synonym', 'human symbol',  " + 
				"    'related synonym', 'old name',  " + 
				"    'rat symbol', 'rat synonym', 'chimpanzee symbol',  " + 
				"    'cattle symbol', 'chicken symbol', 'dog symbol',  " + 
				"    'macaque, rhesus symbol', 'western clawed frog symbol',  " + 
				"    'zebrafish symbol') " + 
				" and n.marker_key = m.marker_key " +
				"order by 1, 4, 2";						// sorting by marker, preference, then term
			
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;
		String previousID = "";		// last marker we processed
		
		// For each feature, we only want to index each unique string once.  So we use the preference field
		// for ordering and then just index the first one for each unique string.
		Set<String> termsSeen = new HashSet<String>();

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");

			// New feature?  If so, clear the cache of lowercase terms we've cached.
			if (!previousID.equals(primaryID)) {
				termsSeen.clear();
				previousID = primaryID;
			}

			String term = rs.getString("term");
			String termLower = term.toLowerCase();
			String termType = rs.getString("term_type");
			Integer markerKey = rs.getInt("marker_key");

			// First time we've seen this feature/term pair?  If so, index it and remember it.
			// If we've not already seen this particular string (case insensitive), we need to cache it.
			if (!termsSeen.contains(termLower)) {
				termsSeen.add(termLower);
				if (features.containsKey(markerKey)) {
					QSFeature feature = features.get(markerKey);

					// Symbols and synonyms go in the exact-match field, names get stemmed.
					int weight = 0;
					if (termType.contains("symbol")) {
						weight = ORTHOLOG_SYMBOL_WEIGHT;
						addDoc(buildDoc(feature, termLower, null, term, termType, weight));
					}
					else if (termType.contains("name")) {
						weight = ORTHOLOG_NAME_WEIGHT;
						addDoc(buildDoc(feature, null, termLower, term, termType, weight));
					}
					else if (termType.contains("synonym")) {
						weight = ORTHOLOG_SYNONYM_WEIGHT;
						addDoc(buildDoc(feature, termLower, null, term, termType, weight));
					}
					i++; 
				}
			}
		}
		rs.close();

		logger.info(" - indexed " + i + " ortholog nomen terms for " + featureType + "s");
	}
	
	// Build a cache of facets (high-level slim terms) for each marker.  Currently uses slimgrid tables for efficiency.
	// dagAbbrev should be a value from grid_name_abbreviation field in marker_grid_heading table.
	public Map<Integer,Set<String>> getFacetValues(String featureType, String dagAbbrev) throws SQLException {
		Map<Integer,Set<String>> keyToFacets = new HashMap<Integer,Set<String>>();
		String cmd = null;

		// only work with markers for now
		if (!MARKER.equals(featureType)) { return keyToFacets; }
	 	
		// can handles GO, Anatomy, and MP facets for now
		if ("C".equals(dagAbbrev) || "F".equals(dagAbbrev) || "P".equals(dagAbbrev) || "Anatomy".equals(dagAbbrev) || "MP".equals(dagAbbrev)) {
			cmd = "select c.marker_key as feature_key, t.term " + 
				"from marker_grid_cell c, marker_grid_heading h, marker_grid_heading_to_term ht, term t " + 
				"where c.value > 0 " + 
				"and c.heading_key = h.heading_key " + 
				"and h.heading_key = ht.heading_key " + 
				"and trim(h.grid_name_abbreviation) = '" + dagAbbrev + "' " +
				"and ht.term_key = t.term_key " +
				"order by c.marker_key";
		}
		
		if (cmd != null) {
			ResultSet rs = ex.executeProto(cmd, cursorLimit);
			while (rs.next()) {
				Integer key = rs.getInt("feature_key");
				
				if (!keyToFacets.containsKey(key)) {
					keyToFacets.put(key, new HashSet<String>());
				}
				keyToFacets.get(key).add(rs.getString("term"));
			}
			rs.close();
			logger.info("Collected " + dagAbbrev + " facets for " + keyToFacets.size() + " markers");
		}
		
		return keyToFacets;
	}
	
	// Retrieve synonyms, create documents, and index them.  For markers, do marker synonyms.  For alleles, do both
	// marker synonyms and allele synonyms.
	private void indexSynonyms(String featureType) throws Exception {
		if ((features == null) || (features.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }

		// used for either allele synonyms or marker synonyms, depending on featureType
		Map<Integer,List<String>> mySynonyms = cacheSynonyms(featureType);
		
		// Both markers and alleles have directly-associated synonyms.
		for (Integer featureKey : mySynonyms.keySet()) {
			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				for (String synonym : mySynonyms.get(featureKey)) {
					addDoc(buildDoc(feature, synonym, null, synonym, "Synonym", SYNONYM_WEIGHT));
				}
			}
		}
		logger.info("Indexed synonyms for " + mySynonyms.size() + " " + featureType + "s");
		
		// Alleles should also be indexed for the synonyms of their respective markers.
		if (ALLELE.contentEquals(featureType)) {
			// used for marker synonyms when the featureType is for alleles
			Map<Integer,List<String>> markerSynonyms = cacheSynonyms(MARKER);

			String cmd = "select a.allele_key, m.marker_key " + 
				"from allele a, marker_to_allele m " + 
				"where a.allele_key = m.allele_key " + 
				"and a.is_wild_type = 0 " +
				"order by a.allele_key";
		
			ResultSet rs = ex.executeProto(cmd, cursorLimit);
			while (rs.next()) {
				Integer alleleKey = rs.getInt("allele_key");
				Integer markerKey = rs.getInt("marker_key");
				
				if (markerSynonyms.containsKey(markerKey)) {
					QSFeature feature = features.get(alleleKey);
					for (String synonym : markerSynonyms.get(markerKey)) {
						addDoc(buildDoc(feature, synonym, null, synonym, "Marker Synonym", MARKER_SYNONYM_WEIGHT));
					}
				}
			}
			rs.close();
			logger.info("Indexed marker synonyms for alleles");
		}
	}
	
	// Look up annotations and index according to the given SQL command.  Expected field names: feature_key, primary_id.
	// The given SQL command must order by the feature_key field to help identify duplicates (including ancestor terms).
	// VocabTermCache vtc is used to look up data for each vocab term.
	// The 4 weights are for the four different data pieces to be indexed.
	private void indexAnnotations (String featureType, String dataType, String cmd, VocabTermCache vtc,
		Integer nameWeight, Integer idWeight, Integer synonymWeight, Integer definitionWeight) throws SQLException {
		
		logger.info(" - indexing " + dataType + " for " + featureType);

		// counter of indexed items
		int i = 0;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next())  {  
			Integer featureKey = rs.getInt("feature_key");
			String termID = rs.getString("primary_id");
			
			if (features.containsKey(featureKey) && vtc.containsKey(termID)) {
				QSFeature feature = features.get(featureKey);
				VocabTerm term = vtc.getTerm(termID);

				// need to also index ancestors of the term, to handle down-the-DAG searches...
				Set<VocabTerm> toIndex = new HashSet<VocabTerm>();
				toIndex.add(term);

				for (VocabTerm ancestor : vtc.getAncestors(term.getTermKey())) {
					toIndex.add(ancestor);
				}
				
				for (VocabTerm termToIndex : toIndex) {
					String name = termToIndex.getTerm();
					if ((nameWeight != null) && (name != null) && (name.length() > 0)) {
						addDoc(buildDoc(feature, null, name, name, dataType, nameWeight));
						i++;
					}
				
					String definition = termToIndex.getDefinition();
					if ((definitionWeight != null) && (definition != null) && (definition.length() > 0)) {
						addDoc(buildDoc(feature, null, definition, definition, dataType + " Definition", definitionWeight));
						i++;
					}
				
					List<String> termIDs = termToIndex.getAllIDs();
					if ((idWeight != null) && (termIDs != null) && (termIDs.size() > 0)) {
						for (String id : termIDs) {
							addDoc(buildDoc(feature, id, null, term.getTerm() + " (ID: " + id + ")", dataType, idWeight));
							i++;
						}
					}
				
					List<String> synonyms = termToIndex.getSynonyms();
					if ((synonymWeight != null) && (synonyms != null) && (synonyms.size() > 0)) {
						for (String synonym : synonyms) {
							addDoc(buildDoc(feature, null, synonym, term.getTerm() + " (synonym: " + synonym + ")", dataType, synonymWeight));
							i++;
						}
					}
				}
			}
		}
		rs.close();
		
		logger.info(" - done with " + dataType + " for " + featureType + " (indexed " + i + " items)");
	}
	
	// Index the MP terms for the given feature type.  Assumes caches are loaded.
	private void indexMP(String featureType) throws SQLException {
		String cmd = null;

		if (MARKER.equals(featureType)) {
			cmd = "select a.term_id as primary_id, m.marker_key as feature_key " + 
				"from annotation a, marker_to_annotation mta, marker m " + 
				"where a.annotation_key = mta.annotation_key " + 
				"and mta.marker_key = m.marker_key " + 
				"and a.annotation_type = 'Mammalian Phenotype/Marker' " + 
				"and m.organism = 'mouse' " + 
				"and a.qualifier is null " +
				"order by m.marker_key";
			
		} else { // feature type is allele
			
		}

		if (cmd != null) {
			indexAnnotations(featureType, "Phenotype", cmd, mpOntologyCache,
				MP_NAME_WEIGHT, MP_ID_WEIGHT, MP_SYNONYM_WEIGHT, MP_DEFINITION_WEIGHT);
		}
	}
	
	/* Load the features of the given type, cache them, generate initial documents and send them to Solr.
	 * Assumes cacheLocations has been run for this featureType.
	 */
	private void buildInitialDocs(String featureType) throws Exception {
		logger.info(" - loading " + featureType + "s");

		Map<Integer, Set<String>> goProcessFacetCache = this.getFacetValues(featureType, "P");
		Map<Integer, Set<String>> goFunctionFacetCache = this.getFacetValues(featureType, "F");
		Map<Integer, Set<String>> goComponentFacetCache = this.getFacetValues(featureType, "C");
		Map<Integer, Set<String>> phenotypeFacetCache = this.getFacetValues(featureType, "MP");
		
		features = new HashMap<Integer,QSFeature>();
		
		long padding = 0;	// amount of initial padding before sequence numbers should begin
		
		String cmd;
		if (MARKER.equals(featureType)) { 
			cmd = "select m.marker_key as feature_key, m.primary_id, m.symbol, m.name, m.marker_subtype as subtype, " + 
					"s.by_symbol as sequence_num " +
				"from marker m, marker_sequence_num s " + 
				"where m.organism = 'mouse' " + 
				"and m.marker_key = s.marker_key " +
				"and m.status = 'official' " +
				"order by m.marker_key";
		} else {
			cmd = "select a.allele_key as feature_key, a.primary_id, a.symbol, a.name, a.allele_type as subtype, " + 
					"s.by_symbol as sequence_num, m.symbol as marker_symbol, m.name as marker_name " +
				"from allele a, allele_sequence_num s, marker_to_allele mta, marker m " + 
				"where a.allele_key = s.allele_key " +
				"and a.allele_key = mta.allele_key " +
				"and a.is_wild_type = 0 " +
				"and mta.marker_key = m.marker_key " +
				"order by a.allele_key";

			// Start allele sequence numbers after marker ones, so we prefer markers to alleles in each star-tier of returns.
			padding = getMaxMarkerSequenceNum();
		}
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		while (rs.next())  {  
			i++;
			Integer featureKey = rs.getInt("feature_key");

			//--- prepare the new feature object
			
			QSFeature feature = new QSFeature(featureKey);
			features.put(featureKey, feature);
			
			feature.primaryID = rs.getString("primary_id");
			feature.symbol = rs.getString("symbol");
			feature.name = rs.getString("name");
			feature.sequenceNum = padding + rs.getLong("sequence_num");	
			feature.featureType = rs.getString("subtype");
			
			if (MARKER.equals(featureType)) {
				feature.isMarker = 1;
			} else {
				feature.isMarker = 0;
			}
			
			if (goProcessFacetCache.containsKey(featureKey)) { feature.goProcessFacets = goProcessFacetCache.get(featureKey); }
			if (goFunctionFacetCache.containsKey(featureKey)) { feature.goFunctionFacets = goFunctionFacetCache.get(featureKey); }
			if (goComponentFacetCache.containsKey(featureKey)) { feature.goComponentFacets = goComponentFacetCache.get(featureKey); }
			if (phenotypeFacetCache.containsKey(featureKey)) { feature.phenotypeFacets = phenotypeFacetCache.get(featureKey); }

			//--- index the new feature object in basic ways (primary ID, symbol, name, etc.)
			
			addDoc(buildDoc(feature, feature.primaryID, null, feature.primaryID, "ID", PRIMARY_ID_WEIGHT));
			addDoc(buildDoc(feature, null, feature.name, feature.name, "Name", NAME_WEIGHT));
			
			// Do not index transgene markers by symbol or synonyms.  (Their Tg alleles already get returned.)
			if (!"transgene".equals(feature.featureType)) {
				addDoc(buildDoc(feature, feature.symbol, null, feature.symbol, "Symbol", SYMBOL_WEIGHT));
			}

			// For alleles, we also need to consider the nomenclature of each one's associated marker.
			if (ALLELE.equals(featureType)) { 
				String markerSymbol = rs.getString("marker_symbol");
				String markerName = rs.getString("marker_name");
				addDoc(buildDoc(feature, markerSymbol, null, markerSymbol, "Marker Symbol", MARKER_SYMBOL_WEIGHT));
				addDoc(buildDoc(feature, null, markerName, markerName, "Marker Name", MARKER_NAME_WEIGHT));
				
				// For transgenic alleles, we need to index by the parts of the allele symbol, including those
				// parts appearing in parentheses.
				
				// Strip out parentheses, hyphens, and commas.  Then skip the "Tg" and index the other pieces by exact match.
				String[] tgParts = feature.symbol.replaceAll("\\(", " ").replaceAll("\\)", " ").replaceAll("-", " ").replaceAll(",", " ").replaceAll("/", " ").split(" ");
				for (String part : tgParts) {
					if (!"Tg".equals(part)) {
						addDoc(buildDoc(feature, part, null, feature.symbol, "Symbol", TRANSGENE_PART_WEIGHT));
					}
				}
			}
		}

		rs.close();

		logger.info("done with basic data for " + i + " " + featureType + "s");
	}
	
	/* process the given feature type, loading data from the database, composing documents, and writing to Solr.
	 */
	private void processFeatureType(String featureType) throws Exception {
		logger.info("beginning " + featureType);
		
		cacheLocations(featureType);
		buildInitialDocs(featureType);

		indexSynonyms(featureType);
		indexOrthologNomenclature(featureType);
		clearIndexedTermCache(featureType);		// only clear once all nomen done

		indexIDs(featureType);
		indexHumanOrthologIDs(featureType);
		indexStrainGenes(featureType);
		indexProteoformIDs(featureType);
		clearIndexedTermCache(featureType);		// only clear once all IDs done
		
		indexProteinDomains(featureType);
		indexProteinFamilies(featureType);
		clearIndexedTermCache(featureType);		// only clear once all protein stuff done

		indexMouseDiseaseAnnotations(featureType);
		indexHumanDiseaseAnnotations(featureType);
		clearIndexedTermCache(featureType);		// only clear once all disease data done
		
		indexMP(featureType);
		clearIndexedTermCache(featureType);		// only clear once all phenotype data done

		logger.info("finished " + featureType);
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		this.setSkipOptimizer(true);
		
		// cache vocabulary term data
		goFunctionCache = new VocabTermCache("Function", ex);
		goProcessCache = new VocabTermCache("Process", ex);
		goComponentCache = new VocabTermCache("Component", ex);
		diseaseOntologyCache = new VocabTermCache("Disease Ontology", ex);
		mpOntologyCache = new VocabTermCache("Mammalian Phenotype", ex);
		this.cacheHighLevelTerms();

		// process one vocabulary at a time, keeping caches in memory only for the current vocabulary
		processFeatureType(MARKER);
		processFeatureType(ALLELE);
		
		// send any remaining documents and commit all the changes to Solr
		if (docs.size() > 0) {
			writeDocs(docs);
		}
		commit();
	}
	
	// private class for caching marker or allele data that will be re-used across multiple documents
	private class QSFeature {
		private Integer featureKey;
		private Integer isMarker;
		public String featureType;
		public String symbol;
		public String primaryID;
		public String name;
		public Long sequenceNum;
		public Set<String> goProcessFacets;
		public Set<String> goFunctionFacets;
		public Set<String> goComponentFacets;
		public Set<String> diseaseFacets;
		public Set<String> phenotypeFacets;
		public Set<String> markerTypeFacets;

		// constructor
		public QSFeature(Integer featureKey) {
			this.featureKey = featureKey;
		}
		
		// compose and return a new SolrInputDocument including the fields for this feature
		public SolrInputDocument getNewDocument() {
			SolrInputDocument doc = new SolrInputDocument();
			String uriPrefix = null;

			if (this.isMarker != null) {
				doc.addField(IndexConstants.QS_IS_MARKER, this.isMarker);

				if (this.isMarker.equals(1)) {
					uriPrefix = uriPrefixes.get(MARKER);
				} else {
					uriPrefix = uriPrefixes.get(ALLELE);
				}
			}

			if (this.primaryID != null) {
				doc.addField(IndexConstants.QS_PRIMARY_ID, this.primaryID);
				if (uriPrefix != null) {
					doc.addField(IndexConstants.QS_DETAIL_URI, uriPrefix + this.primaryID);
				}
			}

			if (this.featureType != null) { doc.addField(IndexConstants.QS_FEATURE_TYPE, this.featureType); }
			if (this.symbol != null) { doc.addField(IndexConstants.QS_SYMBOL, this.symbol); }
			if (this.name != null) { doc.addField(IndexConstants.QS_NAME, this.name); }
			if (this.sequenceNum != null) { doc.addField(IndexConstants.QS_SEQUENCE_NUM, this.sequenceNum); }

			if (chromosome.containsKey(this.featureKey)) { doc.addField(IndexConstants.QS_CHROMOSOME, chromosome.get(featureKey)); }
			if (startCoord.containsKey(this.featureKey)) { doc.addField(IndexConstants.QS_START_COORD, startCoord.get(featureKey)); }
			if (endCoord.containsKey(this.featureKey)) { doc.addField(IndexConstants.QS_END_COORD, endCoord.get(featureKey)); }
			if (strand.containsKey(this.featureKey)) { doc.addField(IndexConstants.QS_STRAND, strand.get(featureKey)); }
				
			if (this.goProcessFacets != null) { doc.addField(IndexConstants.QS_GO_PROCESS_FACETS, this.goProcessFacets); }
			if (this.goFunctionFacets != null) { doc.addField(IndexConstants.QS_GO_FUNCTION_FACETS, this.goFunctionFacets); }
			if (this.goComponentFacets != null) { doc.addField(IndexConstants.QS_GO_COMPONENT_FACETS, this.goComponentFacets); }
			if (this.diseaseFacets != null) { doc.addField(IndexConstants.QS_DISEASE_FACETS, this.diseaseFacets); }
			if (this.phenotypeFacets != null) { doc.addField(IndexConstants.QS_PHENOTYPE_FACETS, this.phenotypeFacets); }
			if (this.markerTypeFacets != null) { doc.addField(IndexConstants.QS_MARKER_TYPE_FACETS, this.markerTypeFacets); }

			return doc;
		}
	}
}