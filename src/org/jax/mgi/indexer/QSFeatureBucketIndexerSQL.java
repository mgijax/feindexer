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
import org.jax.mgi.shr.VocabTerm;
import org.jax.mgi.shr.VocabTermCache;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.util.EasyStemmer;
import org.jax.mgi.shr.fe.util.StopwordRemover;

/* Is: an indexer that builds the index supporting the quick search's feature (marker) bucket (aka- bucket 1).
 * 		Each document in the index represents a single searchable data element (e.g.- symbol, name, synonym, annotated
 * 		term, etc.) for a marker.
 */
public class QSFeatureBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	// weights to prioritize different types of search terms / IDs
	private static int LOCATION_WEIGHT = 2000;
	private static int PRIMARY_ID_WEIGHT = 1500;
	private static int STRAIN_GENE_ID_WEIGHT = 1450;
	private static int SYMBOL_WEIGHT = 1400;
	private static int NAME_WEIGHT = 1350;
	private static int SYNONYM_WEIGHT = 1300;
	private static int ORTHOLOG_ID_WEIGHT = 1250;
	private static int SECONDARY_ID_WEIGHT = 1200;
	private static int PROTEOFORM_ID_WEIGHT = 1150;
	private static int PROTEIN_DOMAIN_WEIGHT = 1100;
	private static int PROTEIN_FAMILY_WEIGHT = 1050;
	private static int ORTHOLOG_SYMBOL_WEIGHT = 1000;
	private static int ORTHOLOG_NAME_WEIGHT = 950;
	private static int ORTHOLOG_SYNONYM_WEIGHT = 900;
	private static int DISEASE_ID_WEIGHT = 850;
	private static int DISEASE_NAME_WEIGHT = 800;
	private static int DISEASE_SYNONYM_WEIGHT = 750;
	private static int DISEASE_ORTHOLOG_WEIGHT = 700;
	private static int GO_ID_WEIGHT = 650;
	private static int GO_NAME_WEIGHT = 600;
	private static int GO_SYNONYM_WEIGHT = 550;
	private static int MP_ID_WEIGHT = 500;
	private static int MP_NAME_WEIGHT = 450;
	private static int MP_SYNONYM_WEIGHT = 400;
	private static int EMAP_ID_WEIGHT = 350;
	private static int EMAP_NAME_WEIGHT = 300;
	private static int EMAP_SYNONYM_WEIGHT = 250;
	private static int HPO_ID_WEIGHT = 200;
	private static int HPO_NAME_WEIGHT = 150;
	private static int HPO_SYNONYM_WEIGHT = 100;
	
	public static Map<Integer, QSFeature> features;			// marker key : QSFeature object

	public static Map<Integer,String> chromosome;			// marker key : chromosome
	public static Map<Integer,String> startCoord;			// marker key : start coordinate
	public static Map<Integer,String> endCoord;				// marker key : end coordinate
	public static Map<Integer,String> strand;				// marker key : strand
	
	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

	private long uniqueKey = 0;						// ascending counter of documents created
	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch
	protected int uncommittedBatchLimit = 500;		// number of batches to allow before doing a Solr commit
	private int uncommittedBatches = 0;				// number of batches sent to Solr and not yet committed

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
	// by marker, and we keep track of each featureID seen and all the terms indexed for it.  Then
	// in addDoc() we check that we haven't already indexed the current term for the current ID.  This should
	//	prevent duplicates within each data type with one point of change (except for query ordering). 
	Map<String,Set<String>> indexedTerms = new HashMap<String,Set<String>>();
	
	// Reset the cache of indexed terms.
	private void clearIndexedTermCache() {
		logger.info("Clearing cache for " + indexedTerms.size() + " markers");
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

	// Add this doc to the batch we're collecting.  If the stack hits our threshold, send it to the server and reset it.
	// Use this for cases where we don't need to monitor uniqueness.
	private void addDocUnchecked(SolrInputDocument doc) {
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
	}
	
	// Build and return a new SolrInputDocument with the given fields filled in.
	private SolrInputDocument buildDoc(QSFeature feature, String exactTerm, String inexactTerm, String stemmedTerm,
			String searchTermDisplay, String searchTermType, Integer searchTermWeight) {

		SolrInputDocument doc = feature.getNewDocument();
		if (exactTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM_EXACT, exactTerm); }
		if (inexactTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM_INEXACT, inexactTerm); }
		if (stemmedTerm != null) {
			doc.addField(IndexConstants.QS_SEARCH_TERM_STEMMED, stemmer.stemAll(stopwordRemover.remove(stemmedTerm)));
		}
		doc.addField(IndexConstants.QS_SEARCH_TERM_DISPLAY, searchTermDisplay);
		doc.addField(IndexConstants.QS_SEARCH_TERM_TYPE, searchTermType);
		doc.addField(IndexConstants.QS_SEARCH_TERM_WEIGHT, searchTermWeight);
		doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
		return doc;
	}

	// Build and return a new SolrInputDocument with the given fields filled in for coordinate searching.
	private SolrInputDocument buildCoordinateDoc(QSFeature feature, String coordType, String chromosome, Long startCoord,
			Long endCoord, Long seqNum, String searchTermDisplay, String searchTermType, Integer searchTermWeight) {

		SolrInputDocument doc = feature.getNewDocument();
		doc.addField(IndexConstants.QS_SEARCH_COORD_TYPE, coordType);
		doc.addField(IndexConstants.QS_SEARCH_CHROMOSOME, chromosome);
		doc.addField(IndexConstants.QS_COORD_SEQUENCE_NUM, seqNum);
		
		if (startCoord != null) { doc.addField(IndexConstants.QS_SEARCH_START_COORD, startCoord); }
		if (endCoord != null) { doc.addField(IndexConstants.QS_SEARCH_END_COORD, endCoord); }

		doc.addField(IndexConstants.QS_SEARCH_TERM_DISPLAY, searchTermDisplay);
		doc.addField(IndexConstants.QS_SEARCH_TERM_TYPE, searchTermType);
		doc.addField(IndexConstants.QS_SEARCH_TERM_WEIGHT, searchTermWeight);
		doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
		return doc;
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
	
	/* Cache the chromosomes, coordinates, and strands for objects of the given feature type.
	 * NOTE: For efficiency's sake, these are not currently used in the fewi.  Instead, it retrieves them
	 * from the database and caches them.
	 */
	private void cacheLocations() throws Exception {
		chromosome = new HashMap<Integer,String>();
		startCoord = new HashMap<Integer,String>();
		endCoord = new HashMap<Integer,String>();
		strand = new HashMap<Integer,String>();
	
		List<String> cmds = new ArrayList<String>();
		
		// may need to pick up chromosome from either coordinates, centimorgans, or cytogenetic band
		// (in order of preference)
		cmds.add("select m.marker_key as feature_key, coord.chromosome, coord.start_coordinate, coord.end_coordinate, "
			+ " coord.strand, cm.chromosome as cm_chromosome, cyto.chromosome as cyto_chromosome " + 
			"from marker m " + 
			"left outer join marker_location coord on (m.marker_key = coord.marker_key "
			+ " and coord.location_type = 'coordinates') " + 
			"left outer join marker_location cyto on (m.marker_key = cyto.marker_key "
			+ " and cyto.location_type = 'cytogenetic') " + 
			"left outer join marker_location cm on (m.marker_key = cm.marker_key "
			+ " and cm.location_type = 'centimorgans') " + 
			"where m.organism = 'mouse' " + 
			"and m.status = 'official'");
		
		for (String cmd : cmds) { 
			ResultSet rs = ex.executeProto(cmd, cursorLimit);

			while (rs.next()) {
				Integer featureKey = rs.getInt("feature_key");
				String chrom = rs.getString("chromosome");

				if (chrom == null) {
					chrom = rs.getString("cm_chromosome");
					if (chrom == null) {
						chrom = rs.getString("cyto_chromosome");
					}
				}

				if (!chromosome.containsKey(featureKey)) { chromosome.put(featureKey, chrom); }
				if (!startCoord.containsKey(featureKey)) { startCoord.put(featureKey, rs.getString("start_coordinate")); }
				if (!endCoord.containsKey(featureKey)) { endCoord.put(featureKey, rs.getString("end_coordinate")); }
				if (!strand.containsKey(featureKey)) { strand.put(featureKey, rs.getString("strand")); }
			}
			rs.close();
		}

		logger.info(" - cached " + chromosome.size() + " locations");
	}
	
	// Load accession IDs and create Solr documents for them.
	private void indexIDs() throws Exception {
		logger.info(" - indexing IDs");
		if ((features == null) || (features.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }

		// include both marker IDs and their related sequence IDs -- but exclude strain gene IDs, so
		// we can do those in a separate method and format their Best Match output differently
		String cmd = "select i.marker_key as feature_key, i.acc_id, i.logical_db, 'Genome Feature' as prefix " + 
			"from marker m, marker_id i " + 
			"where m.marker_key = i.marker_key " + 
			"and m.status = 'official' " + 
			"and m.organism = 'mouse' " + 
			"and m.primary_id != i.acc_id " +
			"and i.private = 0 " +
			"union " +
			"select mts.marker_key, i.acc_id, i.logical_db, 'Genome Feature' as prefix " + 
			"from marker_to_sequence mts, sequence_id i " + 
			"where mts.sequence_key = i.sequence_key" +
			" and i.logical_db not in ('MGI Strain Gene', 'Mouse Genome Project') " +
			"order by 1";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("feature_key");
			String id = rs.getString("acc_id");
			String logicalDB = rs.getString("logical_db");
			String prefix = rs.getString("prefix");
			String suffix = "";
			
			if ("MGI".contentEquals(logicalDB)) {
				logicalDB = prefix + " ID";
			} else if ("Cell Line".equals(prefix)){
				suffix = " (" + logicalDB + ")";
				logicalDB = prefix + " ID";
			} else {
				logicalDB = logicalDB + " ID";
			}

			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDoc(buildDoc(feature, id, null, null, id + suffix, logicalDB, SECONDARY_ID_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " IDs");
	}
	
	// Index ortholog IDs.
	private void indexOrthologIDs() throws Exception {
		logger.info(" - ortholog IDs");

		String cmd = "select distinct m.marker_key, ha.logical_db, ha.acc_id, h.organism " + 
				"from marker h, marker_id ha, " + 
				"  homology_cluster_organism_to_marker hm, " + 
				"  homology_cluster_organism ho, " + 
				"  homology_cluster_organism mo, " + 
				"  homology_cluster_organism_to_marker mm, " + 
				"  marker m " + 
				"where h.organism != 'mouse' " + 
				"and h.marker_key = ha.marker_key " + 
				"and ha.logical_db != 'MyGene' " +
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
			String organism = rs.getString("organism");
			
			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDoc(buildDoc(feature, orthologID, null, null, orthologID + " (" + organism + ")", logicalDB + " ID", ORTHOLOG_ID_WEIGHT));

				// For OMIM IDs we also need to index them without the prefix.
				if (orthologID.startsWith("OMIM:")) {
					String noPrefix = orthologID.replaceAll("OMIM:", "");
					addDoc(buildDoc(feature, noPrefix, null, null, noPrefix + " (" + organism + ")", logicalDB + " ID", ORTHOLOG_ID_WEIGHT));
				}
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " ortholog IDs");
	}

	// Index human ortholog DO (Disease Ontology) annotations.
	private void indexHumanDiseaseAnnotations () throws Exception {
		VocabTermCache diseaseOntologyCache = new VocabTermCache("Disease Ontology", ex);

		// from mouse marker through orthology tables to human marker, then to human DO annotations.
		// Lower part of union is to pick up mouse markers where the human ortholog is an expressed
		// component for a Tg allele.
		String cmd = "select mm.marker_key as mouse_marker_key, a.term_id " + 
				"from marker mm, homology_cluster_organism_to_marker cm, " + 
				"  homology_cluster_organism om, homology_cluster hc, " + 
				"  homology_cluster_organism oh, homology_cluster_organism_to_marker ch, " + 
				"  marker mh, marker_to_annotation mta, annotation a " + 
				"where mm.organism = 'mouse' " + 
				"  and mm.marker_key = cm.marker_key " + 
				"  and cm.cluster_organism_key = om.cluster_organism_key " + 
				"  and om.cluster_key = hc.cluster_key " + 
				"  and hc.source = 'Alliance Direct' " + 
				"  and hc.cluster_key = oh.cluster_key " + 
				"  and oh.cluster_organism_key = ch.cluster_organism_key " + 
				"  and ch.marker_key = mh.marker_key " + 
				"  and mh.organism = 'human' " + 
				"  and mh.marker_key = mta.marker_key " + 
				"  and mta.annotation_key = a.annotation_key " + 
				"  and a.annotation_type = 'DO/Human Marker' " +
				"union " +
				"select h.marker_key, n.term_id " + 
				"from marker h, marker_id i, allele_arm_property o, allele_arm_property pi, " + 
				"  allele_related_marker arm, allele a, marker_to_annotation mta, annotation n " + 
				"where h.marker_key = i.marker_key " + 
				"  and i.acc_id = pi.value " + 
				"  and pi.arm_key = arm.arm_key " + 
				"  and arm.arm_key = o.arm_key " + 
				"  and o.name = 'Non-mouse_Organism' " + 
				"  and o.value = 'Human' " + 
				"  and arm.allele_key = a.allele_key " + 
				"  and a.allele_type in ('Targeted', 'Transgenic') " + 
				"  and h.marker_key = mta.marker_key " + 
				"  and h.organism = 'human' " + 
				"  and mta.annotation_key = n.annotation_key " + 
				"  and n.annotation_type = 'DO/Human Marker' " +
				"order by 1";

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

				addDoc(buildDoc(feature, null, null, term, term, "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
				if (vt.getAllIDs() != null) {
					for (String accID : vt.getAllIDs()) {
						addDoc(buildDoc(feature, accID, null, null, term + " (" + accID + ")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
					}
				}
				
				if (vt.getSynonyms() != null) {
					for (String synonym : vt.getSynonyms()) {
						addDoc(buildDoc(feature, null, null, synonym, term + " (synonym: " + synonym +")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
					}
				}
				
				// ancestors of this vocab term
				for (Integer ancestorKey : vt.getAncestorKeys()) {
					VocabTerm ancestor = diseaseOntologyCache.getTerm(ancestorKey);
					String ancTerm = ancestor.getTerm();
					
					addDoc(buildDoc(feature, null, null, ancTerm, term + " (subterm of " + ancTerm + ")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));

					if (ancestor.getAllIDs() != null) {
						for (String accID : ancestor.getAllIDs()) {
							addDoc(buildDoc(feature, accID, null, null, term + " (subterm of " + ancTerm + ", with ID " + accID + ")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
						}
					}
				
					if (ancestor.getSynonyms() != null) {
						for (String synonym : ancestor.getSynonyms()) {
							addDoc(buildDoc(feature, null, null, synonym, term + " (subterm of " + ancTerm + ", with synonym " + synonym +")", "Disease Ortholog", DISEASE_ORTHOLOG_WEIGHT));
						}
					}
				}
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " human ortholog disease annotations");
	}

	// Index proteoform IDs.
	private void indexProteoformIDs () throws Exception {
		logger.info(" - indexing proteoform IDs");

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
				addDoc(buildDoc(feature, accID, null, null, term + " (" + accID + ")", "Proteoform", PROTEOFORM_ID_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " proteoform IDs");
	}

	// Index strain gene IDs.
	private void indexStrainGenes() throws Exception {
		logger.info(" - indexing strain gene IDs");

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
				addDoc(buildDoc(feature, sgID, null, null, sgID + " (" + logicalDB + ")", "Strain Gene ID", STRAIN_GENE_ID_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " strain gene IDs");
	}

	// Index protein family annotations.
	private void indexProteinFamilies() throws Exception {
		logger.info(" - indexing protein families");

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
				addDoc(buildDoc(feature, termID, null, null, term + " (" + termID + ")", "Protein Family", PROTEIN_FAMILY_WEIGHT));
				addDoc(buildDoc(feature, null, null, term, term, "Protein Domain", PROTEIN_FAMILY_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " protein domains");
	}

	// Index protein domain annotations.
	private void indexProteinDomains() throws Exception {
		logger.info(" - indexing protein domains");

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
				addDoc(buildDoc(feature, termID, null, null, term + " (" + termID + ")", "Protein Domain", PROTEIN_DOMAIN_WEIGHT));
				addDoc(buildDoc(feature, null, null, term, term, "Protein Domain", PROTEIN_DOMAIN_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " protein domains");
	}

	/* Cache and return all marker synonyms, populating the synonyms object.
	 * Note that this caching method is different from the others in that it does not directly modify the object's
	 * cache.  Instead, the map produced is returned.
	 */
	private Map<Integer,List<String>> cacheSynonyms() throws Exception {
		logger.info(" - caching synonyms");
		
		HashMap<Integer,List<String>> mySynonyms = new HashMap<Integer,List<String>>();
		
		// Do not index synonyms for transgene markers, as their Tg alleles already get done.
		String cmd = "select s.marker_key as feature_key, s.synonym " + 
			"from marker_synonym s, marker m " +
			"where s.marker_key = m.marker_key " +
			"and m.marker_subtype != 'transgene' " +
			"order by s.marker_key";
		
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

		logger.info(" - cached " + ct + " synonyms for " + mySynonyms.size() + " markers");
		return mySynonyms;
	}

	/* Index searchable nomenclature (symbol, name, synonyms) for orthologous markers in non-mouse organisms.
	 */
	private void indexOrthologNomenclature() throws Exception {
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
						addDoc(buildDoc(feature, termLower, null, null, term, termType, weight));
					}
					else if (termType.contains("name")) {
						weight = ORTHOLOG_NAME_WEIGHT;
						addDoc(buildDoc(feature, null, null, termLower, term, termType, weight));
					}
					else if (termType.contains("synonym")) {
						weight = ORTHOLOG_SYNONYM_WEIGHT;
						addDoc(buildDoc(feature, termLower, null, null, term, termType, weight));
					}
					i++; 
				}
			}
		}
		rs.close();

		logger.info(" - indexed " + i + " ortholog nomen terms");
	}
	
	// Build a cache of facets (high-level slim terms) for each marker.  Currently uses slimgrid tables for efficiency.
	// dagAbbrev should be a value from grid_name_abbreviation field in marker_grid_heading table.
	public Map<Integer,Set<String>> getFacetValues(String dagAbbrev) throws SQLException {
		Map<Integer,Set<String>> keyToFacets = new HashMap<Integer,Set<String>>();
		String cmd = null;

		if ("C".equals(dagAbbrev) || "F".equals(dagAbbrev) || "P".equals(dagAbbrev) || "Anatomy".equals(dagAbbrev) || "MP".equals(dagAbbrev)) {
			cmd = "select c.marker_key as feature_key, t.term " + 
				"from marker_grid_cell c, marker_grid_heading h, marker_grid_heading_to_term ht, term t " + 
				"where c.value > 0 " + 
				"and c.heading_key = h.heading_key " + 
				"and h.heading_key = ht.heading_key " + 
				"and trim(h.grid_name_abbreviation) = '" + dagAbbrev + "' " +
				"and ht.term_key = t.term_key " +
				"order by c.marker_key";

		} else if ("Feature Type".equals(dagAbbrev)) {
			cmd = "with ancestors as (select a.ancestor_term, t.term  " + 
					"  from term t, term_ancestor a " + 
					"  where t.vocab_name = 'Marker Category' " + 
					"    and t.term_key = a.term_key " + 
					"    and a.ancestor_term != 'other feature type' " + 
					"    and a.ancestor_term != 'other genome feature' " + 
					"    and a.ancestor_term != 'all feature types' " + 
					") " + 
					"select m.marker_key as feature_key, m.marker_subtype as term " + 
					"from marker m " + 
					"where organism = 'mouse' " + 
					"  and status != 'withdrawn' " + 
					"union " + 
					"select m.marker_key as feature_key, a.ancestor_term as term " + 
					"from marker m, ancestors a " + 
					"where m.marker_subtype = a.term " + 
					"  and organism = 'mouse' " + 
					"  and status != 'withdrawn'";

		} else if ("Disease".equals(dagAbbrev)) {
			// top of union is for mouse disease annotations (using data from HMDC tables), while the bottom
			// of the union includes data from human orthologs' disease annotations
			cmd = "with headers as (select distinct header from hdp_annotation) " + 
					"select distinct m.marker_key as feature_key, ha.header as term " + 
					" from hdp_genocluster_marker m,  " + 
					"  hdp_genocluster_genotype gg,  " + 
					"  hdp_genocluster_annotation ga,  " + 
					"  hdp_annotation ha, term t  " + 
					"where m.hdp_genocluster_key = gg.hdp_genocluster_key  " + 
					"  and gg.hdp_genocluster_key = ga.hdp_genocluster_key  " + 
					"  and ga.term_key = ha.term_key  " + 
					"  and gg.genotype_key = ha.genotype_key " + 
					"  and ga.term_key = t.term_key " + 
					"  and t.vocab_name = 'Disease Ontology' " + 
					"  and ga.qualifier_type is null " +
					"union " +
					"select distinct hcm2.marker_key as feature_key, h.header " + 
					"from marker_to_annotation t " + 
					"  inner join annotation a on (t.annotation_key = a.annotation_key and a.qualifier is null) " + 
					"  inner join homology_cluster_organism_to_marker hcm on (t.marker_key = hcm.marker_key) " + 
					"  inner join homology_cluster_organism ho on (hcm.cluster_organism_key = ho.cluster_organism_key and ho.organism = 'human') " + 
					"  inner join homology_cluster hc on (ho.cluster_key = hc.cluster_key and hc.source = 'Alliance Direct') " + 
					"  inner join homology_cluster_organism ho2 on (hc.cluster_key = ho2.cluster_key and ho2.organism = 'mouse') " + 
					"  inner join homology_cluster_organism_to_marker hcm2 on (ho2.cluster_organism_key = hcm2.cluster_organism_key) " + 
					"  inner join term_ancestor ta on (a.term_key = ta.term_key) " + 
					"  inner join headers h on (ta.ancestor_term = h.header) " + 
					"where t.annotation_type = 'DO/Human Marker'" ;
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
	
	// Retrieve synonyms, create documents, and index them.
	private void indexSynonyms() throws Exception {
		if ((features == null) || (features.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }

		Map<Integer,List<String>> mySynonyms = cacheSynonyms();
		
		for (Integer featureKey : mySynonyms.keySet()) {
			if (features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				for (String synonym : mySynonyms.get(featureKey)) {
					addDoc(buildDoc(feature, null, synonym, null, synonym, "Synonym", SYNONYM_WEIGHT));
					addDoc(buildDoc(feature, null, null, synonym, synonym, "Synonym", SYNONYM_WEIGHT));
				}
			}
		}
		logger.info("Indexed synonyms for " + mySynonyms.size() + " markers");
	}
	
	// Look up annotations and index according to the given SQL command.  Expected field names: feature_key, primary_id.
	// The given SQL command must order by the feature_key field to help identify duplicates (including ancestor terms).
	// VocabTermCache vtc is used to look up data for each vocab term.
	// The 4 weights are for the four different data pieces to be indexed.
	private void indexAnnotations (String dataType, String cmd, VocabTermCache vtc,
		Integer nameWeight, Integer idWeight, Integer synonymWeight) throws SQLException {
		
		logger.info(" - indexing " + dataType + "");

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
					String prefix = "";
					int directBoost = 1;	// slight preference for direct annotations over subterms
					if (!term.getPrimaryID().equals(termToIndex.getPrimaryID())) {
						prefix = "Subterm ";
						directBoost = 0;
					}
					
					String name = termToIndex.getTerm();
					if ((nameWeight != null) && (name != null) && (name.length() > 0)) {
						if (!name.equals(term.getTerm())) {
							addDoc(buildDoc(feature, null, null, name, term.getTerm() + " (" + name + ")", prefix + dataType, nameWeight + directBoost));
						} else {
							addDoc(buildDoc(feature, null, null, name, term.getTerm(), prefix + dataType, nameWeight + directBoost));
						}
						i++;
					}
				
					List<String> termIDs = termToIndex.getAllIDs();
					if ((idWeight != null) && (termIDs != null) && (termIDs.size() > 0)) {
						for (String id : termIDs) {
							addDoc(buildDoc(feature, id, null, null, term.getTerm() + " (ID: " + id + ")", prefix + dataType, idWeight + directBoost));
							i++;
						}
					}
				
					List<String> synonyms = termToIndex.getSynonyms();
					if ((synonymWeight != null) && (synonyms != null) && (synonyms.size() > 0)) {
						for (String synonym : synonyms) {
							addDoc(buildDoc(feature, null, null, synonym, term.getTerm() + " (synonym: " + synonym + ")", prefix + dataType, synonymWeight + directBoost));
							i++;
						}
					}
				}
			}
		}
		rs.close();
		
		logger.info(" - done with " + dataType + " (indexed " + i + " items)");
	}
	
	// Add to the index documents for the given marker and its directly-annotated set of EMAPS IDs (no
	// tracing up the DAG yet).
	private void indexExpressionChunk(Integer markerKey, Set<String> emapsTerms, VocabTermCache emapsCache, VocabTermCache emapaCache) {
		QSFeature feature = features.get(markerKey);
		
		// First, we'll identify the annotations we need to add, separating them into three additional sets.
		
		// EMAPA equivalents of direct annotations (ID to report : ID of annotation)
		Map<String,String> emapaDirect = new HashMap<String,String>();
		
		Map<String,String> emapsAncestors = new HashMap<String,String>();	// ancestor ID : ID of an annotation
		Map<String,String> emapaAncestors = new HashMap<String,String>();	// ancestor ID : ID of an annotation
		
		for (String emapsID : emapsTerms) {
			String directEmapa = emapsID.substring(0, emapsID.length() - 2).replaceFirst("EMAPS", "EMAPA");
			if (!emapaDirect.containsKey(directEmapa)) {
				emapaDirect.put(directEmapa, emapsID);
			}
			
			// To ensure that we follow up the DAG in a stage-aware manner, we traverse using the EMAPS vocabulary and then
			// look at the corresponding EMAPA terms.
			
			List<VocabTerm> emapsAncestorList = emapsCache.getAncestors(emapsCache.getTerm(emapsID).getTermKey());
			if (emapsAncestorList != null) {
				for (VocabTerm emapsAncestor : emapsAncestorList) {
					String emapsAncestorID = emapsAncestor.getPrimaryID();
					if (!emapsAncestors.containsKey(emapsAncestorID)) {
						emapsAncestors.put(emapsAncestorID, emapsID);
					}
					
					String emapaAncestorID = emapsAncestorID.substring(0, emapsAncestorID.length() - 2).replaceFirst("EMAPS", "EMAPA");
					if (!emapaAncestors.containsKey(emapaAncestorID)) {
						emapaAncestors.put(emapaAncestorID, emapsID);
					}
				}
			}
		}
		
		// So at this point, we have four maps of data to process:
		//   1. emapsTerms : direct EMAPS annotations, should add tiny boost to score
		//   2. emapaDirect : EMAPA equivalents of those direct EMAPS annotations, should add tiny boost to score
		//   3. emapsAncestors : EMAPS ancestors of those EMAPS annotations from #1, with source EMAPS ID so we can display the source
		//   4. emapaAncestors : EMAPA ancestors of those ancestors from #3, with source EMAPS ID so we can display the source
		// For each ancestor in #3 and #4, we should check that they're not also in the direct annotation maps (#1 and #2).  If so, we
		// can skip the ancestor data because it would just be skipped over anyway.
		
		// #1 : emapsTerms (see above)
		for (String emapsID : emapsTerms) {
			VocabTerm term = emapsCache.getTerm(emapsID);

			// only ID for EMAPS
			addDocUnchecked(buildDoc(feature, emapsID, null, null, term.getTerm() + " (" + emapsID + ")", " Expression", EMAP_ID_WEIGHT + 1));
		}
		
		// #2 : emapaDirect (see above)
		for (String emapaID : emapaDirect.keySet()) {
			VocabTerm term = emapaCache.getTerm(emapaID);
			VocabTerm source = emapsCache.getTerm(emapaDirect.get(emapaID));
			String stage = source.getPrimaryID().substring(source.getPrimaryID().length() - 2);
			String name = term.getTerm();
			List<String> synonyms = term.getSynonyms();
			
			// ID, term name, synonyms for EMAPA
			addDocUnchecked(buildDoc(feature, emapaID, null, null, "TS" + stage + ": " + name + " (ID: " + emapaID + ")", " Expression", EMAP_ID_WEIGHT + 1));
			addDocUnchecked(buildDoc(feature, null, null, name, "TS" + stage + ": " + name, " Expression", EMAP_NAME_WEIGHT + 1));
			if (synonyms != null) {
				for (String synonym : synonyms) {
					addDocUnchecked(buildDoc(feature, null, null, synonym, "TS" + stage + ": " + name, " Expression", EMAP_SYNONYM_WEIGHT + 1));
				}
			}
		}
		
		// #3 : emapsAncestors (see above)
		for (String emapsID : emapsAncestors.keySet()) {
			if (!emapsTerms.contains(emapsID)) {
				VocabTerm term = emapsCache.getTerm(emapsID);
				VocabTerm source = emapsCache.getTerm(emapsAncestors.get(emapsID));
				String stage = source.getPrimaryID().substring(source.getPrimaryID().length() - 2);
				String name = term.getTerm();
			
				// only ID for EMAPS
				addDocUnchecked(buildDoc(feature, emapsID, null, null, source.getTerm() + " (subterm of " + term.getPrimaryID() + ")", "Expression", EMAP_ID_WEIGHT));
			}
		}

		// #4 : emapaAncestors (see above)
		for (String emapaID : emapaAncestors.keySet()) {
			if (!emapaDirect.containsKey(emapaID)) {
				VocabTerm term = emapaCache.getTerm(emapaID);
				VocabTerm source = emapsCache.getTerm(emapaAncestors.get(emapaID));
				String stage = source.getPrimaryID().substring(source.getPrimaryID().length() - 2);
				String name = source.getTerm();
				List<String> synonyms = term.getSynonyms();
			
				// ID, term name, synonyms for EMAPA
				addDocUnchecked(buildDoc(feature, emapaID, null, null, "TS" + stage + ": " + name + " (subterm of " + term.getTerm() + ")", "Expression", EMAP_ID_WEIGHT));
				addDocUnchecked(buildDoc(feature, null, null, name, "TS" + stage + ": " + name + " (subterm of " + term.getTerm() + ")", "Expression", EMAP_NAME_WEIGHT));
				if (synonyms != null) {
					for (String synonym : synonyms) {
						addDocUnchecked(buildDoc(feature, null, null, synonym, "TS" + stage + ": " + name + " (subterm of " + term.getTerm() + ")", "Expression", EMAP_SYNONYM_WEIGHT + 1));
					}
				}
			}
		}
	}
	
	// Index the EMAPS IDs with expression detected either by classical or RNA-Seq experiments.  Assumes caches are loaded.
	// Also index corresponding EMAPA data.  We base the EMAPA data on the EMAPS relationships to ensure that we only 
	// follow stage-aware paths as we follow up the DAG.
	private void indexExpression() throws SQLException {
		logger.info(" - indexing Expression data");

		VocabTermCache emapsCache = new VocabTermCache("EMAPS", ex);
		VocabTermCache emapaCache = new VocabTermCache("EMAPA", ex);

		int minKey = 0;
		int maxKey = 0;
		
		String keyCmd = "select min(marker_key) as min_key, max(marker_key) as max_key from marker where organism = 'mouse'";
		ResultSet krs = ex.executeProto(keyCmd);
		if (krs.next()) {
			minKey = krs.getInt("min_key");
			maxKey = krs.getInt("max_key");
		}
		krs.close();
		
		// query returns marker keys and EMAPS IDs.  Remember that to convert from an EMAPS ID to its EMAPA
		// equivalent, just trim the rightmost two characters.
		String dataCmd = "select csm.marker_key, t.primary_id " + 
				"from expression_ht_consolidated_sample_measurement csm, " + 
				"expression_ht_consolidated_sample cs, term_emap e, term t " + 
				"where csm.consolidated_sample_key = cs.consolidated_sample_key " + 
				"and cs.emapa_key = e.emapa_term_key " + 
				"and cs.theiler_stage = e.stage::text " + 
				"and e.term_key = t.term_key " + 
				"and csm.level != 'Below Cutoff' " + 
				"and csm.marker_key >= <<start key>> " +
				"and csm.marker_key < <<end key>> " +
				"union " + 
				"select ers.marker_key as feaure_key, s.primary_id as term_id " + 
				"from expression_result_summary ers, term s " + 
				"where ers.is_expressed = 'Yes' " + 
				"and ers.structure_key = s.term_key " + 
				"and ers.marker_key >= <<start key>> " +
				"and ers.marker_key < <<end key>> " +
				"order by 1";

		int start = minKey;
		int chunk = 5000;
		int rows = 0;									// counter of annotation rows processed
		int emapsIndexed = 0;							// counter of EMAPS term/marker annotations indexed
		Integer lastMarkerKey = -1;						// last marker key to be processed
		Set<String> emapsIDs = new HashSet<String>();	// IDs to index for the curent marker


		while (start < maxKey) {
			int end = start + chunk;
			logger.info("Looking up Expression data for markers " + start + " to " + end);
			String cmd = dataCmd.replaceAll("<<start key>>", "" + start).replaceAll("<<end key>>", "" + end);

			ResultSet rs = ex.executeProto(cmd, cursorLimit);
			while (rs.next())  {  
				Integer markerKey = rs.getInt("marker_key");
			
				if (!markerKey.equals(lastMarkerKey)) {
					if (!emapsIDs.isEmpty()) {
						indexExpressionChunk(lastMarkerKey, emapsIDs, emapsCache, emapaCache);
						emapsIndexed = emapsIndexed + emapsIDs.size();
						emapsIDs.clear();
					}
					lastMarkerKey = markerKey;
				}
			
				rows++;
				String emapsID = rs.getString("primary_id");

				if (features.containsKey(markerKey) && emapsCache.containsKey(emapsID)) {
					emapsIDs.add(emapsID);
				} // if valid data row
			} // while processing rows

			rs.close();
			start = end;
			logger.info(" - finished");
		}
		
		// index data for last marker
		if (!emapsIDs.isEmpty()) {
			indexExpressionChunk(lastMarkerKey, emapsIDs, emapsCache, emapaCache);
			emapsIndexed = emapsIndexed + emapsIDs.size();
			emapsIDs.clear();
		}

		logger.info(" - processed " + rows + " Expression rows, indexed " + emapsIndexed + " unique marker/EMAPS pairs");
	}

	// Index the GO term annotations.  Assumes caches are loaded.
	private void indexGO() throws SQLException {
		String cmd = "select a.term_id as primary_id, m.marker_key as feature_key " + 
				"from annotation a, marker_to_annotation mta, marker m " + 
				"where a.annotation_key = mta.annotation_key " + 
				"and mta.marker_key = m.marker_key " + 
				"and a.annotation_type = 'GO/Marker' " + 
				"and m.organism = 'mouse' " + 
				"and a.qualifier is null " +
				"and a.dag_name = '<DAG>' " +
				"order by m.marker_key";

		indexAnnotations("Function", cmd.replaceAll("<DAG>", "Molecular Function"), new VocabTermCache("Function", ex),
			GO_NAME_WEIGHT, GO_ID_WEIGHT, GO_SYNONYM_WEIGHT);
		indexAnnotations("Component", cmd.replaceAll("<DAG>", "Cellular Component"), new VocabTermCache("Component", ex),
			GO_NAME_WEIGHT, GO_ID_WEIGHT, GO_SYNONYM_WEIGHT);
		indexAnnotations("Process", cmd.replaceAll("<DAG>", "Biological Process"), new VocabTermCache("Process", ex),
			GO_NAME_WEIGHT, GO_ID_WEIGHT, GO_SYNONYM_WEIGHT);
	}
	
	/* Load the features of the given type, cache them, generate initial documents and send them to Solr.
	 * Assumes cacheLocations has been run.
	 */
	private void buildInitialDocs() throws Exception {
		logger.info(" - loading markers");

		Map<Integer, Set<String>> goProcessFacetCache = this.getFacetValues("P");
		Map<Integer, Set<String>> goFunctionFacetCache = this.getFacetValues("F");
		Map<Integer, Set<String>> goComponentFacetCache = this.getFacetValues("C");
		Map<Integer, Set<String>> phenotypeFacetCache = this.getFacetValues("MP");
		Map<Integer, Set<String>> featureTypeFacetCache = this.getFacetValues("Feature Type");
		Map<Integer, Set<String>> diseaseFacetCache = this.getFacetValues("Disease");
		Map<Integer, Set<String>> expressionFacetCache = this.getFacetValues("Anatomy");
		
		String prefix = "Genome Feature ";
		features = new HashMap<Integer,QSFeature>();
		
		long padding = 0;	// amount of initial padding before sequence numbers should begin
		
		// only current mouse markers, and skip transgenes
		String cmd = "select m.marker_key as feature_key, m.primary_id, m.symbol, m.name, m.marker_subtype as subtype, " + 
				"s.by_symbol as sequence_num " +
			"from marker m, marker_sequence_num s " + 
			"where m.organism = 'mouse' " + 
			"and m.marker_key = s.marker_key " +
			"and m.status = 'official' " +
			"and m.marker_type != 'Transgene' " +
			"order by m.marker_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		while (rs.next())  {  
			i++;
			Integer featureKey = rs.getInt("feature_key");
			String symbol = rs.getString("symbol");
			String name = rs.getString("name");

			//--- prepare the new feature object
			
			QSFeature feature = new QSFeature(featureKey);
			features.put(featureKey, feature);
			
			feature.primaryID = rs.getString("primary_id");
			feature.sequenceNum = padding + rs.getLong("sequence_num");	
			feature.featureType = rs.getString("subtype");
			
			if (goProcessFacetCache.containsKey(featureKey)) { feature.goProcessFacets = goProcessFacetCache.get(featureKey); }
			if (goFunctionFacetCache.containsKey(featureKey)) { feature.goFunctionFacets = goFunctionFacetCache.get(featureKey); }
			if (goComponentFacetCache.containsKey(featureKey)) { feature.goComponentFacets = goComponentFacetCache.get(featureKey); }
			if (phenotypeFacetCache.containsKey(featureKey)) { feature.phenotypeFacets = phenotypeFacetCache.get(featureKey); }
			if (diseaseFacetCache.containsKey(featureKey)) { feature.diseaseFacets = diseaseFacetCache.get(featureKey); }
			if (featureTypeFacetCache.containsKey(featureKey)) { feature.markerTypeFacets = featureTypeFacetCache.get(featureKey); }
			if (expressionFacetCache.containsKey(featureKey)) { feature.expressionFacets = expressionFacetCache.get(featureKey); }

			//--- index the new feature object in basic ways (primary ID, symbol, name, etc.)
			
			addDocUnchecked(buildDoc(feature, feature.primaryID, null, null, feature.primaryID, prefix + "ID", PRIMARY_ID_WEIGHT));
			
			// Do not index transgene markers by symbol or synonyms.  (Their Tg alleles already get returned.)
			if (!"transgene".equalsIgnoreCase(feature.featureType)) {
				addDocUnchecked(buildDoc(feature, symbol, null, null, symbol, "Symbol", SYMBOL_WEIGHT));
			}

			// feature name
			addDocUnchecked(buildDoc(feature, null, null, name, name, "Name", NAME_WEIGHT));
		}

		rs.close();

		logger.info("done with basic data for " + i + " markers");
	}
	
	// Index mouse markers by genomic location.
	private void indexMouseLocations() throws Exception {
		if ((features == null) || (features.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }
		logger.info(" - loading mouse marker locations");

		String cmd = "select m.marker_key as feature_key, n.by_location " + 
				"from marker m, marker_sequence_num n " + 
				"where n.marker_key = m.marker_key " + 
				"and m.organism = 'mouse' " + 
				"and m.status = 'official' " + 
				"order by n.by_location";
		
		long seqNum = 0;	// sequential sequence number for markers by mouse genomic location
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		while (rs.next())  {  
			Integer featureKey = rs.getInt("feature_key");
			seqNum++;
			
			Long mouseStartCoord = null;
			Long mouseEndCoord = null;
			
			if (startCoord.containsKey(featureKey) && (startCoord.get(featureKey) != null)) {
				mouseStartCoord = Long.parseLong(startCoord.get(featureKey));
			}
			if (endCoord.containsKey(featureKey) && (endCoord.get(featureKey) != null)) {
				mouseEndCoord = Long.parseLong(endCoord.get(featureKey));
			}

			if (chromosome.containsKey(featureKey) && features.containsKey(featureKey)) {
				QSFeature feature = features.get(featureKey);
				addDocUnchecked(buildCoordinateDoc(feature, IndexConstants.QS_SEARCHTYPE_MOUSE_COORD,
					chromosome.get(featureKey), mouseStartCoord, mouseEndCoord, seqNum, "Location",
					"Overlaps specified coordinate range", LOCATION_WEIGHT)); 
			}
		}
		
		rs.close();
		logger.info("Indexed mouse locations for " + seqNum + " markers");
	}
	
	// Index mouse markers by genomic location of their human orthologs.
	private void indexHumanLocations() throws Exception {
		if ((features == null) || (features.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }
		logger.info(" - loading locations for human orthologs");

		String cmd = "with human_coords as ( " + 
				"select m.marker_key as human_marker_key, m.symbol, ml.chromosome, ml.start_coordinate, ml.end_coordinate, ml.build_identifier, " + 
				"sn.by_location, ml.location_type, ml.strand " + 
				"from marker m, marker_location ml, marker_sequence_num sn " + 
				"where m.marker_key = ml.marker_key  " + 
				"and m.organism = 'human'  " + 
				"and m.status = 'official'  " + 
				"and m.marker_key = sn.marker_key " + 
				") " + 
				"select m.marker_key, c.symbol, c.chromosome, c.start_coordinate, c.end_coordinate, c.strand, c.build_identifier " + 
				"from marker m, homology_cluster_organism_to_marker mm, " + 
				"  homology_cluster_organism mo, homology_cluster hc, " + 
				"  homology_cluster_organism ho, human_coords c, " + 
				"  homology_cluster_organism_to_marker hm " + 
				"where m.marker_key = mm.marker_key " + 
				"and m.organism = 'mouse' " + 
				"and mm.cluster_organism_key = mo.cluster_organism_key " + 
				"and mo.organism = 'mouse' " + 
				"and mo.cluster_key = hc.cluster_key " + 
				"and hc.source = 'Alliance Direct' " + 
				"and hc.cluster_key = ho.cluster_key " + 
				"and ho.cluster_organism_key = hm.cluster_organism_key " + 
				"and ho.organism = 'human' " + 
				"and hm.marker_key = c.human_marker_key " + 
				"order by c.by_location, c.location_type";

		long seqNum = 0;	// sequential sequence number for markers by human genomic location
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		
		Integer lastMarkerKey = -1;

		while (rs.next())  {  
			Integer featureKey = rs.getInt("marker_key");
			seqNum++;
			
			String humanSymbol = rs.getString("symbol");
			String humanChromosome = rs.getString("chromosome");
			Long humanStartCoord = rs.getLong("start_coordinate");
			Long humanEndCoord = rs.getLong("end_coordinate");
			String humanStrand = rs.getString("strand");
			String build = rs.getString("build_identifier");
			
			if (!featureKey.equals(lastMarkerKey) && features.containsKey(featureKey)) {
				lastMarkerKey = featureKey;

				String location = humanSymbol + ", Chr" + humanChromosome;
				if ((humanStartCoord != null) && (humanEndCoord != null) && (humanStrand != null) && (build != null)) {
					location = location + ":" + humanStartCoord + "-" + humanEndCoord + " (" + humanStrand + ") (" + build + ")";
				}
					
				QSFeature feature = features.get(featureKey);
				addDocUnchecked(buildCoordinateDoc(feature, IndexConstants.QS_SEARCHTYPE_HUMAN_COORD,
					humanChromosome, humanStartCoord, humanEndCoord, seqNum, location,
					"Human ortholog overlaps specified coordinate range", LOCATION_WEIGHT)); 
			}

		}
		
		rs.close();
		logger.info("Indexed " + seqNum + " human locations for mouse markers");
	}
	
	/* process the given feature type, loading data from the database, composing documents, and writing to Solr.
	 */
	private void processFeatureType() throws Exception {
		cacheLocations();
		buildInitialDocs();

		indexHumanLocations();
		indexMouseLocations();
		
		indexSynonyms();
		indexOrthologNomenclature();
		clearIndexedTermCache();		// only clear once all nomen done

		indexIDs();
		indexOrthologIDs();
		indexStrainGenes();
		indexProteoformIDs();
		clearIndexedTermCache();		// only clear once all IDs done
		
		indexProteinDomains();
		indexProteinFamilies();
		clearIndexedTermCache();		// only clear once all protein stuff done

		indexExpression();
		clearIndexedTermCache();		// only clear once all EMAPS expression data are done
		
		indexHumanDiseaseAnnotations();
		clearIndexedTermCache();		// only clear once all disease data done
		
		indexGO();
		clearIndexedTermCache();		// only clear once all GO data done
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		this.setSkipOptimizer(true);
		
		this.cacheHighLevelTerms();

		processFeatureType();
		
		// send any remaining documents and commit all the changes to Solr
		if (docs.size() > 0) {
			writeDocs(docs);
		}
		commit();
	}
	
	// private class for caching marker data that will be re-used across multiple documents
	private class QSFeature {
		private Integer featureKey;
		public String featureType;
		public String primaryID;
		public Long sequenceNum;
		public Set<String> goProcessFacets;
		public Set<String> goFunctionFacets;
		public Set<String> goComponentFacets;
		public Set<String> diseaseFacets;
		public Set<String> phenotypeFacets;
		public Set<String> markerTypeFacets;
		public Set<String> expressionFacets;

		// constructor
		public QSFeature(Integer featureKey) {
			this.featureKey = featureKey;
		}
		
		// compose and return a new SolrInputDocument including the fields for this feature
		public SolrInputDocument getNewDocument() {
			SolrInputDocument doc = new SolrInputDocument();

			if (this.primaryID != null) {
				doc.addField(IndexConstants.QS_PRIMARY_ID, this.primaryID);
			}

			if (this.featureType != null) { doc.addField(IndexConstants.QS_FEATURE_TYPE, this.featureType); }
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
			if (this.expressionFacets != null) { doc.addField(IndexConstants.QS_EXPRESSION_FACETS, this.expressionFacets); }

			return doc;
		}
	}
}