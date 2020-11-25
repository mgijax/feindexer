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
	private static int SECONDARY_ID_WEIGHT = 95;
	private static int SYMBOL_WEIGHT = 90;
	private static int NAME_WEIGHT = 85;
	private static int MARKER_SYMBOL_WEIGHT = 80;
	private static int MARKER_NAME_WEIGHT = 75;
	
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
	private int maxMarkerSeqNum = -1;				// maximum sequence num assigned to markers

	private Map<Integer,List<String>> synonyms;		// marker or allele key : list of synonyms

	private Map<Integer,Set<String>> orthologNomenOrg;		// marker key : set of "<organism & term type>:<term>"
	private Map<Integer,Set<String>> orthologNomen;			// marker key : set of just symbols, names, synonyms across all organisms
	
	private Map<Integer,Set<Integer>> goProcessAnnotations;		// marker key : annotated term key from GO Process DAG
	private Map<Integer,Set<Integer>> goFunctionAnnotations;	// marker key : annotated term key from GO Function DAG
	private Map<Integer,Set<Integer>> goComponentAnnotations;	// marker key : annotated term key from GO Component DAG
	private Map<Integer,Set<String>> mpAnnotations;				// marker or allele key : annotated MP term, ID, synonym
	private Map<Integer,Set<String>> hpoAnnotations;			// marker or allele key : annotated HPO term, ID, synonym
	private Map<Integer,Set<String>> diseaseAnnotations;		// marker or allele key : annotated DO term, ID, synonym
	private Map<Integer,Set<String>> proteinDomains;			// marker or allele key : annotated protein domains, ID (no synonyms)
	private Map<Integer,Set<String>> gxdAnnotations;			// marker or allele key : annotated EMAPA structure, ID, synonym
	private Map<Integer,Set<String>> gxdAnnotationsWithTS;		// marker or allele key : annotated EMAPA structure, ID, synonym, with TS prepended

	private Map<Integer,Set<Integer>> highLevelTerms;		// maps from a term key to the keys of its high-level ancestors
	
	private VocabTermCache goProcessCache;
	private VocabTermCache goFunctionCache;
	private VocabTermCache goComponentCache;
	
	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSFeatureBucketIndexerSQL() {
		super("qsFeatureBucket");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	// Add this doc to the batch we're collecting.  If the stack hits our threshold, send it to the server and reset it.
	private void addDoc(SolrInputDocument doc) {
		docs.add(doc);
		if (docs.size() >= solrBatchSize)  {
			writeDocs(docs);
			docs = new ArrayList<SolrInputDocument>();
		}
	}
	
	// Build and return a new SolrInputDocument with the given fields filled in.
	private SolrInputDocument buildDoc(QSFeature feature, String searchID, String searchTerm, String searchTermDisplay,
			String searchTermType, Integer searchTermWeight) {

		SolrInputDocument doc = feature.getNewDocument();
		if (searchID != null) { doc.addField(IndexConstants.QS_SEARCH_ID, searchID); }
		if (searchTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM, searchTerm); }
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
			cmd = "select i.marker_key as feature_key, i.acc_id, i.logical_db " + 
				"from marker m, marker_id i " + 
				"where m.marker_key = i.marker_key " + 
				"and m.status = 'official' " + 
				"and m.organism = 'mouse' " + 
				"and m.primary_id != i.acc_id " +
				"and i.private = 0";
		} else {
			cmd = "select i.allele_key as feature_key, i.acc_id, i.logical_db " + 
					"from allele_id i, allele a " + 
					"where i.private = 0 " +
					"and i.allele_key = a.allele_key " +
					"and a.primary_id != i.acc_id " +
					"and a.is_wild_type = 0";
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

	/* Cache protein domain annotations for markers, populating the proteinDomains object.  If feature type
	 * is for alleles, just skip this.
	 */
/*	private void cacheProteinDomains(String featureType) throws Exception {
		proteinDomains = new HashMap<Integer,Set<String>>();
		if (ALLELE.equals(featureType)) { return; }

		logger.info(" - caching protein domains for " + featureType);

		String cmd = "select distinct m.marker_key as feature_key, a.term_id as primary_id, a.term "
			+ "from annotation a, marker_to_annotation mta, marker m "
			+ "where a.annotation_key = mta.annotation_key "
			+ "and mta.marker_key = m.marker_key "
			+ "and a.vocab_name = 'InterPro Domains' ";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of terms processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("feature_key");
			
			if (!proteinDomains.containsKey(featureKey)) {
				proteinDomains.put(featureKey, new HashSet<String>());
			}
			proteinDomains.get(featureKey).add(rs.getString("primary_id"));
			proteinDomains.get(featureKey).add(rs.getString("term"));
		}
		rs.close();
		
		logger.info(" - cached " + ct + " protein domains for " + proteinDomains.size() + " " + featureType + "s");
	}
*/	
	/* Cache and return all synonyms for the given feature type (markers or alleles), populating the synonyms object.
	 * Note that this caching method is different from the others in that it does not directly modify the object's
	 * cache.  Instead, the map produced is returned.  This lets us also easily cache marker synonyms when dealing
	 * with alleles.
	 */
/*	private Map<Integer,List<String>> cacheSynonyms(String featureType) throws Exception {
		logger.info(" - caching synonyms for " + featureType);
		
		HashMap<Integer,List<String>> mySynonyms = new HashMap<Integer,List<String>>();
		
		String cmd;
		if (MARKER.equals(featureType)) {
			cmd = "select s.marker_key as feature_key, s.synonym " + 
					"from marker_synonym s";
		} else {
			cmd = "select s.allele_key as feature_key, s.synonym " + 
					"from allele_synonym s, allele a " +
					"where s.allele_key = a.allele_key " +
					"and a.is_wild_type = 0";
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
*/	
	/* Cache searchable nomenclature (symbol, name, synonyms) for orthologous markers in non-mouse organisms.
	 */
/*	private void cacheOrthologNomenclature(String featureType) throws Exception {
		orthologNomen = new HashMap<Integer,Set<String>>();
		orthologNomenOrg = new HashMap<Integer,Set<String>>();

		// For now, bail out if looking for allele data.  May need to add in the future.
		if (!MARKER.equals(featureType)) { return; }

		// Retrieve terms to be indexed with ortholog nomenclature.  Prefer old mouse nomen, then human,
		// then rat before all the others.  We can thus prioritize which organisms are shown for which
		// terms in the "best match" column in the fewi.
		String cmd = "select marker_key as feature_key, term, term_type, case " + 
				"  when term_type like 'old%' then 1 " + 
				"  when term_type like 'human%' then 2 " + 
				"  when term_type like 'rat%' then 3 " + 
				"  else 4 " + 
				"  end as preference " + 
				"			from marker_searchable_nomenclature  " + 
				"			where term_type in ( " + 
				"			    'old symbol', 'human name', 'human synonym', 'human symbol',  " + 
				"			    'related synonym', 'old name',  " + 
				"			    'rat symbol', 'rat synonym', 'chimpanzee symbol',  " + 
				"			    'cattle symbol', 'chicken symbol', 'dog symbol',  " + 
				"			    'rhesus macaque symbol', 'western clawed frog symbol',  " + 
				"			    'zebrafish symbol')  " + 
				"order by 1, 4, 2";
			
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;
		Integer lastFeatureKey = 0;
		Set<String> termsSeen = new HashSet<String>();

		while (rs.next())  {  
			Integer featureKey = rs.getInt("feature_key");

			// New feature?  If so, clear the cache of lowercase terms we've cached.
			if (!featureKey.equals(lastFeatureKey)) {
				termsSeen.clear();
				lastFeatureKey = featureKey;
			}

			String term = rs.getString("term");
			String termLower = term.toLowerCase();
			String termType = rs.getString("term_type");

			// First time we've seen this feature?  If so, add it to our maps.
			if (!orthologNomen.containsKey(featureKey)) {
				orthologNomen.put(featureKey, new HashSet<String>());
				orthologNomenOrg.put(featureKey, new HashSet<String>());
			}
			
			// If we've not already seen this particular string (case insensitive), we need to cache it.
			if (!termsSeen.contains(termLower)) {
				termsSeen.add(termLower);
				orthologNomen.get(featureKey).add(term);
				orthologNomenOrg.get(featureKey).add(termType + ":" + term);
				i++;
			}
		}
		rs.close();

		logger.info(" - cached " + i + " ortholog nomen terms for " + orthologNomen.size() + " " + featureType + "s");
	}
*/	
/*	private void addVocabAnnotations(SolrInputDocument doc, Integer featureKey, Map<Integer,Set<Integer>> annotatedTerms,
		VocabTermCache termCache, String idField, String termField, String synonymField, String definitionField,
		String ancestorField) {
			
		// set of ancestor keys already added as facet values
		Set<Integer> seenIt = new HashSet<Integer>();
					
		if (annotatedTerms.containsKey(featureKey)) {
			for (Integer termKey : annotatedTerms.get(featureKey).toArray(new Integer[0])) {
				VocabTerm term = termCache.getTerm(termKey);

				if (term != null) {
					// For this term, we must index not only it but also its ancestor terms.
					List<VocabTerm> toIndex = new ArrayList<VocabTerm>();
					toIndex.add(term);
					if (termCache.getAncestors(termKey) != null) {
						toIndex.addAll(termCache.getAncestors(termKey));
					}

					for (VocabTerm iTerm : toIndex) {
						if (iTerm.getTerm() != null) { doc.addField(termField, iTerm.getTerm()); }

						if (iTerm.getDefinition() != null) { doc.addField(definitionField, iTerm.getDefinition()); }

						if (iTerm.getAllIDs() != null) {
							for (String accID : iTerm.getAllIDs()) {
								doc.addField(idField, accID);
							}
						}

						if (iTerm.getSynonyms() != null) {
							for (String synonym : iTerm.getSynonyms()) {
								doc.addField(synonymField, synonym);
							}
						}
					}
					
					if ((ancestorField != null) && this.highLevelTerms.containsKey(termKey)) {
						for (Integer ancestorKey : this.highLevelTerms.get(termKey)) {
							if (!seenIt.contains(ancestorKey)) {
								VocabTerm ancestor = termCache.getTerm(ancestorKey);
								if (ancestor != null) {
									doc.addField(ancestorField, ancestor.getTerm());
								}
								seenIt.add(ancestorKey);
							}
						}
					}
				}
			}
		}
	}
*/			
	
	
	
	private void indexAnnotations(String featureType) throws Exception {
		if ((features == null) || (features.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }
		
	}
	
	private void indexSynonyms(String featureType) throws Exception {
		if ((features == null) || (features.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }

/*		Map<Integer,List<String>> markerSynonyms = null;
		if (ALLELE.equals(featureType)) {
			markerSynonyms = cacheSynonyms(MARKER);
		}
			// TODO -- marker synonyms for alleles

		// TBD
			if (synonyms.containsKey(featureKey)) {
				for (String s : synonyms.get(featureKey)) {
					doc.addField(IndexConstants.QS_SYNONYM, s);
				}
			}
*/			
	}

	
	
	
	
	
	/* Load the features of the given type, cache them, generate initial documents and send them to Solr.
	 * Assumes cacheLocations has been run for this featureType.
	 */
	private void buildInitialDocs(String featureType) throws Exception {
		logger.info(" - loading " + featureType + "s");

/*		Map<Integer, Set<String>> goProcessFacetCache = this.getAnnotationFacets();
		Map<Integer, Set<String>> goFunctionFacetCache = this.getAnnotationFacets();
		Map<Integer, Set<String>> goComponentFacetCache = this.getAnnotationFacets();
*/		
		this.features = new HashMap<Integer,QSFeature>();
		
		long padding = 0;	// amount of initial padding before sequence numbers should begin
		
		String cmd;
		if (MARKER.equals(featureType)) { 
			cmd = "select m.marker_key as feature_key, m.primary_id, m.symbol, m.name, m.marker_subtype as subtype, " + 
					"s.by_symbol as sequence_num " +
				"from marker m, marker_sequence_num s " + 
				"where m.organism = 'mouse' " + 
				"and m.marker_key = s.marker_key " +
				"and m.status = 'official'";
		} else {
			cmd = "select a.allele_key as feature_key, a.primary_id, a.symbol, a.name, a.allele_type as subtype, " + 
					"s.by_symbol as sequence_num, m.symbol as marker_symbol, m.name as marker_name " +
				"from allele a, allele_sequence_num s, marker_to_allele mta, marker m " + 
				"where a.allele_key = s.allele_key " +
				"and a.allele_key = mta.allele_key " +
				"and a.is_wild_type = 0 " +
				"and mta.marker_key = m.marker_key";

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
			
/*			if (goProcessFacetCache.containsKey(featureKey)) { feature.goProcessFacets = goProcessFacetCache.get(featureKey); }
			if (goFunctionFacetCache.containsKey(featureKey)) { feature.goFunctionFacets = goFunctionFacetCache.get(featureKey); }
			if (goComponentFacetCache.containsKey(featureKey)) { feature.goComponentFacets = goComponentFacetCache.get(featureKey); }
*/			
			// TODO -- add when constructing QSFeature object
			// public List<String> diseaseFacets;
			// public List<String> phenotypeFacets;
			// public List<String> markerTypeFacets;

			//--- index the new feature object in basic ways (primary ID, symbol, name, etc.)
			
			addDoc(buildDoc(feature, feature.primaryID, null, feature.primaryID, "ID", PRIMARY_ID_WEIGHT));
			addDoc(buildDoc(feature, null, feature.symbol, feature.symbol, "Symbol", SYMBOL_WEIGHT));
			addDoc(buildDoc(feature, null, feature.name, feature.name, "Name", NAME_WEIGHT));

			// For alleles, we also need to consider the nomenclature of each one's associated marker.
			if (ALLELE.equals(featureType)) { 
				String markerSymbol = rs.getString("marker_symbol");
				String markerName = rs.getString("marker_name");
				addDoc(buildDoc(feature, null, markerSymbol, markerSymbol, "Marker Symbol", MARKER_SYMBOL_WEIGHT));
				addDoc(buildDoc(feature, null, markerName, markerName, "Marker Name", MARKER_NAME_WEIGHT));
			}
		
/*			if (allIDs.containsKey(featureKey)) {
				for (String id : allIDs.get(featureKey)) {
					doc.addField(IndexConstants.QS_ACC_ID, id);
				}
			}
			
			if (proteinDomains.containsKey(featureKey)) {
				for (String domain : proteinDomains.get(featureKey).toArray(new String[0])) {
					doc.addField(IndexConstants.QS_PROTEIN_DOMAINS, domain);
				}
			}
			
			if (orthologNomen.containsKey(featureKey)) {
				for (String term : orthologNomen.get(featureKey).toArray(new String[0])) {
					doc.addField(IndexConstants.QS_ORTHOLOG_NOMEN, term);
				}
				for (String term : orthologNomenOrg.get(featureKey).toArray(new String[0])) {
					doc.addField(IndexConstants.QS_ORTHOLOG_NOMEN_ORG, term);
				}
			}
			
			if (MARKER.contentEquals(featureType)) {
				addVocabAnnotations(doc, featureKey, goFunctionAnnotations, goFunctionCache,
					IndexConstants.QS_FUNCTION_ANNOTATIONS_ID, IndexConstants.QS_FUNCTION_ANNOTATIONS_TERM,
					IndexConstants.QS_FUNCTION_ANNOTATIONS_SYNONYM, IndexConstants.QS_FUNCTION_ANNOTATIONS_DEFINITION,
					IndexConstants.QS_GO_FUNCTION_FACETS);
			
				addVocabAnnotations(doc, featureKey, goProcessAnnotations, goProcessCache,
					IndexConstants.QS_PROCESS_ANNOTATIONS_ID, IndexConstants.QS_PROCESS_ANNOTATIONS_TERM,
					IndexConstants.QS_PROCESS_ANNOTATIONS_SYNONYM, IndexConstants.QS_PROCESS_ANNOTATIONS_DEFINITION,
					IndexConstants.QS_GO_PROCESS_FACETS);
			
				addVocabAnnotations(doc, featureKey, goComponentAnnotations, goComponentCache,
					IndexConstants.QS_COMPONENT_ANNOTATIONS_ID, IndexConstants.QS_COMPONENT_ANNOTATIONS_TERM,
					IndexConstants.QS_COMPONENT_ANNOTATIONS_SYNONYM, IndexConstants.QS_COMPONENT_ANNOTATIONS_DEFINITION,
					IndexConstants.QS_GO_COMPONENT_FACETS);
			}
*/			
		}

		rs.close();

		logger.info("done with basic data for " + i + " " + featureType + "s");
	}
	
	/* Load annotations to vocab terms from the given feature type, then populate and return a cache such
	 * that each feature key maps to all the term keys that are annotated to it (excluding NOT annotations
	 * or those with a ND evidence code).
	 */
/*	private Map<Integer,Set<Integer>> cacheVocabAnnotations(String featureType, String annotType, String dagName) throws SQLException {
		Map<Integer,Set<Integer>> map = new HashMap<Integer,Set<Integer>>();
		
		if (MARKER.equals(featureType)) {
			String cmd = "select a.term_key, mta.marker_key " + 
				"from annotation a, term t, marker_to_annotation mta " +
				"where a.annotation_type = '" + annotType + "' " +
				"and a.object_type ilike '" + featureType + "' " + 
				"and a.term_key = t.term_key " + 
				"and t.display_vocab_name = '" + dagName + "' " +
				"and a.annotation_key = mta.annotation_key " + 
				"and (a.qualifier is null or a.qualifier != 'NOT') " + 
				"and (a.evidence_code is null or a.evidence_code != 'ND')";
			
			ResultSet rs = ex.executeProto(cmd, cursorLimit);
			while (rs.next() ) {
				Integer featureKey = rs.getInt("marker_key");
				if (!map.containsKey(featureKey)) {
					map.put(featureKey, new HashSet<Integer>());
				}
				map.get(featureKey).add(rs.getInt("term_key"));
			}
			rs.close();
		}
		
		logger.info("Cached " + annotType + " annotations for " + map.size() + " " + featureType + "s");
		return map;
	}
*/	
	/* process the given feature type, loading data from the database, composing documents, and writing to Solr.
	 */
	private void processFeatureType(String featureType) throws Exception {
		logger.info("beginning " + featureType);
		
		cacheLocations(featureType);
		buildInitialDocs(featureType);
		indexIDs(featureType);

/*		indexSynonyms(featureType);
		indexOrthologNomenclature(featureType);
		indexProteinDomains(featureType);
		
		this.synonyms = cacheSynonyms(featureType);
		cacheOrthologNomenclature(featureType);
		cacheProteinDomains(featureType);

		// need to do annotations
		
		if (MARKER.equals(featureType)) {
			this.goFunctionAnnotations = cacheVocabAnnotations(featureType, "GO/Marker", "Function");
			this.goProcessAnnotations = cacheVocabAnnotations(featureType, "GO/Marker", "Process");
			this.goComponentAnnotations = cacheVocabAnnotations(featureType, "GO/Marker", "Component");
		} else {
			this.goFunctionAnnotations = new HashMap<Integer,Set<Integer>>();
			this.goProcessAnnotations = new HashMap<Integer,Set<Integer>>();
			this.goComponentAnnotations = new HashMap<Integer,Set<Integer>>();
		}

		processFeatures(featureType);
*/		
		logger.info("finished " + featureType);
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// cache vocabulary term data
		this.goFunctionCache = new VocabTermCache("Function", ex);
		this.goProcessCache = new VocabTermCache("Process", ex);
		this.goComponentCache = new VocabTermCache("Component", ex);
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