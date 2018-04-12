package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.DistinctSolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;
import org.jax.mgi.shr.fe.query.SolrLocationTranslator;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.mgi.shr.jsonmodel.GridMarker;

/* Is: parent class of the various HMDC-related indexers (Hdp*)
 * Has: knowledge of how to produce various temp tables, mappings, and such
 *   that are useful across the suite of HMDC-related indexers
 */
public abstract class HdpIndexerSQL extends Indexer {
	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	// single instance shared for converting objects into JSON
	protected ObjectMapper mapper = new ObjectMapper();
	
	protected int cursorLimit = 10000;				// number of records to retrieve at once

	protected int uniqueKey = 0;					// (incremental) unique key for index documents

	protected String disease = "Disease Ontology";					// vocab name for disease terms
	protected String mp = "Mammalian Phenotype";	// vocab name for mouse phenotype terms
	protected String hpo = "Human Phenotype Ontology";	// vocab name for human phenotype terms

	protected int dbChunkSize = 30000;				// number of annotations to process in each batch
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	protected Map<Integer,Integer> homologyMap = null;			// marker key -> gridcluster key
	protected Map<Integer, String> markerSymbolMap = null;		// marker key -> marker symbol
	protected Map<Integer, String> markerNameMap = null;		// marker key -> marker name
	protected Map<Integer, String> markerIdMap = null;			// marker key -> primary marker ID
	protected Map<String, Set<String>> markerAllIdMap = null;	// marker key -> set of marker IDs
	protected Set<Integer> mouseMarkers = null;					// marker keys for mouse markers
	protected Set<Integer> humanMarkers = null;					// marker keys for human markers
	protected Map<String,Set<String>> markerSynonymMap = null;	// marker key -> marker synonyms 
	protected Map<String,Set<String>> markerCoordinates = null;	// marker key -> coordinates
	protected Map<Integer,Set<String>> markerFeatureTypes = null;	// marker key -> set of feature types
	protected Map<Integer,Set<Integer>> markerOrthologs = null;		// marker key -> set of ortholog marker keys
	protected Map<Integer,Integer> markerToHomologyCluster = null;	// marker key -> hybrid homology cluster key

	protected Map<String,Set<String>> markersPerDisease = null;	// disease ID -> marker keys
	protected Map<String,Set<String>> headersPerTerm = null;	// disease ID -> header terms
	protected Map<String,Integer> refCountPerDisease = null;	// disease ID -> count of refs
	protected Map<String,Integer> modelCountPerDisease = null;	// disease ID -> count of models

	protected Map<String,Set<String>> termAlternateIds = null;	// term ID -> alternate term IDs
	protected Set<Integer> diseaseTerms = null;					// set of term keys for disease terms
	protected Set<Integer> mpTerms = null;						// set of term keys for MP terms
	protected Set<Integer> hpoTerms = null;						// set of term keys for HPO terms
	protected Map<Integer,String> terms = null;					// term key -> term
	protected Map<Integer,String> termIds = null;				// term key -> primary term ID
	protected Map<Integer,Set<Integer>> relatedAnnotations = null;	// annot key -> set of related annot keys
	protected Map<Integer,Integer> annotationTermKeys = null;		// annot key -> term key of annotation
	protected Set<Integer> notAnnotations = null;				// set of annotations keys with NOT qualifiers
	protected Map<Integer,Set<Integer>> termAncestors = null;	// term key -> set of its ancestor term keys
	protected Map<Integer,String> mpHeaderText = null;			// MP header term key -> string to display

	protected Map<Integer,List<Integer>> diseaseToHpo = null;		// DO term key -> List of HPO term keys
	protected Map<Integer,Set<Integer>> hpoHeaderToMp = null;	// HPO header key -> set of MP header keys

	protected Map<Integer,Set<Integer>> expressedComponents = null;	// source marker key -> expressed marker key
	
	/* We use the term "basic search units" for the grid, referring to the basic unit that we
	 * are searching for -- genoclusters for mouse data and marker/disease pairs for human
	 * data.  If our search matches a BSU, then all of its annotations are returned.  If a BSU
	 * was not matched by the search, then none of its data are returned.
	 */
	protected Map<Integer,BSU> bsuMap = null;				// maps BSU key to its cached data
	Map<Integer,Map<Integer,Integer>> humanBsuMap = null;	// marker key -> disease key -> BSU key
	Map<Integer,Map<Integer,Integer>> mouseBsuMap = null;	// genocluster key -> gridcluster key -> BSU key

	Map<Integer,Integer> genotypeToGenocluster = null;		// genotype key -> genocluster key
	
	Map<Integer,String> gcToHumanMarkers = null;	// maps gridcluster key to human marker data as JSON
	Map<Integer,String> gcToMouseMarkers = null;	// maps gridcluster key to mouse marker data as JSON
	Map<Integer,String> allelePairs = null;			// maps genocluster key to allele pair data
	Set<Integer> conditionalGenoclusters = null;	// set of conditional genocluster keys

	// genocluster key to set of corresponding grid cluster keys
	protected Map<Integer,Set<Integer>> genoclusterToGridcluster = null;
	
	// gridcluster key to feature types for mouse markers in the cluster
	protected Map<String,Set<String>> featureTypeMap = null;

	// maps from each disease and phenotype term key to its DAG-based sequence number
	protected Map<Integer,Integer> dagTermSortMap = null;

	// maps from each disease and phenotype term to its smart-alpha sequence number
	protected Map<String,Integer> termSortMap = null;

	// one higher than the largest sequence number in 'termSortMap'
	protected int maxTermSeqNum = 0;

	// mapping from each disease and phenotype ID to the corresponding term's synonyms
	protected Map<String,Set<String>> termSynonymMap = null;

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	protected HdpIndexerSQL(String solrIndexName) {
		super(solrIndexName);
	}

	/*------------------------------------------------------*/
	/*--- methods for dealing with data cached in memory ---*/
	/*------------------------------------------------------*/

	/* cache the mapping between genotypes and genoclusters
	 */
	protected void cacheGenotypeToGenocluster() throws Exception {
		if (genotypeToGenocluster != null) { return; }
		
		logger.info("retrieving genotype/genocluster pairs");
		Timer.reset();
		
		// We assume each genotype is part of only one genocluster.
		genotypeToGenocluster = new HashMap<Integer,Integer>();
		
		String genotypeQuery = "select genotype_key, hdp_genocluster_key "
			+ "from hdp_genocluster_genotype";
		
		ResultSet rs = ex.executeProto(genotypeQuery, cursorLimit);
		
		while (rs.next()) {
			genotypeToGenocluster.put(rs.getInt("genotype_key"), rs.getInt("hdp_genocluster_key"));
		}
		rs.close();

		logger.info("finished retrieving genoclusters for " + genotypeToGenocluster.size() + " genotypes " + Timer.getElapsedMessage());
	}
	
	/* retrieve the genocluster key for the given genotype key
	 */
	protected Integer getGenocluster(Integer genotypeKey) throws Exception {
		if (genotypeToGenocluster == null) { cacheGenotypeToGenocluster(); }
		if (genotypeToGenocluster.containsKey(genotypeKey)) {
			return genotypeToGenocluster.get(genotypeKey);
		}
		return null;
	}
	
	/* retrieve the mapping from each (Integer) term key to a Set of its (String) ancestor term keys
	 */
	protected Map<Integer,Set<Integer>> getTermAncestors() throws Exception {
		if (termAncestors != null) { return null; }

		logger.info("retrieving ancestors of terms");
		Timer.reset();

		String ancestorQuery = "select ta.term_key, ta.ancestor_term_key "
				+ "from term_ancestor ta "
				+ "where exists (select 1 from term t "
				+ "  where t.vocab_name in ('Disease Ontology', 'Mammalian Phenotype', 'Human Phenotype Ontology') "
				+ "    and t.term_key = ta.term_key)";

		termAncestors = new HashMap<Integer,Set<Integer>>();

		ResultSet rs = ex.executeProto(ancestorQuery, cursorLimit);
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");

			if (!termAncestors.containsKey(termKey)) {
				termAncestors.put(termKey, new HashSet<Integer>());
			}
			termAncestors.get(termKey).add(rs.getInt("ancestor_term_key"));
		}
		rs.close();

		logger.info("finished retrieving ancestors for " + termAncestors.size() + " terms " + Timer.getElapsedMessage());
		return termAncestors;
	}

	protected Map<Integer,Set<Integer>> childToParents = null;	// child term key -> parent term keys
	protected Map<Integer,Set<Integer>> ancestors = null;		// child term key -> all ancestors

	protected void getTermRelationships() throws Exception {
		if (childToParents != null) { return; }

		logger.info("retrieving parent/child term relationships");
		Timer.reset();

		String parentQuery = "select ta.term_key as parent_key, "
				+ "  ta.child_term_key as child_key "
				+ "from term_child ta "
				+ "where exists (select 1 from term t "
				+ "  where t.vocab_name in ('Disease Ontology', 'Mammalian Phenotype', 'Human Phenotype Ontology') "
				+ "    and t.term_key = ta.term_key)";

		childToParents = new HashMap<Integer,Set<Integer>>();
		ancestors = new HashMap<Integer,Set<Integer>>();

		ResultSet rs = ex.executeProto(parentQuery);
		while (rs.next()) {
			Integer childKey = rs.getInt("child_key");

			if (!childToParents.containsKey(childKey)) {
				childToParents.put(childKey, new HashSet<Integer>());
			}
			childToParents.get(childKey).add(rs.getInt("parent_key"));
		}
		rs.close();

		logger.info("finished retrieving parent/child relationships for " + childToParents.size() + " child terms");
	}

	protected Set<Integer> getTermAncestors (Integer termKey) throws Exception {
		if (childToParents == null) { getTermRelationships(); }

		// already calculated ancestors for this term?  return them.
		if (ancestors.containsKey(termKey)) { return ancestors.get(termKey); }

		// no parents for this term?  It's a root, so return null.
		if (!childToParents.containsKey(termKey)) { return null; }

		Set<Integer> myAncestors = new HashSet<Integer>();

		for (Integer parent : childToParents.get(termKey)) {
			myAncestors.add(parent);
			Set<Integer> parentsAncestors = getTermAncestors(parent);
			if (parentsAncestors != null) {
				myAncestors.addAll(parentsAncestors);
			}
		}
		ancestors.put(termKey, myAncestors);

		return myAncestors;
	}

	/* iterate over the ancestors of the given term and collect terms, synonyms, and IDs in a
	 * Set to be returned, as specified by the parameters.  Returns null if 'termKey' is
	 * unknown or has no ancestors.
	 */
	protected Set<String> getTermAncestorData(Integer termKey, boolean getTerms,
			boolean getSynonyms, boolean getIds) throws Exception {
		Set<Integer> ancestors = getTermAncestors(termKey);
		if (ancestors == null) { return null; }

		Set<String> out = new HashSet<String>();
		for (Integer ancestorTermKey : ancestors) {
			if (getTerms) {
				String term = getTerm(ancestorTermKey);
				if (term != null) { out.add(term); }
			}

			String termId = getTermId(ancestorTermKey);

			if (getSynonyms && (termId != null)) {
				Set<String> synonyms = getTermSynonyms(termId);
				if (synonyms != null) { out.addAll(synonyms); }
			}

			if (getIds && (termId != null)) {
				out.add(termId);
				Set<String> altIds = getAlternateTermIds(termId);
				if (altIds != null) { out.addAll(altIds); }
			}
		}
		return out;
	}

	/* get the primary and secondary IDs associated with the ancestors of the given term key,
	 * or null if there are none or if the term key is unknown
	 */
	protected Set<String> getTermAncestorIDs(Integer termKey) throws Exception {
		return getTermAncestorData(termKey, false, false, true);
	}

	/* get the terms and their synonyms that are ancestors of the given term key, or
	 * null if there are none or if the term key is unknown
	 */
	protected Set<String> getTermAncestorText(Integer termKey) throws Exception {
		return getTermAncestorData(termKey, true, true, false);
	}

	/* retrieve the mapping from each (Integer) marker key to a Set of its (String) feature types,
	 * for those markers with non-null feature types
	 */
	protected Map<Integer,Set<String>> getMarkerFeatureTypes() throws Exception {
		if (markerFeatureTypes == null) {
			logger.info("retrieving feature types for markers");
			Timer.reset();

			String featureTypeQuery = "select marker_key, marker_subtype "
					+ "from marker "
					+ "where marker_subtype is not null";

			markerFeatureTypes = new HashMap<Integer,Set<String>>();

			ResultSet rs = ex.executeProto(featureTypeQuery, cursorLimit);
			while (rs.next()) {
				Integer markerKey = rs.getInt("marker_key");

				if (!markerFeatureTypes.containsKey(markerKey)) {
					markerFeatureTypes.put(markerKey, new HashSet<String>());
				}
				markerFeatureTypes.get(markerKey).add(rs.getString("marker_subtype"));
			}
			rs.close();

			logger.info("finished retrieving feature types for " + markerFeatureTypes.size() + " markers " + Timer.getElapsedMessage());
		}
		return markerFeatureTypes;
	}

	/* get the feature types for the marker with the specified key
	 */
	protected Set<String> getMarkerFeatureTypes(Integer markerKey) throws Exception {
		if (markerFeatureTypes == null) { getMarkerFeatureTypes(); }
		if (markerFeatureTypes.containsKey(markerKey)) {
			return markerFeatureTypes.get(markerKey);
		}
		return null;
	}

	/* retrieve the mapping from each phenotype and disease ID to the alternate
	 * IDs for the corresponding term
	 */
	protected Map<String,Set<String>> getAlternateTermIds() throws Exception {
		if (termAlternateIds == null) {
			logger.info("retrieving alternate IDs for terms");
			Timer.reset();

			String termIdQuery="select t.primary_id as term_id, ti.acc_id as alt_id "
					+ "from term t, term_id ti "
					+ "where t.term_key = ti.term_key "
					+ "  and t.vocab_name in ('Mammalian Phenotype', 'Disease Ontology', 'Human Phenotype Ontology') ";

			termAlternateIds = populateLookup(termIdQuery,"term_id","alt_id","alternate IDs to term IDs");
			
			// For searching, add the alternate form for DO IDs
			for (String termId : termAlternateIds.keySet()) {
				Set<String> altIds = termAlternateIds.get(termId);
				List<String> newIds = new ArrayList<String>();
				// This adds the Number part of OMIM to the alt ids also for searching
				for (String altId : altIds) {
					if (altId.startsWith("OMIM:")) {
						newIds.add(altId.replaceFirst("OMIM:", ""));
					}
				}
				altIds.addAll(newIds);
			}

			logger.info("finished retrieving alternate IDs" + Timer.getElapsedMessage());
		}
		return termAlternateIds;
	}
	
	/* get the alternate term IDs for the term identified by the given primary ID
	 */
	protected Set<String> getAlternateTermIds(String termId) throws Exception {
		if (termAlternateIds == null) { getAlternateTermIds(); }
		if (termAlternateIds.containsKey(termId)) {
			return termAlternateIds.get(termId);
		}
		return null;
	}
	
	protected List<String> getDiseaseDoIds(String termId) throws Exception {
		List<String> doIds = new ArrayList<String>();
		
		Set<String> altIds = getAlternateTermIds(termId);
		if (altIds == null) { return doIds; }
		
		for (String altId : altIds) {
			if (altId.startsWith("DOID:")) {
				doIds.add(altId);
			}
		}
		
		Collections.sort(doIds);
		
		return doIds;
	}

	protected List<String> getDiseaseOmimIds(String termId) throws Exception {
		List<String> omimIds = new ArrayList<String>();
		
		Set<String> altIds = getAlternateTermIds(termId);
		if (altIds == null) { return omimIds; }
		
		for (String altId : altIds) {
			if (altId.startsWith("OMIM:")) {
				omimIds.add(altId);
			}
		}
		
		Collections.sort(omimIds);
		
		return omimIds;
	}

	/* get the alternate term IDs for the term identified by the given term key
	 */
	protected Set<String> getAlternateTermIds(Integer termKey) throws Exception {
		if (termKey == null) { return null; }
		String termId = getTermId(termKey);
		if (termId == null) { return null; }
		return getAlternateTermIds(termId);
	}

	/* retrieve the mapping from each disease and phenotype ID to the
	 * corresponding term's synonyms
	 */
	protected Map<String,Set<String>> getTermSynonymMap() throws Exception {
		if (termSynonymMap == null) {
			logger.info("retrieving synonyms for diseases and phenotypes");
			Timer.reset();

			String termSynonymQuery="select t.primary_id term_id,ts.synonym "+
					"from term t,term_synonym ts "+
					"where t.term_key=ts.term_key " +
					"and t.vocab_name in ('Disease Ontology','Mammalian Phenotype', 'Human Phenotype Ontology') ";
			termSynonymMap = populateLookup(termSynonymQuery,"term_id","synonym","disease + MP synonyms to term IDs");

			logger.info("finished retrieving synonyms for diseases and phenotypes" + Timer.getElapsedMessage());
		}
		return termSynonymMap;
	}

	/* get the synonyms for the given disease or phenotype term ID
	 */
	protected Set<String> getTermSynonyms(String termID) throws Exception {
		if (termSynonymMap == null) { getTermSynonymMap(); }
		if (termSynonymMap.containsKey(termID)) {
			return termSynonymMap.get(termID);
		}
		return null;
	}

	/* get the synonyms for the given disease or phenotype term key
	 */
	protected Set<String> getTermSynonyms(Integer termKey) throws Exception {
		String termId = this.getTermId(termKey);
		if (termId == null) { return null; }
		return getTermSynonyms(termId);
	}

	/* retrieve the mapping from each (String) marker key to the marker's synonyms, including
	 * the synonyms for all orthologous markers.
	 */
	protected Map<String, Set<String>> getMarkerSynonymMap() throws Exception {
		if (markerSynonymMap == null) {
			logger.info("retrieving synonyms for markers");
			Timer.reset();

			String markerSynonymQuery="select distinct msn.marker_key, msn.term synonym " +
					"from marker_searchable_nomenclature msn " +
					"where msn.term_type in ('old symbol','old name','synonym') " +
					"UNION " +
					"select distinct ms.marker_key, ms.synonym " +
					"from marker_synonym ms ";
			markerSynonymMap = populateLookup(markerSynonymQuery,"marker_key","synonym","marker keys to synonyms");

			logger.info("finished retrieving synonyms for markers" + Timer.getElapsedMessage());
		}
		return markerSynonymMap;
	}

	/* get the synonyms for the given marker key, including synonyms for orthologous markers.
	 */
	protected Set<String> getMarkerSynonyms(Integer markerKey) throws Exception {
		if (markerSynonymMap == null) { getMarkerSynonymMap(); }
		String markerKeyString = markerKey.toString();
		if (markerSynonymMap.containsKey(markerKeyString)) {
			return markerSynonymMap.get(markerKeyString);
		}
		return null;
	}

	/* retrieve the marker keys for mouse orthologs of the given human marker
	 */
	protected Set<Integer> getMouseOrthologs (Integer humanMarkerKey) throws Exception {
		Set<Integer> orthologs = getMarkerOrthologs(humanMarkerKey);
		if (orthologs == null) { return null; }
		
		Set<Integer> subset = new HashSet<Integer>();
		for (Integer otherKey : orthologs) {
			if (this.isMouse(otherKey)) {
				subset.add(otherKey);
			}
		}
		return subset;
	}
	
	/* retrieve the marker keys for human orthologs of the given mouse marker
	 */
	protected Set<Integer> getHumanOrthologs (Integer mouseMarkerKey) throws Exception {
		Set<Integer> orthologs = getMarkerOrthologs(mouseMarkerKey);
		if (orthologs == null) { return null; }
		
		Set<Integer> subset = new HashSet<Integer>();
		for (Integer otherKey : orthologs) {
			if (this.isHuman(otherKey)) {
				subset.add(otherKey);
			}
		}
		return subset;
	}
	
	/* get any single-token synonyms for the given marker key (no whitespace), including synonyms for
	 * orthologous markers.  If 'includeOrthologs' is true, then also include any single-token synonyms
	 * for mouse and human markers in the orthology class.
	 */
	protected Set<String> getMarkerSingleTokenSynonyms(Integer markerKey, boolean includeOrthologs) throws Exception {
		Set<String> subset = new HashSet<String>();

		// first add single-token synonyms for this marker, if there are any
		Set<String> allSynonyms = getMarkerSynonyms(markerKey);
		if (allSynonyms != null) {
			for (String synonym : allSynonyms) {
				if (!synonym.contains(" ")) {
					subset.add(synonym);
				}
			}
		}
		
		if (includeOrthologs) {
			Set<Integer> orthologMarkerKeys = new HashSet<Integer>();		// mouse + human orthologs
			Set<Integer> mouseOrthologKeys = getMouseOrthologs(markerKey);	// just mouse
			Set<Integer> humanOrthologKeys = getHumanOrthologs(markerKey);	// just human

			if (mouseOrthologKeys != null) { orthologMarkerKeys.addAll(mouseOrthologKeys); }
			if (humanOrthologKeys != null) { orthologMarkerKeys.addAll(humanOrthologKeys); }
			
			for (Integer orthologMarker : orthologMarkerKeys) {
				subset.addAll(getMarkerSingleTokenSynonyms(orthologMarker, false));
			}
		}

		return subset;
	}

	/* retrieve the mapping from each (String) marker key to the spatial strings for its coordinates
	 */
	protected Map<String, Set<String>> getMarkerCoordinateMap() throws Exception {
		if (markerCoordinates == null) {
			logger.info("building map of marker coordinates to marker keys");
			Timer.reset();

			// load all mouse and human marker locations to be encoded for Solr spatial queries
			String markerLocationQuery="select distinct m.marker_key, ml.chromosome, "
					+ "  ml.strand, ml.start_coordinate, ml.end_coordinate "
					+ "from marker_location ml, "
					+ "  marker m "
					+ "where m.marker_key=ml.marker_key "
					+ "  and ml.sequence_num=1 "
					+ "  and ml.start_coordinate is not null "
					+ "  and ml.chromosome != 'UN' ";

			markerCoordinates = new HashMap<String,Set<String>>();
			ResultSet rs = ex.executeProto(markerLocationQuery, cursorLimit);

			while (rs.next()) {
				String markerKey = rs.getString("marker_key");
				String chromosome = rs.getString("chromosome");
				Long start = rs.getLong("start_coordinate");
				Long end = rs.getLong("end_coordinate");
				if(!markerCoordinates.containsKey(markerKey)) {
					markerCoordinates.put(markerKey, new HashSet<String>());
				}
				// NOTE: we are ignoring strand at this time. We can support searching both (in theory), so we will just treat everything as positive for now.
				// AS OF 2013/08/06 -kstone
				String spatialString = SolrLocationTranslator.getIndexValue(chromosome,start,end,true);

				// only add locations for chromosomes we can map. The algorithm will ignore any weird values
				if(spatialString != null && !spatialString.equals("")) {
					markerCoordinates.get(markerKey).add(spatialString);
				}
			}
			rs.close();
			logger.info("done retrieving marker locations" + Timer.getElapsedMessage());
		}
		return markerCoordinates;
	}

	/* get the spatial strings for the coordinates of the given marker key
	 */
	protected Set<String> getMarkerCoordinates(Integer markerKey) throws Exception {
		if (markerCoordinates == null) { getMarkerCoordinateMap(); }
		String markerKeyString = markerKey.toString();
		if (markerCoordinates.containsKey(markerKeyString)) {
			return markerCoordinates.get(markerKeyString);
		}
		return null;
	}

	/* add the coordinates for the given marker to the appropriate bin (human or mouse) 
	 * in the given document.  Optionally include coordinates for the marker's orthologs.
	 */
	protected void addMarkerCoordinates(DistinctSolrInputDocument doc, Integer markerKey, boolean includeOrthologs) throws Exception {
		if (markerKey == null) { return; }
		
		boolean isHumanMarker = isHuman(markerKey);
		
		// add the marker itself
		if (isHumanMarker) {
			doc.addAllDistinct(DiseasePortalFields.HUMAN_COORDINATE, getMarkerCoordinates(markerKey));
		} else {
			doc.addAllDistinct(DiseasePortalFields.MOUSE_COORDINATE, getMarkerCoordinates(markerKey));
		}
		
		// if we need to add coordinates for orthologs, look up the orthologous markers and
		// add the coordinates for each.  Note that we do not add orthologs from the same organism as
		// the specified marker -- only orthologs from a different organism.
		if (includeOrthologs) {
			Set<Integer> myOrthologs = getMarkerOrthologs(markerKey);
			if (myOrthologs != null) {
				for (Integer orthologKey : myOrthologs) {
					if ((isHumanMarker && !isHuman(orthologKey)) || (!isHumanMarker && isHuman(orthologKey))) {
						addMarkerCoordinates(doc, orthologKey, false);
					}
				}
			}
		}
	}
	
	/* build the mapping of term keys to a sqeuence number for each one, where the terms
	 * are sorted in order of a DAG-based traversal (to group terms together that are
	 * similar biologically)
	 */
	private void cacheDagTermOrdering() throws SQLException {
		if (dagTermSortMap != null) { return; }
		
		logger.info("getting DAG-based sequence numbers for terms");
		Timer.reset();

		dagTermSortMap = new HashMap<Integer,Integer>();
		
		String query = "select distinct t.term_key, s.by_dfs "
			+ "from term t, term_sequence_num s "
			+ "where t.vocab_name in ('Disease Ontology','Mammalian Phenotype', 'Human Phenotype Ontology') "
			+ "  and t.term_key = s.term_key";
		ResultSet rs = ex.executeProto(query, cursorLimit);

		while(rs.next()) {
			dagTermSortMap.put(rs.getInt("term_key"), rs.getInt("by_dfs"));
		}
		rs.close();
		logger.info("finished getting DAG sequence numbers for " + dagTermSortMap.size() + " diseases and phenotypes " + Timer.getElapsedMessage());
	}
	
	/* get the DAG-based sequence number for the term with the given key, allowing
	 * biologically-similar terms to be grouped together
	 */
	protected int getDagSequenceNum(int termKey) throws SQLException {
		if (dagTermSortMap == null) { cacheDagTermOrdering(); }
		if (!dagTermSortMap.containsKey(termKey)) {
			// if unknown term (should not happen), add to end
			dagTermSortMap.put(termKey, dagTermSortMap.size() + 1);
		}
		return dagTermSortMap.get(termKey);
	}
	
	/* build the mapping from each disease and phenotype term to its smart-alpha
	 * sequence number and remember the next sequence number to be assigned.
	 */
	private Map<String,Integer> buildTermSortMap() throws SQLException {
		logger.info("calculating sequence numbers for diseases and phenotypes");
		Timer.reset();

		// first gather the terms to be sorted
		ArrayList<String> termsToSort = new ArrayList<String>();
		String query = "select distinct term " +
				"from term " +
				"where vocab_name in ('Disease Ontology','Mammalian Phenotype', 'Human Phenotype Ontology') ";
		ResultSet rs = ex.executeProto(query, cursorLimit);
		while(rs.next()) {
			termsToSort.add(rs.getString("term"));
		}
		rs.close();
		logger.info("  - collected data in list");

		//sort the terms using smart alpha
		Collections.sort(termsToSort,new SmartAlphaComparator());
		logger.info("  - sorted list in smart-alpha order");

		termSortMap = new HashMap<String,Integer>();
		for(int i=0;i<termsToSort.size();i++) {
			termSortMap.put(termsToSort.get(i), i);
		}
		maxTermSeqNum = termSortMap.keySet().size() + 1;
		logger.info("finished calculating sequence numbers for " + (maxTermSeqNum - 1) + " diseases and phenotypes " + Timer.getElapsedMessage());

		return termSortMap;
	}

	/* get the smart-alpha sequence number for the given disease or phenotype term
	 */
	public int getTermSequenceNum(String term) throws SQLException {
		if (termSortMap == null) {
			buildTermSortMap();
		}
		if (termSortMap.containsKey(term)) {
			return termSortMap.get(term);
		}
		return maxTermSeqNum;
	}

	/* get the mapping from each (String) marker key to a set of its searchable IDs
	 */
	protected Map<String,Set<String>> getMarkerAllIdMap() throws Exception {
		if (markerAllIdMap == null) {
			logger.info("Loading marker IDs");
			Timer.reset();

			// load all marker IDs (for every organism)
			String markerIdQuery="select marker_key, acc_id "
					+ "from marker_id "
					+ "where logical_db not in ('ABA','Download data from the QTL Archive','FuncBase','GENSAT','GEO','HomoloGene','MyGene','RIKEN Cluster','UniGene') ";
			markerAllIdMap = populateLookup(markerIdQuery,"marker_key","acc_id","marker keys to IDs");

			logger.info("Finished loading IDs for " + markerAllIdMap.size() + " markers" + Timer.getElapsedMessage());

			// For searching, add the alternate form for OMIM IDs
			for (String markerKey : markerAllIdMap.keySet()) {
				Set<String> altIds = markerAllIdMap.get(markerKey);
				List<String> newIds = new ArrayList<String>();
				// This adds the Number part of OMIM to the alt ids also for searching
				for (String altId : altIds) {
					if (altId.startsWith("OMIM:")) {
						newIds.add(altId.replaceFirst("OMIM:", ""));
					}
				}
				altIds.addAll(newIds);
			}
		}
		return markerAllIdMap;
	}

	/* get the set of marker IDs for the given marker key
	 */
	protected Set<String> getMarkerIds(Integer markerID) throws Exception {
		if (markerAllIdMap == null) { getMarkerAllIdMap(); }
		String markerIdString = markerID.toString();
		if (markerAllIdMap.containsKey(markerIdString)) {
			return markerAllIdMap.get(markerIdString);
		}
		return null;
	}

	/* If we've not already retrieved the homology cluster data from the database,
	 * get it now.  This pulls the union homology data, including HGNC and HomoloGene.
	 * The mapping is from (Integer) marker key to (Integer) grid cluster key.
	 */
	protected Map<Integer,Integer> getHomologyMap() throws SQLException {
		if (homologyMap == null) {
			logger.info("Retrieving homologyMap data");
			Timer.reset();
			homologyMap = new HashMap<Integer,Integer>();

			String homologyQuery = "select gcm.marker_key, gcm.hdp_gridcluster_key "
				+ "from hdp_gridcluster_marker gcm";
			
			ResultSet rs = ex.executeProto(homologyQuery, cursorLimit);
			while (rs.next()) {
				homologyMap.put(rs.getInt("marker_key"), rs.getInt("hdp_gridcluster_key"));
			}
			rs.close();

			logger.info("Finished retrieving homologyMap data (" + homologyMap.size() + " markers)" + Timer.getElapsedMessage());
		}
		return homologyMap;
	}

	/* return the gridcluster key for the given marker key
	 */
	protected Integer getGridClusterKey (int markerKey) throws SQLException {
		if (homologyMap == null) { getHomologyMap(); }
		if (homologyMap.containsKey(markerKey)) {
			return homologyMap.get(markerKey);
		}
		return null;
	}

	protected void cacheGridClusterKeys() throws SQLException {
		if (genoclusterToGridcluster != null) { return; }
		
		logger.info("Caching grid clusters per genocluster");
		Timer.reset();

		genoclusterToGridcluster = new HashMap<Integer,Set<Integer>>();

		String gcQuery = "select distinct gc.hdp_genocluster_key, gcm.hdp_gridcluster_key "
			+ "from hdp_genocluster_marker gc, "
			+ "  hdp_gridcluster_marker gcm "
			+ "where gc.marker_key = gcm.marker_key";

		ResultSet rs = ex.executeProto(gcQuery, cursorLimit);
		while (rs.next()) {
			Integer genoclusterKey = rs.getInt("hdp_genocluster_key");

			if (!genoclusterToGridcluster.containsKey(genoclusterKey)) {
				genoclusterToGridcluster.put(genoclusterKey, new HashSet<Integer>());
			}
			genoclusterToGridcluster.get(genoclusterKey).add(rs.getInt("hdp_gridcluster_key"));
		}
		rs.close();

		logger.info("Finished retrieving gridclusters for (" + markerIdMap.size() + " genoclusters)" + Timer.getElapsedMessage());
	}
	
	/* retrieve the grid cluster keys associated with the given genoclusterKey (can be
	 * more than one).  returns null if none exist.
	 */
	protected Set<Integer> getGridClusterKeys (int genoclusterKey) throws SQLException {
		if (genoclusterToGridcluster == null) { cacheGridClusterKeys(); }
		if (genoclusterToGridcluster.containsKey(genoclusterKey)) {
			return genoclusterToGridcluster.get(genoclusterKey);
		}
		return null;
	}
	
	/* If we've not already retrieved the feature type data from the database,
	 * get it now.  This pulls the feature types for all mouse markers in the
	 * various grid clusters.  Mapping is from (String) grid cluster keys to a
	 * Set of (String) feature types.
	 */
	protected Map<String,Set<String>> getFeatureTypeMap() throws Exception {
		if (featureTypeMap == null) {
			logger.info("Retrieving feature types");
			Timer.reset();

			// look up the feature types for all mouse markers in a 
			// homology cluster, and associate them with the gridcluster
			// (to use for filtering query results in HMDC).  Also include
			// mouse markers that are not part of a homology cluster.
			String featureTypeQuery =
					"select distinct gc.hdp_gridcluster_key, "
							+ "  m.marker_subtype as feature_type "
							+ "from hdp_gridcluster_marker gc, "
							+ "  homology_cluster_organism_to_marker sm, "
							+ "  homology_cluster_organism so, "
							+ "  homology_cluster_organism oo, "
							+ "  homology_cluster_organism_to_marker om, "
							+ "  marker m "
							+ "where gc.marker_key = sm.marker_key "
							+ "  and sm.cluster_organism_key = so.cluster_organism_key "
							+ "  and so.cluster_key = oo.cluster_key "
							+ "  and oo.cluster_organism_key = om.cluster_organism_key "
							+ "  and oo.organism = 'mouse' "
							+ "  and om.marker_key = m.marker_key "
							+ "  and m.marker_subtype is not null "
							+ "union "
							+ "select distinct gc.hdp_gridcluster_key, "
							+ "  m.marker_subtype as feature_type "
							+ "from hdp_gridcluster_marker gc, "
							+ "  marker m "
							+ "where gc.marker_key = m.marker_key "
							+ "  and not exists (select 1 from "
							+ "    homology_cluster_organism_to_marker sm "
							+ "    where gc.marker_key = sm.marker_key) "
							+ "  and m.marker_subtype is not null";

			featureTypeMap = populateLookupOrdered(featureTypeQuery,
					"hdp_gridcluster_key", "feature_type", "gridcluster keys to feature types");

			logger.info("Finished retrieving feature types (" + featureTypeMap.size() + " gridclusters)" + Timer.getElapsedMessage());
		}
		return featureTypeMap;
	}

	/* return the feature types associated with all markers in the given grid cluster
	 */
	protected Set<String> getFeatureTypes(Integer gridClusterKey) throws Exception {
		if (gridClusterKey == null) { return null; }
		if (featureTypeMap == null) { getFeatureTypeMap(); }

		String gckString = gridClusterKey.toString();
		if (featureTypeMap.containsKey(gckString)) {
			return featureTypeMap.get(gckString);
		}
		return null;
	}

	/* cache a mapping from (String) disease ID to a Set of (String) marker keys that are
	 * positively associated with that disease.  (no annotations with a NOT qualifier)
	 */
	protected void getMarkersPerDisease() throws Exception {
		if (markersPerDisease != null) { return; }

		logger.info("Retrieving markers per disease");
		Timer.reset();

		// This had used getNonNormalAnnotationsTable() as a source, but optimizing to
		// bring the 'where' clause up into this query and simplify.  Added closure to
		// consider DAG relationships of diseases when collecting their markers.
		String markerQuery = 
				"with closure as ( "
					+ "select ha.term_key, ha.term_id, s.ancestor_primary_id "
					+ "from hdp_annotation ha, term_ancestor s "
					+ "where ha.term_key = s.term_key "
					+ "  and ha.vocab_name = 'Disease Ontology' "
					+ "union " 
					+ "select ha.term_key, ha.term_id, ha.term_id "
					+ "from hdp_annotation ha "
					+ "where ha.vocab_name = 'Disease Ontology' "
					+ ") "
				+ "select distinct c.ancestor_primary_id as term_id, h.marker_key, m.symbol "
				+ "from hdp_annotation h, marker m, closure c "
				+ "where h.vocab_name='Disease Ontology' "
				+ " and h.organism_key in (1, 2) "
				+ " and h.term_id = c.term_id "
				+ " and h.marker_key = m.marker_key "
				+ " and h.qualifier_type is null "
				+ " and (h.genotype_type!='complex' or h.genotype_type is null) "
				+ "order by 1, 3";

		markersPerDisease = populateLookupOrdered(markerQuery, "term_id", "marker_key", "diseases to markers");

		logger.info("Finished retrieving markers for (" + markersPerDisease.size() + " diseases)" + Timer.getElapsedMessage());
		return;
	}

	/* get the markers for the disease specified by 'diseaseID'
	 */
	protected Set<String> getMarkersByDisease (String diseaseID) throws Exception {
		if (markersPerDisease == null) { getMarkersPerDisease(); }
		if (markersPerDisease.containsKey(diseaseID)) {
			return markersPerDisease.get(diseaseID);
		}
		return null;
	}

	/* get a mapping from (String) disease ID to a Set of (String) header terms.
	 */
	protected Map<String,Set<String>> cacheHeadersPerTerm() throws Exception {
		if (headersPerTerm == null) {
			logger.info("Retrieving headers per term");
			Timer.reset();

			String headerQuery = "select distinct term_id, header "
					+ "from hdp_annotation "
					+ "order by term_id, header";
			headersPerTerm = populateLookupOrdered(headerQuery, "term_id", "header", "terms to headers");

			logger.info("Finished retrieving headers for (" + headersPerTerm.size() + " diseases)" + Timer.getElapsedMessage());
		}
		return headersPerTerm;
	}

	/* get the headers for the disease specified by 'diseaseID'
	 */
	protected Set<String> getHeadersPerTerm (String diseaseID) throws Exception {
		if (headersPerTerm == null) { cacheHeadersPerTerm(); }
		if (headersPerTerm.containsKey(diseaseID)) {
			return headersPerTerm.get(diseaseID);
		}
		return null;
	}

	/* get the headers for the disease specified by the given term key
	 */
	protected Set<String> getHeadersPerTerm (Integer diseaseKey) throws Exception {
		if (diseaseKey == null) { return null; }
		String termId = getTermId(diseaseKey);
		if (termId == null) { return null; }
		return getHeadersPerTerm(termId);
	}

	/* get a mapping from (String) disease ID to an (Integer) count of references
	 */
	protected Map<String,Integer> getDiseaseReferenceCounts() throws Exception {
		if (refCountPerDisease == null) {
			Timer.reset();
			refCountPerDisease = new HashMap<String,Integer>();

			logger.info("building counts of disease relevant refs to disease ID");
			
			// updated to include references for descendant terms, since diseases are now a DAG
			String diseaseRefCountQuery = 
				"with closure as ( "
					+ "select ha.term_key, ha.term_id, s.ancestor_primary_id "
					+ "from hdp_annotation ha, term_ancestor s "
					+ "where ha.term_key = s.term_key "
					+ "  and ha.vocab_name = 'Disease Ontology' "
					+ "union " 
					+ "select ha.term_key, ha.term_id, ha.term_id "
					+ "from hdp_annotation ha "
					+ "where ha.vocab_name = 'Disease Ontology' "
					+ ") "
				+ "select c.ancestor_primary_id as disease_id, count(distinct trt.reference_key) as ref_count "
				+ "from hdp_term_to_reference trt, hdp_annotation ha, closure c "
				+ "where ha.term_key = trt.term_key "
				+ " and ha.term_key = c.term_key "
				+ " and ha.vocab_name='Disease Ontology' "
				+ "group by 1";

			ResultSet rs = ex.executeProto(diseaseRefCountQuery, cursorLimit);
			while(rs.next()) {
				refCountPerDisease.put(rs.getString("disease_id"),rs.getInt("ref_count"));
			}
			rs.close();
			logger.info("done building ref counts for " + refCountPerDisease.size() + " diseases" + Timer.getElapsedMessage());
		}
		return refCountPerDisease;
	}

	/* get the count of disease-relevant references for the given disease
	 */
	protected int getDiseaseReferenceCount(String diseaseID) throws Exception {
		if (refCountPerDisease == null) { getDiseaseReferenceCounts(); }
		if (refCountPerDisease.containsKey(diseaseID)) {
			return refCountPerDisease.get(diseaseID);
		}
		return 0;
	}

	/* get a mapping from (Integer) marker key to a Set of (Integer) orthologous marker keys
	 */
	protected Map<Integer,Set<Integer>> getMarkerOrthologs() throws Exception {
		if (markerOrthologs == null) {
			Timer.reset();
			markerOrthologs = new HashMap<Integer,Set<Integer>>();

			logger.info("building mapping from each marker to its orthologs");
			String orthologQuery = "select distinct otm.marker_key as marker_key, "
					+ "  other_otm.marker_key as other_marker_key "
					+ "from homology_cluster_organism o, "
					+ "  homology_cluster_organism_to_marker otm, "
					+ "  homology_cluster_organism other_o, "
					+ "  homology_cluster_organism_to_marker other_otm "
					+ "where other_o.cluster_key=o.cluster_key "
					+ "  and o.cluster_organism_key=otm.cluster_organism_key "
					+ "  and other_o.cluster_organism_key=other_otm.cluster_organism_key "
					+ "  and otm.marker_key!=other_otm.marker_key";

			ResultSet rs = ex.executeProto(orthologQuery, cursorLimit);
			while(rs.next()) {
				Integer markerKey = rs.getInt("marker_key");
				if (!markerOrthologs.containsKey(markerKey)) {
					markerOrthologs.put(markerKey, new HashSet<Integer>());
				}
				markerOrthologs.get(markerKey).add(rs.getInt("other_marker_key"));
			}
			rs.close();
			logger.info("done collecting orthologs for " + markerOrthologs.size() + " markers" + Timer.getElapsedMessage());
		}
		return markerOrthologs;
	}

	/* get the set of marker keys that are orthologous to the given marker key, or null
	 * if the given marker key has no orthologs
	 */
	protected Set<Integer> getMarkerOrthologs(Integer markerKey) throws Exception {
		if (markerOrthologs == null) { getMarkerOrthologs(); }
		if (markerOrthologs.containsKey(markerKey)) {
			return markerOrthologs.get(markerKey);
		}
		return null;
	}

	/* get a mapping from (String) disease ID to an (Integer) count of disease models
	 */
	protected Map<String,Integer> getDiseaseModelCounts() throws Exception {
		if (modelCountPerDisease == null) {
			Timer.reset();
			modelCountPerDisease = new HashMap<String,Integer>();

			logger.info("building counts of disease models for disease IDs");

			// updated to include models for descendant terms, since diseases are now a DAG
			String diseaseModelQuery=
				"with closure as ( "
					+ "select ha.term_key, ha.term_id, s.ancestor_primary_id "
					+ "from hdp_annotation ha, term_ancestor s "
					+ "where ha.term_key = s.term_key "
					+ " and ha.vocab_name = 'Disease Ontology' "
					+ "union "
					+ "select ha.term_key, ha.term_id, ha.term_id "
					+ "from hdp_annotation ha "
					+ "where ha.vocab_name = 'Disease Ontology' "
					+ ") "
				+ "select c.ancestor_primary_id as disease_id, "
				+ " count(distinct dm.disease_model_key) as diseaseModelCount "
				+ "from disease_model dm, closure c "
				+ "where dm.is_not_model=0 "
				+ " and dm.disease_id = c.term_id "
				+ "group by 1";

			ResultSet rs = ex.executeProto(diseaseModelQuery, cursorLimit);
			while(rs.next()) {
				modelCountPerDisease.put(rs.getString("disease_id"),rs.getInt("diseaseModelCount"));
			}
			rs.close();
			logger.info("done building model counts for " + modelCountPerDisease.size() + " diseases" + Timer.getElapsedMessage());
		}
		return modelCountPerDisease;
	}

	/* get the count of disease models for the given disease
	 */
	protected int getDiseaseModelCount(String diseaseID) throws Exception {
		if (modelCountPerDisease == null) { getDiseaseModelCounts(); }
		if (modelCountPerDisease.containsKey(diseaseID)) {
			return modelCountPerDisease.get(diseaseID);
		}
		return 0;
	}

	/* populate the three caches of basic marker data (symbols, names, and primary IDs)
	 */
	private void cacheBasicMarkerData() throws SQLException {
		if (markerSymbolMap != null) { return; }

		logger.info("Caching basic marker data");
		Timer.reset();

		markerSymbolMap = new HashMap<Integer, String>();
		markerNameMap = new HashMap<Integer, String>();
		markerIdMap = new HashMap<Integer, String>();

		String markerQuery = "select marker_key, symbol, name, primary_id "
				+ "from marker "
				+ "where status != 'withdrawn' ";

		ResultSet rs = ex.executeProto(markerQuery, cursorLimit);
		while (rs.next()) {
			markerSymbolMap.put(rs.getInt("marker_key"), rs.getString("symbol"));
			markerNameMap.put(rs.getInt("marker_key"), rs.getString("name"));
			markerIdMap.put(rs.getInt("marker_key"), rs.getString("primary_id"));
		}
		rs.close();

		logger.info("Finished retrieving basic marker data (" + markerIdMap.size() + " markers)" + Timer.getElapsedMessage());
	}

	/* get a mapping from marker key to marker symbol for all current markers, cached
	 * in memory after the first retrieval from the database.  Mapping is from (Integer)
	 * marker key to (String) marker symbol.
	 */
	protected Map<Integer, String> getMarkerSymbolMap() throws SQLException {
		if (markerSymbolMap == null) { cacheBasicMarkerData(); }
		return markerSymbolMap;
	}

	/* get a mapping from marker key to marker name for all current markers, cached
	 * in memory after the first retrieval from the database.  Mapping is from (Integer)
	 * marker key to (String) marker name.
	 */
	protected Map<Integer, String> getMarkerNameMap() throws SQLException {
		if (markerNameMap == null) { cacheBasicMarkerData(); }
		return markerNameMap;
	}

	/* get a mapping from marker key to primary marker ID for all current markers, cached
	 * in memory after the first retrieval from the database.  Mapping is from (Integer)
	 * marker key to (String) marker ID.
	 */
	protected Map<Integer, String> getMarkerIdMap() throws SQLException {
		if (markerIdMap == null) { cacheBasicMarkerData(); }
		return markerIdMap;
	}

	/* retrieve the marker symbol for the given 'markerKey' or null if the key
	 * is not recognized.
	 */
	protected String getMarkerSymbol(int markerKey) throws SQLException {
		if (markerSymbolMap == null) { cacheBasicMarkerData(); }
		if (markerSymbolMap.containsKey(markerKey)) {
			return markerSymbolMap.get(markerKey);
		}
		return null;
	}

	/* retrieve the marker name for the given 'markerKey' or null if the key
	 * is not recognized.
	 */
	protected String getMarkerName(int markerKey) throws SQLException {
		if (markerNameMap == null) { cacheBasicMarkerData(); }
		if (markerNameMap.containsKey(markerKey)) {
			return markerNameMap.get(markerKey);
		}
		return null;
	}

	/* retrieve the primary ID for the given 'markerKey' or null if the key
	 * is not recognized.
	 */
	protected String getMarkerID(int markerKey) throws SQLException {
		if (markerIdMap == null) { cacheBasicMarkerData(); }
		if (markerIdMap.containsKey(markerKey)) {
			return markerIdMap.get(markerKey);
		}
		return null;
	}

	/* retrieve from the database the sets of mouse and human marker keys, and remember
	 * them in memory.
	 */
	private void cacheMarkerOrganismData() throws SQLException {
		if (mouseMarkers != null) { return; }

		logger.info("Identifying human and mouse markers");
		Timer.reset();

		mouseMarkers = new HashSet<Integer>();
		humanMarkers = new HashSet<Integer>();

		String organismQuery = "select marker_key, organism "
				+ "from marker "
				+ "where status != 'withdrawn' "
				+ "  and organism in ('human', 'mouse') ";

		ResultSet rs = ex.executeProto(organismQuery, cursorLimit);
		while (rs.next()) {
			if ("human".equals(rs.getString("organism"))) {
				humanMarkers.add(rs.getInt("marker_key"));
			} else {
				mouseMarkers.add(rs.getInt("marker_key"));
			}
		}
		rs.close();

		logger.info("Identified " + mouseMarkers.size() + " mouse and "
				+ humanMarkers.size() + " human markers" + Timer.getElapsedMessage());
	}

	/* returns true if the given 'markerKey' identifies a mouse marker, false if not.
	 */
	protected boolean isMouse(Integer markerKey) throws SQLException {
		if (mouseMarkers == null) { cacheMarkerOrganismData(); }
		return mouseMarkers.contains(markerKey);
	}

	/* returns true if the given 'markerKey' identifies a human marker, false if not.
	 */
	protected boolean isHuman(Integer markerKey) throws SQLException {
		if (humanMarkers == null) { cacheMarkerOrganismData(); }
		return humanMarkers.contains(markerKey);
	}

	/* load data from the database to populate several caches stored as instance variables;
	 * specifically populates diseaseTerms, mpTerms, hpoTerms, terms, and termIds.
	 */
	protected void cacheBasicTermData() throws Exception {
		if (diseaseTerms != null) { return; }

		logger.info("Caching basic term data");
		Timer.reset();

		diseaseTerms = new HashSet<Integer>();
		mpTerms = new HashSet<Integer>();
		hpoTerms = new HashSet<Integer>();
		terms = new HashMap<Integer,String>();
		termIds = new HashMap<Integer,String>();

		String termQuery = "select term_key, term, primary_id, vocab_name "
				+ "from term "
				+ "where vocab_name in ('Disease Ontology', 'Mammalian Phenotype', 'Human Phenotype Ontology')";

		ResultSet rs = ex.executeProto(termQuery, cursorLimit);
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			terms.put(termKey, rs.getString("term"));
			termIds.put(termKey, rs.getString("primary_id"));

			if (disease.equals(rs.getString("vocab_name"))) { diseaseTerms.add(termKey); }
			else if (hpo.equals(rs.getString("vocab_name"))) { hpoTerms.add(termKey); }
			else { mpTerms.add(termKey); }
		}
		rs.close();

		logger.info("Finished retrieving basic term data (" + terms.size() + " terms)" + Timer.getElapsedMessage());
		
		mpHeaderText = new HashMap<Integer,String>();
		
		String headerQuery = "select t.term_key, s.synonym "
			+ "from term t, term_synonym s "
			+ "where t.vocab_name = 'Mammalian Phenotype' "
			+ "  and t.term_key = s.term_key "
			+ "  and s.synonym_type = 'Synonym Type 1'";
		
		rs = ex.executeProto(headerQuery);
		
		while (rs.next()) {
			mpHeaderText.put(rs.getInt("term_key"), rs.getString("synonym"));
		}
		rs.close();
		
		logger.info("Finished retrieving MP header display strings " + Timer.getElapsedMessage());
	}

	/* get the display string for the given MP header term key; null if key is not for an MP header
	 */
	protected String getMpHeaderDisplay(int mpHeaderKey) throws Exception {
		if (mpHeaderText == null) { cacheBasicTermData(); }
		if (mpHeaderText.containsKey(mpHeaderKey)) { return mpHeaderText.get(mpHeaderKey); }
		return null;
	}
	
	/* get the vocabulary name for the given term key, or null if key is unknown
	 */
	protected String getVocabulary(Integer termKey) throws Exception {
		if (diseaseTerms == null) { cacheBasicTermData(); }
		if (diseaseTerms.contains(termKey)) { return disease; }
		if (mpTerms.contains(termKey)) { return mp; }
		if (hpoTerms.contains(termKey)) { return hpo; }
		return null;
	}

	/* get the term corresponding to the given term key, or null if key is unknown
	 */
	protected String getTerm(Integer termKey) throws Exception {
		if (terms == null) { cacheBasicTermData(); }
		if (terms.containsKey(termKey)) { return terms.get(termKey); }
		return null;
	}

	/* get the primary term ID corresponding to the given term key, or null if key is unknown
	 */
	protected String getTermId(Integer termKey) throws Exception {
		if (termIds == null) { cacheBasicTermData(); }
		if (termIds.containsKey(termKey)) { return termIds.get(termKey); }
		return null;
	}

	/* cache basic data for annotations, populating the instance variables annotationTermKeys
	 * and notAnnotations
	 */
	protected void cacheBasicAnnotationData() throws Exception {
		if (notAnnotations != null) { return; }

		logger.info("Caching basic annotation data");
		Timer.reset();

		annotationTermKeys = new HashMap<Integer,Integer>();
		notAnnotations = new HashSet<Integer>();

		String annotQuery = "select hdp_annotation_key, term_key, qualifier_type "
				+ "from hdp_annotation";

		ResultSet rs = ex.executeProto(annotQuery, cursorLimit);
		while (rs.next()) {
			Integer hdpAnnotationKey = rs.getInt("hdp_annotation_key");
			String qualifier = rs.getString("qualifier_type");

			annotationTermKeys.put(hdpAnnotationKey, rs.getInt("term_key"));
			if ((qualifier != null) && "NOT".equals(qualifier)) {
				notAnnotations.add(hdpAnnotationKey);
			}
		}
		rs.close();

		logger.info("Finished retrieving basic annotation data (" + annotationTermKeys.size() + " annotations)" + Timer.getElapsedMessage());
	}

	/* determine if the annotation with the given hdp_annotation_key has a NOT qualifier
	 */
	protected boolean isNotAnnotation(Integer annotationKey) throws Exception {
		if (notAnnotations == null) { cacheBasicAnnotationData(); }
		if (notAnnotations.contains(annotationKey)) { return true; }
		return false;
	}

	/* get the term key for the given hdp_annotation_key, or null if annotation key is unknown
	 */
	protected Integer getAnnotatedTermKey(Integer annotationKey) throws Exception {
		if (annotationTermKeys == null) { cacheBasicAnnotationData(); }
		if (annotationTermKeys.containsKey(annotationKey)) {
			return annotationTermKeys.get(annotationKey);
		}
		return null;
	}

	/* populate the instance variable relatedAnnotations, tracking which annotations are related
	 * to which other annotations, either by being for the same genocluster (for mouse data) or for
	 * the same human markers
	 */
	protected void cacheAnnotationRelationships() throws SQLException {
		if (relatedAnnotations != null) { return; }

		logger.info("Caching annotation relationships");
		Timer.reset();

		relatedAnnotations = new HashMap<Integer,Set<Integer>>();

		String mouseQuery = "with genoclusters as ( "
				+ "  select distinct on (ha.hdp_annotation_key) ha.hdp_annotation_key, "
				+ "    gg.hdp_genocluster_key "
				+ "  from hdp_annotation ha, "
				+ "    hdp_genocluster_genotype gg "
				+ "  where ha.genotype_key = gg.genotype_key) "
				+ "select h1.hdp_annotation_key as annotKey1, "
				+ "  g2.hdp_annotation_key as annotKey2 "
				+ "from hdp_annotation h1, genoclusters g1, genoclusters g2 "
				+ "where h1.hdp_annotation_key = g1.hdp_annotation_key "
				+ "  and g1.hdp_genocluster_key = g2.hdp_genocluster_key "
				+ "  and g1.hdp_annotation_key != g2.hdp_annotation_key";

		ResultSet rs = ex.executeProto(mouseQuery, cursorLimit);
		while (rs.next()) {
			Integer annotKey1 = rs.getInt("annotKey1");

			if (!relatedAnnotations.containsKey(annotKey1)) {
				relatedAnnotations.put(annotKey1, new HashSet<Integer>());
			}
			relatedAnnotations.get(annotKey1).add(rs.getInt("annotKey2"));
		}
		rs.close();

		int mouseCount = relatedAnnotations.size();
		logger.info("Got relationships for " + mouseCount + " mouse annotations)" + Timer.getElapsedMessage());
	}

	/* get the set of annotation keys that are related to the given hdp_annotation_key, based on
	 * mouse data for the same genocluster and human data for the same marker.  Returns null
	 * if there are no relationships for the hdp_annotation_key.
	 */
	protected Set<Integer> getRelatedAnnotations(Integer annotationKey) throws Exception {
		if (relatedAnnotations == null) { cacheAnnotationRelationships(); }
		if (relatedAnnotations.containsKey(annotationKey)) {
			return relatedAnnotations.get(annotationKey);
		}
		return null;
	}

	/* get the set of IDs and/or terms connected to the given annotation through our set of
	 * "related annotations".  Boolean flags indicate whether to return terms or IDs or both,
	 * and whether to bring back diseases or phenotypes or both.  Returns null if no related
	 * annotations or if the given annotation key is unknown.  Also includes relevant terms,
	 * synonyms, and IDs of ancestors, according to the parameters.
	 */
	protected Set<String> getRelatedTerms (Integer annotationKey, boolean getTerms, boolean getIds, boolean getDiseases, boolean getPhenotypes) throws Exception {

		Set<Integer> relatedAnnot = getRelatedAnnotations(annotationKey);
		if (relatedAnnot == null) { return null; }

		Set<String> out = new HashSet<String>();		// set of strings to return

		for (Integer annotKey : relatedAnnot) {
			Integer termKey = getAnnotatedTermKey(annotKey);
			String vocab = getVocabulary(termKey);
			if ( (getDiseases && disease.equals(vocab)) || (getPhenotypes && mp.equals(vocab)) ) {
				if (getTerms) {
					String term = getTerm(termKey);
					if (term != null) { out.add(term); }
					Set<String> ancestorTerms = getTermAncestorText(termKey);

					// if getting terms, also add synonyms
					String termId = getTermId(termKey);
					Set<String> ancestorSynonyms = getTermSynonyms(termId);
					if (ancestorSynonyms != null) {
						out.addAll(ancestorSynonyms);
					}

					if (ancestorTerms != null) { out.addAll(ancestorTerms); }
				}
				if (getIds) {
					String termId = getTermId(termKey);
					if (termId != null) { out.add(termId); }

					// need to add both alternate IDs and ancestor IDs

					if (termId != null) {
						Set<String> altIds = getAlternateTermIds(termId);
						if (altIds != null) { out.addAll(altIds); }
					}

					Set<String> ancestorIds = getTermAncestorIDs(termKey);
					if (ancestorIds != null) { out.addAll(ancestorIds); }
				}
			}
		}
		return out;
	}

	/* get the set of disease IDs and terms which come via related annotations for the specified annotation
	 */
	protected Set<String> getRelatedDiseases(Integer annotationKey, boolean getTerms, boolean getIds) throws Exception {
		return getRelatedTerms(annotationKey, getTerms, getIds, true, false);
	}

	/* get the set of phenotype IDs and terms which come via related annotations for the specified annotation
	 */
	protected Set<String> getRelatedPhenotypes(Integer annotationKey, boolean getTerms, boolean getIds) throws Exception {
		return getRelatedTerms(annotationKey, getTerms, getIds, false, true);
	}

	/* cache the hybrid homology cluster keys for each human and mouse marker that is
	 * in a cluster.
	 */
	protected void cacheHomologyClusterKeys() throws Exception {
		if (markerToHomologyCluster != null) { return; }
		
		logger.info("retrieving homology clusters");
		Timer.reset();

		markerToHomologyCluster = new HashMap<Integer,Integer>();
		
		// need to first hit the hybrid homology to see which source was chosen;
		// then use that info to hit the homology tables again to get the source
		// cluster key
		String homologyQuery = "with hybrid as ("
			+ "  select otm.marker_key, hc.cluster_key, "
			+ "    case when hc.secondary_source = 'HomoloGene and HGNC' then 'HGNC' "
			+ "    else hc.secondary_source "
			+ "    end secondary_source "
			+ "  from homology_cluster_organism_to_marker otm, "
			+ "    homology_cluster_organism hco, homology_cluster hc "
			+ "  where otm.cluster_organism_key = hco.cluster_organism_key "
			+ "    and hco.organism in ('human', 'mouse') "
			+ "    and hco.cluster_key = hc.cluster_key " 
			+ "    and hc.source = 'HomoloGene and HGNC' "
			+ ")"
			+ "select otm.marker_key, hc.cluster_key "
			+ "from hybrid h, homology_cluster_organism_to_marker otm, "
			+ "  homology_cluster_organism hco, homology_cluster hc "
			+ "where otm.cluster_organism_key = hco.cluster_organism_key "
			+ "  and hco.organism in ('human', 'mouse') "
			+ "  and hco.cluster_key = hc.cluster_key "
			+ "  and otm.marker_key = h.marker_key "
			+ "  and h.secondary_source = hc.source";
		
		ResultSet rs = ex.executeProto(homologyQuery, cursorLimit);
		
		while (rs.next()) {
			markerToHomologyCluster.put(rs.getInt("marker_key"), rs.getInt("cluster_key"));
		}
		rs.close();
		logger.info("  - retrieved homology clusters for " + markerToHomologyCluster.size() + " markers " + Timer.getElapsedMessage());
	}
	
	/* get the homology cluster key for the given marker (use hybrid homology)
	 */
	protected Integer getHomologyClusterKey(int markerKey) throws Exception {
		if (markerToHomologyCluster == null) { cacheHomologyClusterKeys(); }
		if (markerToHomologyCluster.containsKey(markerKey)) {
			return markerToHomologyCluster.get(markerKey);
		}
		return null;
	}
	
	/* cache the human and mouse marker data for each grid cluster
	 */
	protected void cacheGridClusterMarkers() throws Exception {
		if (gcToHumanMarkers != null) { return; }
		cacheHomologyClusterKeys();

		logger.info("retrieving gridcluster markers");
		Timer.reset();

		gcToHumanMarkers = new HashMap<Integer,String>();
		gcToMouseMarkers = new HashMap<Integer,String>();

		String markerQuery = "select gcm.hdp_gridcluster_key, m.organism, m.symbol, m.marker_key, "
				+ "  m.primary_id, ms.by_symbol, m.marker_type, m.marker_subtype, m.name "
				+ "from hdp_gridcluster_marker gcm, marker m, marker_sequence_num ms "
				+ "where gcm.marker_key = m.marker_key "
				+ "  and m.marker_key = ms.marker_key "
				+ "order by gcm.hdp_gridcluster_key, m.organism, ms.by_symbol";

		ResultSet rs = ex.executeProto(markerQuery, cursorLimit);
		
		List<GridMarker> humanGM = null;
		List<GridMarker> mouseGM = null;
		
		int lastGcKey = -1;
		
		while (rs.next()) {
			Integer gcKey = rs.getInt("hdp_gridcluster_key");
			String organism = rs.getString("organism");
			String symbol = rs.getString("symbol");
			String name = rs.getString("name");
			String accId = rs.getString("primary_id");
			String markerType = rs.getString("marker_type");
			String markerSubType = rs.getString("marker_subtype");
			Integer markerKey = rs.getInt("marker_key");

			// beginning to collect for a new gridcluster
			if (lastGcKey != gcKey.intValue()) {
				// need to save the old gridcluster data, if there is any
				if (lastGcKey >= 0) {
					if (humanGM.size() > 0) { gcToHumanMarkers.put(lastGcKey, mapper.writeValueAsString(humanGM)); }
					if (mouseGM.size() > 0) { gcToMouseMarkers.put(lastGcKey, mapper.writeValueAsString(mouseGM)); }
				}
				humanGM = new ArrayList<GridMarker>();
				mouseGM = new ArrayList<GridMarker>();
				lastGcKey = gcKey.intValue();
			}

			if ("human".equals(organism)) {
				humanGM.add(new GridMarker(symbol, accId, name, markerType, getHomologyClusterKey(markerKey)));
			} else {
				mouseGM.add(new GridMarker(symbol, accId, name, markerSubType, getHomologyClusterKey(markerKey)));
			}
		}
		
		// add the last ones found
		if (lastGcKey >= 0) {
			if (humanGM.size() > 0) { gcToHumanMarkers.put(lastGcKey, mapper.writeValueAsString(humanGM)); }
			if (mouseGM.size() > 0) { gcToMouseMarkers.put(lastGcKey, mapper.writeValueAsString(mouseGM)); }
		}

		rs.close();
		logger.info("  - retrieved marker data for gridclusters " + Timer.getElapsedMessage());
	}

	/* get the mouse marker data (as JSON) for the given grid cluster key;
	 * returns null if no mouse markers or unknown grid cluster key
	 */
	protected String getMouseMarkers(int gridClusterKey) throws Exception {
		if (gcToMouseMarkers == null) { cacheGridClusterMarkers(); }
		if (gcToMouseMarkers.containsKey(gridClusterKey)) {
			return gcToMouseMarkers.get(gridClusterKey);
		}
		return null;
	}

	/* get the human marker data (as JSON) for the given grid cluster key;
	 * returns null if no human markers or unknown grid cluster key
	 */
	protected String getHumanMarkers(int gridClusterKey) throws Exception {
		if (gcToHumanMarkers == null) { cacheGridClusterMarkers(); }
		if (gcToHumanMarkers.containsKey(gridClusterKey)) {
			return gcToHumanMarkers.get(gridClusterKey);
		}
		return null;
	}
	
	/* strip the markup out of the allele combination and leave jsut the allele symbols
	 */
	protected String stripAlleleMarkup(String combination) {
		return combination.replaceAll("\\\\Allele.[^|]*.([^|]*).[^)]*.", "$1");
	}
	
	/* takes a genotype's allele pairs from the database and reformats them for web display
	 */
	protected String formatAllelePairsForDisplay(String combination) {
		if (combination == null) { return ""; }
		return stripAlleleMarkup(combination).replaceAll("<", "@@@sup@@@").replaceAll(">", "</sup>")
			.replaceAll("@@@sup@@@", "<sup>").trim().replaceAll("\n", "<br/>");
	}

	/* cache the allele pairs in memory for each genocluster
	 */
	protected void cacheAllelePairs() throws Exception {
		if (allelePairs != null) { return; }

		logger.info("retrieving allele pairs for genoclusters");
		Timer.reset();

		// grab the allele combination from the first genotype for each genocluster
		String allelePairQuery = "select distinct on (gc1.hdp_genocluster_key) "
				+ "  gc1.hdp_genocluster_key, g1.combination_1, g1.is_conditional "
				+ "from hdp_genocluster_genotype gc1, "
				+ "  genotype g1 "
				+ "where gc1.genotype_key = g1.genotype_key";

		allelePairs = new HashMap<Integer,String>();
		conditionalGenoclusters = new HashSet<Integer>();

		ResultSet rs = ex.executeProto(allelePairQuery, cursorLimit);
		while (rs.next()) {
			allelePairs.put(rs.getInt("hdp_genocluster_key"), formatAllelePairsForDisplay(rs.getString("combination_1")));
			if (rs.getInt("is_conditional") == 1) {
				conditionalGenoclusters.add(rs.getInt("hdp_genocluster_key"));
			}
		}
		rs.close();

		logger.info("finished retrieving allele pairs for " + allelePairs.size() + " genoclusters " + Timer.getElapsedMessage());
		logger.info("found " + conditionalGenoclusters.size() + " conditional genoclusters");
	}

	/* get the allele pairs for the specified genocluster as a String with embedded
	 * \Allele() tags, suitable for formatting by the fewi's NotesTagConverter.  Returns
	 * null if no allele pairs for the specified genocluster key.
	 */
	protected String getAllelePairs(int genoClusterKey) throws Exception {
		if (allelePairs == null) { cacheAllelePairs(); }
		if (allelePairs.containsKey(genoClusterKey)) {
			return allelePairs.get(genoClusterKey);
		}
		return null;
	}

	/* returns true if the specified genocluster is condtional, false if not
	 */
	protected boolean isConditional(int genoClusterKey) throws Exception {
		if (conditionalGenoclusters == null) { cacheAllelePairs(); }
		return conditionalGenoclusters.contains(genoClusterKey);
	}

	/* populate the caches of DO terms to HPO terms and of HPO header terms to MP header terms
	 */
	protected void cacheHpoMaps() throws Exception {
		// get the mapping from DO term to HPO terms

		logger.info("retrieving HPO mappings");
		Timer.reset();

		// do not make this distinct, as duplicates are intentional and important
		String diseaseToHpoQuery = "select term_key_1 as do_key, term_key_2 as hpo_key "
			+ "from term_to_term tt "
			+ "where tt.relationship_type = 'DO to HPO'";
		
		diseaseToHpo = new HashMap<Integer,List<Integer>>();

		ResultSet rs = ex.executeProto(diseaseToHpoQuery, cursorLimit);
		while (rs.next()) {
			Integer doKey = rs.getInt("do_key");
			Integer hpoKey = rs.getInt("hpo_key");
			
			if (!diseaseToHpo.containsKey(doKey)) {
				diseaseToHpo.put(doKey, new ArrayList<Integer>());
			}
			diseaseToHpo.get(doKey).add(hpoKey);
		}
		rs.close();
		logger.info(" - got HPO terms for " + diseaseToHpo.size() + " DO terms " + Timer.getElapsedMessage());
		
		// get the mapping from HPO high-level terms to MP headers
		String hpoHeaderToMpQuery = "select term_key_1 as mp_key, term_key_2 as hpo_key "
			+ "from term_to_term tt "
			+ "where tt.relationship_type = 'MP header to HPO high-level'";

		hpoHeaderToMp = new HashMap<Integer,Set<Integer>>();

		ResultSet rs2 = ex.executeProto(hpoHeaderToMpQuery, cursorLimit);
		while (rs2.next()) {
			Integer mpKey = rs2.getInt("mp_key");
			Integer hpoKey = rs2.getInt("hpo_key");
			
			if (!hpoHeaderToMp.containsKey(hpoKey)) {
				hpoHeaderToMp.put(hpoKey, new HashSet<Integer>());
			}
			hpoHeaderToMp.get(hpoKey).add(mpKey);
		}
		rs2.close();
		logger.info(" - got MP headers for " + hpoHeaderToMp.size() + " HPO terms " + Timer.getElapsedMessage());
		logger.info("finished retrieving HPO mappings ");
	}

	/* get the term keys for the HPO terms associated with the given DO term key;
	 * null if there are none.
	 */
	protected List<Integer> getHpoTermKeys(Integer diseaseTermKey) throws Exception {
		if (diseaseToHpo == null) { cacheHpoMaps(); }
		if (diseaseToHpo.containsKey(diseaseTermKey)) { return diseaseToHpo.get(diseaseTermKey); }
		return null;
	}
	
	/* get the term keys for the MP headers associated with the given HPO high-level term key;
	 * null if there are none.
	 */
	private Set<Integer> getDirectMpHeaderKeys(Integer hpoTermKey) throws Exception {
		if (hpoHeaderToMp == null) { cacheHpoMaps(); }
		if (hpoHeaderToMp.containsKey(hpoTermKey)) { return hpoHeaderToMp.get(hpoTermKey); }
		return null;
	}
	
	/* returns true if the given key is for an HPO high-level term, false if not
	 */
	private boolean isHighLevelHpo(Integer hpoTermKey) throws Exception {
		if (hpoHeaderToMp == null) { cacheHpoMaps(); }
		return hpoHeaderToMp.containsKey(hpoTermKey);
	}
	
	/* get the term keys for the MP headers associated with any HPO high-level terms that are
	 * ancestors of the given HPO term (or are the term itself). returns empty set if none.
	 */
	protected Set<Integer> getMpHeaderKeys(Integer hpoTermKey) throws Exception {
		Set<Integer> union = new HashSet<Integer>();
		
		// check the term itself, to see if it is an HPO high-level term
		Set<Integer> headerKeys = getDirectMpHeaderKeys(hpoTermKey);
		if (headerKeys != null) {
			union.addAll(headerKeys);
		}
		
		// now check the ancestors of the term and pick up any of their headers
		for (Integer termKey : getTermAncestors(hpoTermKey)) {
			if (isHighLevelHpo(termKey)) {
				headerKeys = getDirectMpHeaderKeys(termKey);
				if (headerKeys != null) {
					union.addAll(headerKeys);
				}
			}
		}
		return union;
	}

	/* cache the 'expressed component' relationships from the database (marker to expressed marker,
	 * both 'expresses' and 'expresses ortholog of').  We also include all markers in the homology
	 * cluster that involves the expressed marker.
	 */
	protected void cacheExpressedComponents() throws Exception {
		if (expressedComponents != null) { return; }

		logger.info("retrieving expressed components");
		Timer.reset();

		// top half of union is mouse-to-mouse, bottom is mouse-to-human
		String ecQuery = "select m.marker_key, arm.related_marker_key "
			+ "from allele_related_marker arm, allele a, marker_to_allele mta, "
			+ "  marker m "
			+ "where arm.relationship_category = 'expresses_component' "
			+ "  and arm.allele_key = a.allele_key "
			+ "  and a.allele_key = mta.allele_key "
			+ "  and mta.marker_key = m.marker_key "
			+ "  and m.marker_type = 'Transgene'"
			+ "union "
			+ "select m.marker_key, r.marker_key as expressed_marker_key "
			+ "from allele a, marker_to_allele m, allele_related_marker arm, allele_arm_property po, "
			+ "  allele_arm_property pi, allele_arm_property ps, marker_id ri, marker r "
			+ "where m.allele_key = a.allele_key "
			+ "  and a.allele_key = arm.allele_key "
			+ "  and arm.arm_key = po.arm_key "
			+ "  and arm.arm_key = pi.arm_key "
			+ "  and arm.arm_key = ps.arm_key "
			+ "  and po.name = 'Non-mouse_Organism' "
			+ "  and pi.name = 'Non-mouse_NCBI_Gene_ID' "
			+ "  and ps.name = 'Non-mouse_Gene_Symbol' "
			+ "  and pi.value = ri.acc_id "
			+ "  and ps.value = r.symbol "
			+ "  and po.value ilike r.organism "
			+ "  and r.organism = 'human' "
			+ "  and ri.marker_key = r.marker_key";

		expressedComponents = new HashMap<Integer,Set<Integer>>();

		ResultSet rs = ex.executeProto(ecQuery, cursorLimit);
		while (rs.next()) {
			Integer markerKey = rs.getInt("marker_key");

			if (!expressedComponents.containsKey(markerKey)) {
				expressedComponents.put(markerKey, new HashSet<Integer>());
			}
			expressedComponents.get(markerKey).add(rs.getInt("related_marker_key"));
		}
		rs.close();

		logger.info("finished expressed components for " + expressedComponents.size() + " transgenes " + Timer.getElapsedMessage());
		
		// now add any missing human/mouse orthologs for the expressed components
		
		for (Integer markerKey : expressedComponents.keySet()) {
			Set<Integer> orthologs = new HashSet<Integer>();
			for (Integer expressedMarkerKey : expressedComponents.get(markerKey)) {
				Set<Integer> mouseOrthologs = getMouseOrthologs(expressedMarkerKey);
				Set<Integer> humanOrthologs = getHumanOrthologs(expressedMarkerKey);
				if (mouseOrthologs != null) { orthologs.addAll(mouseOrthologs); }
				if (humanOrthologs != null) { orthologs.addAll(humanOrthologs); }
			}
			if (orthologs.size() > 0) {
				expressedComponents.get(markerKey).addAll(orthologs);
			}
		}
		logger.info("included orthologs for expressed components " + Timer.getElapsedMessage());
	}
	
	/* add to the Solr document the symbols, names, synonyms, and IDs for 'expressed component' markers
	 * of transgenes
	 */
	protected void addExpressedComponents(DistinctSolrInputDocument doc, Integer sourceMarkerKey) throws Exception {
		if (expressedComponents == null) { cacheExpressedComponents(); }
		
		if (expressedComponents.containsKey(sourceMarkerKey)) {
			for (Integer expressedMarkerKey : expressedComponents.get(sourceMarkerKey)) {
				// Solr fields specific for expressed component markers
				doc.addDistinctField(DiseasePortalFields.EC_SYMBOL, getMarkerSymbol(expressedMarkerKey));
				doc.addAllDistinct(DiseasePortalFields.EC_SYNONYM, getMarkerSynonyms(expressedMarkerKey));
				doc.addAllDistinct(DiseasePortalFields.EC_SYNONYM_SINGLE_TOKEN, getMarkerSingleTokenSynonyms(expressedMarkerKey, true));
				doc.addDistinctField(DiseasePortalFields.EC_NAME, getMarkerName(expressedMarkerKey));
				doc.addAllDistinct(DiseasePortalFields.EC_ID, getMarkerIds(expressedMarkerKey));
			}
		}
	}

	/*---------------------------------------------------------------------*/
	/*--- methods dealing with "basic search units" (BSUs) for the grid ---*/
	/*---------------------------------------------------------------------*/

	/* look up data for the BSUs and cache them in memory, assigning a new integer key
	 * to each BSU.  Each BSU's uniqueness is defined by:
	 *		1. marker/disease pair for human data
	 *		2. genocluster/gridcluster pair for mouse data
	 */
	protected void cacheBsus() throws Exception {
		logger.info("entered cacheBsus()");
		if (bsuMap != null) { return; }

		// We need the 'condtionalGenotypes' to determine which genoclusters are conditional.
		if (conditionalGenoclusters == null) { cacheAllelePairs(); }

		Timer.reset();
		logger.info("Caching BSUs");
		int bsuKey = 0;					// incremental key, identifying each BSU
		bsuMap = new HashMap<Integer,BSU>();
		humanBsuMap = new HashMap<Integer,Map<Integer,Integer>>();

		/* Of note:
		 *   1. No genoclusters have human markers.
		 *   2. Many genoclusters have multiple mouse markers.
		 *   3. No markers are in more than one gridcluster.
		 *   4. A small number of markers are not in a homology cluster.
		 *   5. It is possible for a genocluster to have more than one gridcluster,
		 *   	happening often for transgenes.  (in 900+ genoclusters currently)
		 */

		// human marker/disease data, plus homology cluster key
		String humanQuery = "select gcm.hdp_gridcluster_key, gcm.marker_key, " 
				+ "  ha.term_key, hc.cluster_key "
				+ "from hdp_gridcluster_marker gcm "
				+ "inner join hdp_annotation ha on ( "
				+ "  gcm.marker_key = ha.marker_key "
				+ "  and ha.organism_key = 2 " 
				+ "  and ha.annotation_type = 1022) "
				+ "left outer join homology_cluster hc on ( "
				+ "  gcm.hdp_gridcluster_key = hc.cluster_key) "
				+ "order by gcm.marker_key, ha.term_key";

		ResultSet rs = ex.executeProto(humanQuery, cursorLimit);
		while (rs.next()) {
			bsuKey++;
			BSU bsu = new BSU(bsuKey);

			Integer markerKey = rs.getInt("marker_key");
			Integer termKey = rs.getInt("term_key");
			Integer gridclusterKey = rs.getInt("hdp_gridcluster_key");

			bsu.setHumanData(gridclusterKey, markerKey, termKey, rs.getInt("cluster_key"));
			bsuMap.put(bsuKey, bsu);

			if (!humanBsuMap.containsKey(markerKey)) {
				humanBsuMap.put(markerKey, new HashMap<Integer,Integer>());
			}
			if (!humanBsuMap.get(markerKey).containsKey(termKey)) {
				humanBsuMap.get(markerKey).put(termKey, bsuKey);
			}
		}
		rs.close();
		int humanCount = bsuMap.size();
		logger.info("Cached " + humanCount + " human marker/disease BSUs " + Timer.getElapsedMessage());

		// mouse genocluster data

		Timer.reset();

		mouseBsuMap = new HashMap<Integer,Map<Integer,Integer>>();

		String mouseQuery = "select distinct gg.hdp_genocluster_key, gcm.hdp_gridcluster_key "
			+ "from hdp_genocluster_genotype gg "
			+ "left outer join hdp_genocluster_marker gc on (gg.hdp_genocluster_key = gc.hdp_genocluster_key) "
			+ "left outer join hdp_gridcluster_marker gcm on (gc.marker_key = gcm.marker_key) "
			+ "order by gg.hdp_genocluster_key, gcm.hdp_gridcluster_key";

		ResultSet rs2 = ex.executeProto(mouseQuery, cursorLimit);
		
		while (rs2.next()) {
			Integer genoclusterKey = rs2.getInt("hdp_genocluster_key");
			Integer gridclusterKey = rs2.getInt("hdp_gridcluster_key");
			
			bsuKey++;
			BSU bsu = new BSU(bsuKey);

			boolean isConditional = this.isConditional(genoclusterKey);

			bsu.setMouseData(gridclusterKey, genoclusterKey, isConditional);
			bsuMap.put(bsuKey, bsu);
			
			if (!mouseBsuMap.containsKey(genoclusterKey)) {
				mouseBsuMap.put(genoclusterKey, new HashMap<Integer,Integer>());
			}
			mouseBsuMap.get(genoclusterKey).put(gridclusterKey, bsuKey);
		}
		rs2.close();
		logger.info("Cached " + (bsuMap.size() - humanCount) + " mouse genocluster BSUs " + Timer.getElapsedMessage());
	}

	/* get the BSU for the given human marker key and disease term key.  Returns null
	 * if there is no BSU for the pair.
	 */
	protected BSU getHumanBsu(int humanMarkerKey, int termKey) throws Exception {
		if (humanBsuMap == null) { cacheBsus(); }
		if (humanBsuMap.containsKey(humanMarkerKey)) {
			if (humanBsuMap.get(humanMarkerKey).containsKey(termKey)) {
				int bsuKey = humanBsuMap.get(humanMarkerKey).get(termKey);
				return bsuMap.get(bsuKey);
			}
		}
		return null;
	}

	/* get the BSU for the given mouse genocluster/gridcluster pair.  Returns null
	 * if there is no corresponding BSU for that pair.
	 */
	protected BSU getMouseBsu(int genoclusterKey, int gridclusterKey) throws Exception {
		if (mouseBsuMap == null) { cacheBsus(); }
		if (mouseBsuMap.containsKey(genoclusterKey)) {
			if (mouseBsuMap.get(genoclusterKey).containsKey(gridclusterKey)) {
				return bsuMap.get(mouseBsuMap.get(genoclusterKey).get(gridclusterKey));
			}
		}
		return null;
	}

	/* add to the Solr document the data for the HPO terms associated with the given DO term key;
	 * only for use in human marker/disease annotations.
	 */
	protected void addHpoData(DistinctSolrInputDocument doc, Integer diseaseTermKey) throws Exception {
		if (diseaseTermKey == null) { return; }
	
		List<Integer> hpoTermKeys = getHpoTermKeys(diseaseTermKey);
		if (hpoTermKeys == null) { return; }
		
		for (Integer termKey : hpoTermKeys) {
			doc.addDistinctField(DiseasePortalFields.HPO_ID, getTermId(termKey));
			doc.addAllDistinct(DiseasePortalFields.HPO_ID, getAlternateTermIds(termKey));
			doc.addAllDistinct(DiseasePortalFields.HPO_ID, getTermAncestorIDs(termKey));
			doc.addDistinctField(DiseasePortalFields.HPO_TEXT, getTerm(termKey));
			doc.addAllDistinct(DiseasePortalFields.HPO_TEXT, getTermAncestorText(termKey));
			doc.addAllDistinct(DiseasePortalFields.HPO_TEXT, getTermSynonyms(termKey));
		}
	}

	/* private inner class, used to hold the data for a "basic search unit" for the grid --
	 * either a genocluster/gridcluster pair for mouse data or a marker/disease pair for
	 * human data.  BSU class is available within the package.
	 */
	class BSU {
		public int bsuKey;						// unique key for this BSU
		public Integer genoclusterKey;			// key of genocluster for mouse data (optional)
		public Integer gridclusterKey;			// gridcluster key (identifies a grid row)
		public Integer humanMarkerKey;			// key of marker for human data 
		public Integer diseaseKey;				// key of disease term for human data 
		public boolean isMouseData = true;		// is this mouse data (true) or human (false)?
		public boolean isConditional = false;	// conditional mouse genocluster (true) or not (false)?
		public Integer homologyClusterKey;		// key of homology cluster for human data

		public BSU(int bsuKey) {
			this.bsuKey = bsuKey;
		}

		public void setMouseData(Integer gridclusterKey, int genoclusterKey, boolean isConditional) {
			this.gridclusterKey = gridclusterKey;
			this.genoclusterKey = genoclusterKey;
			this.isConditional = isConditional;
			this.isMouseData = true;
		}

		public void setHumanData(Integer gridclusterKey, int humanMarkerKey, int diseaseKey, int homologyClusterKey) {
			this.gridclusterKey = gridclusterKey;
			this.humanMarkerKey = humanMarkerKey;
			this.diseaseKey = diseaseKey;
			this.homologyClusterKey = homologyClusterKey;
			this.isMouseData = false;
			this.isConditional = false;
		}
	}
}
