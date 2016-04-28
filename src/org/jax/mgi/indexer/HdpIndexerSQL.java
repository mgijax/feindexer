package org.jax.mgi.indexer;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;
import org.jax.mgi.shr.fe.query.SolrLocationTranslator;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.org.mgi.shr.fe.util.GridMarker;

/* Is: parent class of the various HMDC-related indexers (Hdp*)
 * Has: knowledge of how to produce various temp tables, mappings, and such
 *   that are useful across the suite of HMDC-related indexers
 */
public abstract class HdpIndexerSQL extends Indexer {
	/* This abstract method is inherited from the parent class (Indexer) and
	 * should be defined in any concrete subclasses.
	 *    abstract void index() throws Exception {}
	 */
	

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	// single instance shared for converting objects into JSON
	protected ObjectMapper mapper = new ObjectMapper();
	
	protected int cursorLimit = 10000;				// number of records to retrieve at once

	protected int uniqueKey = 0;					// (incremental) unique key for index documents

	protected String omim = "OMIM";					// vocab name for disease terms
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
	protected Map<Integer,Set<Integer>> markerOrthologs = null;	// marker key -> set of ortholog marker keys

	protected Map<String,Set<String>> markersPerDisease = null;	// disease ID -> marker keys
	protected Map<String,Set<String>> headersPerDisease = null;	// disease ID -> header terms
	protected Map<String,Integer> refCountPerDisease = null;	// disease ID -> count of refs
	protected Map<String,Integer> modelCountPerDisease = null;	// disease ID -> count of models

	protected Map<String,Set<String>> termAlternateIds = null;	// term ID -> alternate term IDs
	protected Set<Integer> omimTerms = null;					// set of term keys for OMIM terms
	protected Set<Integer> mpTerms = null;						// set of term keys for MP terms
	protected Set<Integer> hpoTerms = null;						// set of term keys for HPO terms
	protected Map<Integer,String> terms = null;					// term key -> term
	protected Map<Integer,String> termIds = null;				// term key -> primary term ID
	protected Map<Integer,Set<Integer>> relatedAnnotations = null;	// annot key -> set of related annot keys
	protected Map<Integer,Integer> annotationTermKeys = null;		// annot key -> term key of annotation
	protected Map<Integer,Set<Integer>> termKeyToAnnotations = null;	// term key -> set of annotation keys
	protected Set<Integer> notAnnotations = null;				// set of annotations keys with NOT qualifiers
	protected Map<Integer,Set<Integer>> termAncestors = null;	// term key -> set of its ancestor term keys

	protected Map<Integer,Set<Integer>> omimToHpo = null;		// OMIM term key -> set of HPO term keys
	protected Map<Integer,Set<Integer>> hpoHeaderToMp = null;	// HPO header key -> set of MP header keys
	
	/* We use the term "basic search units" for the grid, referring to the basic unit that we
	 * are searching for -- genoclusters for mouse data and marker/disease pairs for human
	 * data.  If our search matches a BSU, then all of its annotations are returned.  If a BSU
	 * was not matched by the search, then none of its data are returned.
	 */
	protected Map<Integer,BSU> bsuMap = null;				// maps BSU key to its cached data
	Map<Integer,Map<Integer,Integer>> humanBsuMap = null;	// marker key -> disease key -> BSU key
	Map<Integer,Map<Integer,Integer>> mouseBsuMap = null;	// genocluster key -> gridcluster key -> BSU key

	Map<Integer,String> gcToHumanMarkers = null;	// maps gridcluster key to human marker data as JSON
	Map<Integer,String> gcToMouseMarkers = null;	// maps gridcluster key to mouse marker data as JSON
	Map<Integer,String> allelePairs = null;			// maps genocluster key to allele pair data
	Set<Integer> conditionalGenoclusters = null;	// set of conditional genocluster keys

	// genocluster key to set of corresponding grid cluster keys
	protected Map<Integer,Set<Integer>> genoclusterToGridcluster = null;
	
	// gridcluster key to feature types for mouse markers in the cluster
	protected Map<String,Set<String>> featureTypeMap = null;

	// name of table mapping annotations to genoclusters
	protected String annotationToGenocluster = null;

	// name of table like hdp_annotation table, but with only non-normal annotations
	protected String nonNormalAnnotations = null;

	// name of table which joins each annotation in a genocluster to each other
	// annotation for the genocluster, to allow display of all annotations for
	// the genocluster, even if only a subset matched
	protected String annotationCrossProduct = null;

	// name of table which maps from a disease annotation to all other diseases for
	// that genocluster
	protected String diseaseToDisease = null;

	// name of the table which maps from a disease annotation to all phenotypes for
	// that genocluster
	protected String diseaseToPhenotype = null;

	// name of the table which maps from a phenotype annotation to all diseases for
	// that genocluster
	protected String phenotypeToDisease = null;

	// name of the table which maps from a phenotype annotation to disease annotations
	// for the same genocluster
	protected String phenotypeToDiseaseViaGenocluster = null;

	// name of the table which maps from a phenotype annotation to disease annotations
	// for the same genotype (not genocluster)
	protected String phenotypeToDiseaseViaGenotype = null;

	// name of the table which maps from a phenotype annotation to other phenotype
	// annotations for the same genocluster
	protected String phenotypeToPhenotype = null;

	// maps from each disease and phenotype term to its smart-alpha sequence number
	protected Map<String,Integer> termSortMap = null;

	// one higher than the largest sequence number in 'termSortMap'
	protected int maxTermSeqNum = 0;

	// mapping from each disease and phenotype ID to the corresponding term's synonyms
	protected Map<String,Set<String>> termSynonymMap = null;

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	protected HdpIndexerSQL(String httpPropName) {
		super(httpPropName);
	}

	/*------------------------------------------------------*/
	/*--- methods for dealing with data cached in memory ---*/
	/*------------------------------------------------------*/

	/* retrieve the mapping from each (Integer) term key to a Set of its (String) ancestor term keys
	 */
	protected Map<Integer,Set<Integer>> getTermAncestors() throws Exception {
		if (termAncestors != null) { return null; }

		logger.info("retrieving ancestors of terms");
		Timer.reset();

		String ancestorQuery = "select ta.term_key, ta.ancestor_term_key "
				+ "from term_ancestor ta "
				+ "where exists (select 1 from term t "
				+ "  where t.vocab_name in ('OMIM', 'Mammalian Phenotype', 'Human Phenotype Ontology') "
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

	/* get the ancestor term keys for the given term key, or null if there are no ancestors
	 */
	protected Set<Integer> getTermAncestorsold(Integer termKey) throws Exception {
		if (termAncestors == null) { getTermAncestors(); }
		if (termAncestors.containsKey(termKey)) { return termAncestors.get(termKey); }
		return null;
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
				+ "  where t.vocab_name in ('OMIM', 'Mammalian Phenotype', 'Human Phenotype Ontology') "
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
					+ "  and t.vocab_name in ('Mammalian Phenotype', 'OMIM', 'Human Phenotype Ontology') ";

			termAlternateIds = populateLookup(termIdQuery,"term_id","alt_id","alternate IDs to term IDs");

			logger.info("finished retrieving alternate IDs" + Timer.getElapsedMessage());
		}
		return termAlternateIds;
	}

	/* get the alternate term IDs for the term identified by the given primary ID
	 */
	protected Set<String> getAlternateTermIds(String termID) throws Exception {
		if (termAlternateIds == null) { getAlternateTermIds(); }
		if (termAlternateIds.containsKey(termID)) {
			return termAlternateIds.get(termID);
		}
		return null;
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
					"and t.vocab_name in ('OMIM','Mammalian Phenotype', 'Human Phenotype Ontology') ";
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
				"where vocab_name in ('OMIM','Mammalian Phenotype', 'Human Phenotype Ontology') ";
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
					+ "where logical_db not in ('ABA','Download data from the QTL Archive','FuncBase','GENSAT','GEO','HomoloGene','RIKEN Cluster','UniGene') ";
			markerAllIdMap = populateLookup(markerIdQuery,"marker_key","acc_id","marker keys to IDs");

			logger.info("Finished loading IDs for " + markerAllIdMap.size() + " markers" + Timer.getElapsedMessage());
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
			+ "from hdp_genocluster gc, "
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

	/* get a mapping from (String) disease ID to a Set of (String) marker keys that are
	 * positively associated with that disease.  (no annotations with a NOT qualifier)
	 */
	protected Map<String,Set<String>> getMarkersPerDisease() throws Exception {
		if (markersPerDisease == null) {
			logger.info("Retrieving markers per disease");
			Timer.reset();

			// This had used getNonNormalAnnotationsTable() as a source, but optimizing to
			// bring the 'where' clause up into this query and simplify.
			String markerQuery = "select distinct h.term_id, h.marker_key, m.symbol "
					+ "from hdp_annotation h, marker m "
					+ "where h.vocab_name='OMIM' "
					+ "  and h.organism_key in (1, 2) "
					+ "  and h.marker_key = m.marker_key "
					+ "  and h.qualifier_type is null "
					+ "  and (h.genotype_type!='complex' or h.genotype_type is null) "
					+ "order by h.term_id, m.symbol";

			markersPerDisease = populateLookupOrdered(markerQuery,
					"term_id", "marker_key", "diseases to markers");

			logger.info("Finished retrieving markers for (" + markersPerDisease.size() + " diseases)" + Timer.getElapsedMessage());
		}
		return markersPerDisease;
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
	protected Map<String,Set<String>> getHeadersPerDisease() throws Exception {
		if (headersPerDisease == null) {
			logger.info("Retrieving headers per disease");
			Timer.reset();

			String headerQuery = "select distinct term_id, header "
					+ "from hdp_annotation "
					+ "where vocab_name = '" + omim + "' "
					+ "order by term_id, header";
			headersPerDisease = populateLookupOrdered(headerQuery, "term_id", "header", "diseases to headers");

			logger.info("Finished retrieving headers for (" + headersPerDisease.size() + " diseases)" + Timer.getElapsedMessage());
		}
		return headersPerDisease;
	}

	/* get the headers for the disease specified by 'diseaseID'
	 */
	protected Set<String> getHeadersByDisease (String diseaseID) throws Exception {
		if (headersPerDisease == null) { getHeadersPerDisease(); }
		if (headersPerDisease.containsKey(diseaseID)) {
			return headersPerDisease.get(diseaseID);
		}
		return null;
	}

	/* get the headers for the disease specified by the given term key
	 */
	protected Set<String> getHeadersByDisease (Integer diseaseKey) throws Exception {
		if (diseaseKey == null) { return null; }
		String termId = getTermId(diseaseKey);
		if (termId == null) { return null; }
		return getHeadersByDisease(termId);
	}

	/* get a mapping from (String) disease ID to an (Integer) count of references
	 */
	protected Map<String,Integer> getDiseaseReferenceCounts() throws Exception {
		if (refCountPerDisease == null) {
			Timer.reset();
			refCountPerDisease = new HashMap<String,Integer>();

			logger.info("building counts of disease relevant refs to disease ID");
			String diseaseRefCountQuery = "select ha.term_id as disease_id, "
					+ "  count(distinct trt.reference_key) as ref_count "
					+ "from hdp_term_to_reference trt, "
					+ "  hdp_annotation ha "
					+ "where ha.term_key=trt.term_key "
					+ "  and ha.vocab_name='OMIM' "
					+ "group by disease_id ";

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
					+ "  and o.organism in ('mouse', 'human') "
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

			String diseaseModelQuery="select dm.disease_id, count(dm.disease_model_key) diseaseModelCount " +
					"from disease_model dm " +
					"where is_not_model=0 " +
					"group by disease_id ";

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
	 * specifically populates omimTerms, mpTerms, hpoTerms, terms, and termIds.
	 */
	protected void cacheBasicTermData() throws Exception {
		if (omimTerms != null) { return; }

		logger.info("Caching basic term data");
		Timer.reset();

		omimTerms = new HashSet<Integer>();
		mpTerms = new HashSet<Integer>();
		hpoTerms = new HashSet<Integer>();
		terms = new HashMap<Integer,String>();
		termIds = new HashMap<Integer,String>();

		String termQuery = "select term_key, term, primary_id, vocab_name "
				+ "from term "
				+ "where vocab_name in ('OMIM', 'Mammalian Phenotype', 'Human Phenotype Ontology')";

		ResultSet rs = ex.executeProto(termQuery, cursorLimit);
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			terms.put(termKey, rs.getString("term"));
			termIds.put(termKey, rs.getString("primary_id"));

			if (omim.equals(rs.getString("vocab_name"))) { omimTerms.add(termKey); }
			else if (hpo.equals(rs.getString("vocab_name"))) { hpoTerms.add(termKey); }
			else { mpTerms.add(termKey); }
		}
		rs.close();

		logger.info("Finished retrieving basic term data (" + terms.size() + " terms)" + Timer.getElapsedMessage());
	}

	/* get the vocabulary name for the given term key, or null if key is unknown
	 */
	protected String getVocabulary(Integer termKey) throws Exception {
		if (omimTerms == null) { cacheBasicTermData(); }
		if (omimTerms.contains(termKey)) { return omim; }
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
		termKeyToAnnotations = new HashMap<Integer,Set<Integer>>();

		String annotQuery = "select hdp_annotation_key, term_key, qualifier_type "
				+ "from hdp_annotation";

		ResultSet rs = ex.executeProto(annotQuery, cursorLimit);
		while (rs.next()) {
			Integer hdpAnnotationKey = rs.getInt("hdp_annotation_key");
			String qualifier = rs.getString("qualifier_type");
			Integer termKey = rs.getInt("term_key");

			annotationTermKeys.put(hdpAnnotationKey, rs.getInt("term_key"));
			if ((qualifier != null) && "NOT".equals(qualifier)) {
				notAnnotations.add(hdpAnnotationKey);
			}

			if (!termKeyToAnnotations.containsKey(termKey)) {
				termKeyToAnnotations.put(termKey, new HashSet<Integer>());
			}
			termKeyToAnnotations.get(termKey).add(hdpAnnotationKey);
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

	/* get the annotations using the given term key, or null if none exist
	 */
	protected Set<Integer> getAnnotationsForTerm(Integer termKey) throws Exception {
		if (termKeyToAnnotations == null) { cacheBasicAnnotationData(); }
		if (termKeyToAnnotations.containsKey(termKey)) { return termKeyToAnnotations.get(termKey); }
		return null;
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

		/* Commented out this section, ensuring that we do NOT bring back additional diseases for
		 *  human markers.  We only want to bring back the disease that matches the user's query.
		 * 		Timer.reset();
		 *		String humanQuery = "select ha1.hdp_annotation_key as annotKey1, "
		 *			+ "  ha2.hdp_annotation_key as annotKey2 "
		 *			+ "from hdp_annotation ha1, "
		 *			+ "  hdp_annotation ha2 "
		 *			+ "where ha1.term_id != ha2.term_id "
		 *			+ "  and ha1.marker_key = ha2.marker_key "
		 *			+ "  and ha1.organism_key = 2 "
		 *			+ "  and ha2.organism_key = 2";
		 *
		 *		ResultSet rs2 = ex.executeProto(humanQuery, cursorLimit);
		 *		while (rs2.next()) {
		 *			Integer annotKey1 = rs2.getInt("annotKey1");
		 *			
		 *			if (!relatedAnnotations.containsKey(annotKey1)) {
		 *				relatedAnnotations.put(annotKey1, new HashSet<Integer>());
		 *			}
		 *			relatedAnnotations.get(annotKey1).add(rs2.getInt("annotKey2"));
		 *		}
		 *		rs2.close();
		 *			
		 *		logger.info("Got relationships for " + (relatedAnnotations.size() - mouseCount) + " human annotations)" + Timer.getElapsedMessage());
		 */
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
	protected Set<String> getRelatedTerms (Integer annotationKey, boolean getTerms, boolean getIds,
			boolean getDiseases, boolean getPhenotypes) throws Exception {

		Set<Integer> relatedAnnot = getRelatedAnnotations(annotationKey);
		if (relatedAnnot == null) { return null; }

		Set<String> out = new HashSet<String>();		// set of strings to return

		for (Integer annotKey : relatedAnnot) {
			Integer termKey = getAnnotatedTermKey(annotKey);
			String vocab = getVocabulary(termKey);
			if ( (getDiseases && omim.equals(vocab)) || (getPhenotypes && mp.equals(vocab)) ) {
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

	/* get the set of disease IDs and terms which come via related annotations for the specified term key
	 */
	protected Set<String> getRelatedDiseasesForTerm(Integer termKey, boolean getTerms, boolean getIds) throws Exception {
		Set<Integer> annotKeys = getAnnotationsForTerm(termKey);
		if (annotKeys != null) {
			Set<String> out = new HashSet<String>();
			for (Integer annotKey : annotKeys) {
				Set<String> relatedTerms = getRelatedDiseases(annotKey, getTerms, getIds);
				if (relatedTerms != null) {
					out.addAll(relatedTerms);
				}
			}
			return out;
		}
		return null;
	}

	/* get the set of phenotype IDs and terms which come via related annotations for the specified term key
	 */
	protected Set<String> getRelatedPhenotypesForTerm(Integer termKey, boolean getTerms, boolean getIds) throws Exception {
		Set<Integer> annotKeys = getAnnotationsForTerm(termKey);
		if (annotKeys != null) {
			Set<String> out = new HashSet<String>();
			for (Integer annotKey : annotKeys) {
				Set<String> relatedTerms = getRelatedPhenotypes(annotKey, getTerms, getIds);
				if (relatedTerms != null) {
					out.addAll(relatedTerms);
				}
			}
			return out;
		}
		return null;
	}

	/*-----------------------------------------------*/
	/*--- public methods for building temp tables ---*/
	/*-----------------------------------------------*/

	/* returns the name of the table which maps each annotation to its genocluster,
	 * building the table if necessary
	 */
	public String getAnnotationToGenoclusterTable() {
		if (this.annotationToGenocluster == null) {
			// need to build the temp table and index it appropriately

			Timer.reset();
			this.annotationToGenocluster = "tmp_ha_genocluster";
			logger.info("creating " + this.annotationToGenocluster + " (temp table of hdp_annotation to hdp_genocluster_key)");

			// The 'distinct on' keeps the first record seen for each distinct hdp_annotation_key.
			String genoClusterKeyQuery = "select distinct on(hdp_annotation_key) hdp_annotation_key, hdp_genocluster_key\n" + 
					"into temp " + this.annotationToGenocluster + " \n" + 
					"from hdp_annotation ha,\n" + 
					"	hdp_genocluster_genotype gcg\n" + 
					"where ha.genotype_key=gcg.genotype_key";

			fillTempTable(genoClusterKeyQuery);
			createTempIndex(this.annotationToGenocluster, "hdp_annotation_key");
			createTempIndex(this.annotationToGenocluster, "hdp_genocluster_key");
			analyze(this.annotationToGenocluster);

			logger.info("done creating " + this.annotationToGenocluster + Timer.getElapsedMessage());
		}
		return this.annotationToGenocluster;
	}

	/* returns the name of the table which contains non-normal annotations,
	 * building the table if necessary
	 */
	public String getNonNormalAnnotationsTable() {
		if (this.nonNormalAnnotations == null) {
			// need to build the temp table and index it appropriately

			Timer.reset();
			this.nonNormalAnnotations = "tmp_hdp_annotation_nn";
			logger.info("creating " + this.nonNormalAnnotations + " (temp table of hdp_annotation minus normals)");

			String noNormalsHdpQuery="select * " +
					"INTO TEMP " + this.nonNormalAnnotations + " " +
					"from hdp_annotation " +
					"where qualifier_type is null ";

			fillTempTable(noNormalsHdpQuery);
			createTempIndex(this.nonNormalAnnotations, "hdp_annotation_key");
			createTempIndex(this.nonNormalAnnotations, "genotype_key");
			createTempIndex(this.nonNormalAnnotations, "marker_key");
			createTempIndex(this.nonNormalAnnotations, "term_key");
			createTempIndex(this.nonNormalAnnotations, "term_id");
			analyze(this.nonNormalAnnotations);

			logger.info("done creating " + this.nonNormalAnnotations + Timer.getElapsedMessage());
		}
		return this.nonNormalAnnotations;
	}

	/* returns the name of the table with the cross-product of annotations
	 * (each annotation for a genocluster related to every other annotation
	 * in the genocluster)
	 */
	public String getAnnotationCrossProductTable() {
		if (this.annotationCrossProduct == null) {
			// need to build the temp table and index it appropriately

			this.annotationCrossProduct = "tmp_ha_cross";
			Timer.reset();
			logger.info("creating " + this.annotationCrossProduct + "(hdp_annotation cross hdp_annotation via genocluster)");

			// two-column temp table (hdp_annotation_key, hdp_genocluster_key)
			String gcTable = this.getAnnotationToGenoclusterTable();

			// first add mouse annotations (via genoclusters) to the temp table
			String hdpMouseAnnotationCrossQuery ="select ha1.hdp_annotation_key ha_key1,\n" + 
					"ha1.term_id term_id1,\n" + 
					"ha1.term term1,\n" + 
					"ha1.vocab_name vocab1,\n" + 
					"ha2.hdp_annotation_key ha_key2,\n" + 
					"ha2.term_id term_id2,\n" + 
					"ha2.term term2,\n" + 
					"ha2.vocab_name vocab2\n" + 
					"into temp " + this.annotationCrossProduct + "\n" +
					"from hdp_annotation ha1,\n" + 
					gcTable + " gc1,\n" + 
					"hdp_annotation ha2,\n" + 
					gcTable + " gc2\n" + 
					"where ha1.hdp_annotation_key = gc1.hdp_annotation_key\n" + 
					"and ha2.hdp_annotation_key = gc2.hdp_annotation_key\n" +  
					"and ha1.term_id != ha2.term_id "+
					"and gc1.hdp_genocluster_key = gc2.hdp_genocluster_key";
			fillTempTable(hdpMouseAnnotationCrossQuery);

			// then add human annotations (direct to markers) to the temp table
			String hdpHumanAnnotationCrossQuery = "insert into " + this.annotationCrossProduct + " " +
					"select ha1.hdp_annotation_key ha_key1, ha1.term_id term_id1, ha1.term term1, ha1.vocab_name vocab1, " +
					"ha2.hdp_annotation_key ha_key2, ha2.term_id term_id2, ha2.term term2, ha2.vocab_name vocab2 " +
					"from hdp_annotation ha1, hdp_annotation ha2 " +
					"where ha1.term_id != ha2.term_id " +
					"and ha1.marker_key = ha2.marker_key " +
					"and ha1.organism_key=2 and ha2.organism_key=2";
			fillTempTable(hdpHumanAnnotationCrossQuery);

			// only two fields are used in WHERE clauses, so we'll index just those
			createTempIndex(this.annotationCrossProduct, "vocab1");
			createTempIndex(this.annotationCrossProduct, "vocab2");
			analyze(this.annotationCrossProduct);
			logger.info("done creating " + this.annotationCrossProduct + Timer.getElapsedMessage());
		}
		return this.annotationCrossProduct;
	}

	/* returns the name of the table with the mapping from each disease annotation
	 * to all other diseases for the same genocluster
	 */
	public String getDiseaseToDiseaseTable() {
		if (this.diseaseToDisease == null) {
			// need to build the temp table and index it appropriately

			Timer.reset();
			String crossTable = this.getAnnotationCrossProductTable();
			this.diseaseToDisease = "tmp_disease_to_disease";
			logger.info("creating " + this.diseaseToDisease + "(diseases to diseases via genocluster)");

			String diseaseToDiseaseQuery = "select hc.term_id1 omim_id, \n" + 
					"        	hc.term1 omim_term, \n" + 
					"			hc.ha_key2 hdp_annotation_key \n" + 
					"		into temp " + this.diseaseToDisease + " \n" + 
					"			from " + crossTable + " \n" + 
					"			where hc.vocab1='OMIM' \n" + 
					"				and hc.vocab2='OMIM' ";
			fillTempTable(diseaseToDiseaseQuery);
			createTempIndex(this.diseaseToDisease, "hdp_annotation_key");			
			analyze(this.diseaseToDisease);
			logger.info("done creating " + this.diseaseToDisease + Timer.getElapsedMessage());
		}
		return this.diseaseToDisease;
	}

	/* returns the name of the table with the mapping from each disease annotation
	 * to all phenotypes for the same genocluster
	 */
	public String getDiseaseToPhenotypeTable() {
		if (this.diseaseToPhenotype == null) {
			// need to build the temp table and index it appropriately

			Timer.reset();
			String crossTable = this.getAnnotationCrossProductTable();
			this.diseaseToPhenotype = "tmp_disease_to_mp";
			logger.info("creating " + this.diseaseToPhenotype + "(diseases to phenotypes via genocluster)");

			String diseaseToMpQuery = "select hc.term_id2 omim_id, \n" + 
					"        	hc.term2 omim_term, \n" + 
					"			hc.ha_key1 hdp_annotation_key\n" + 
					"		into temp " + this.diseaseToPhenotype + " \n" + 
					"			from " + crossTable + " hc \n" + 
					"			where hc.vocab1='Mammalian Phenotype' \n" + 
					"				and hc.vocab2='OMIM'";
			fillTempTable(diseaseToMpQuery);
			createTempIndex(this.diseaseToPhenotype, "hdp_annotation_key");
			analyze(this.diseaseToPhenotype);
			logger.info("done creating " + this.diseaseToPhenotype + Timer.getElapsedMessage());
		}
		return this.diseaseToPhenotype;
	}

	/* returns the name of the table with the mapping from each phenotype annotation
	 * to disease annotations for the same genocluster
	 */
	public String getPhenotypeToDiseaseViaGenocluster() {
		if (this.phenotypeToDiseaseViaGenocluster == null) {
			// need to build the temp table and index it appropriately

			Timer.reset();
			String crossTable = this.getAnnotationCrossProductTable();
			this.phenotypeToDiseaseViaGenocluster = "tmp_mp_to_ss_disease";
			logger.info("creating " + this.phenotypeToDiseaseViaGenocluster + "(phenotypes to diseases via genocluster)");
			String mpToSSDiseaseQuery = "select hc.term_id1 mp_id, \n" + 
					"        	hc.term1 mp_term, \n" + 
					"			hc.ha_key2 hdp_annotation_key\n" + 
					"		into temp " + this.phenotypeToDiseaseViaGenocluster + " \n" + 
					"			from " + crossTable + " \n" + 
					"			where hc.vocab1='Mammalian Phenotype' \n" + 
					"				and hc.vocab2='OMIM'";
			fillTempTable(mpToSSDiseaseQuery);
			createTempIndex(this.phenotypeToDiseaseViaGenocluster, "hdp_annotation_key");
			analyze(this.phenotypeToDiseaseViaGenocluster);
			logger.info("done creating " + this.phenotypeToDiseaseViaGenocluster + Timer.getElapsedMessage());
		}
		return this.phenotypeToDiseaseViaGenocluster;
	}

	/* returns the name of the table with the mapping from each phenotype annotation
	 * to disease annotations for the same genotype
	 */
	public String getPhenotypeToDiseaseViaGenotype() {
		if (this.phenotypeToDiseaseViaGenotype == null) {
			// need to build the temp table and index it appropriately

			Timer.reset();
			this.phenotypeToDiseaseViaGenotype = "tmp_mp_to_disease";
			logger.info("creating " + this.phenotypeToDiseaseViaGenotype + "(phenotypes to diseases via genotype)");
			String mpToDiseaseQuery = "select distinct ha_mp.term_id mp_id, \n" + 
					"        	ha_mp.term mp_term, \n" + 
					"			ha_omim.hdp_annotation_key \n" + 
					"		into temp " + this.phenotypeToDiseaseViaGenotype + " \n" + 
					"			from hdp_annotation ha_mp, \n" + 
					"				hdp_annotation ha_omim\n" + 
					"			where ha_mp.genotype_key=ha_omim.genotype_key \n" + 
					"				and (ha_mp.genotype_type!='complex' or ha_mp.genotype_type is null) \n" + 
					"				and (ha_omim.genotype_type!='complex' or ha_omim.genotype_type is null) \n" + 
					"				and ha_mp.vocab_name='Mammalian Phenotype' \n" + 
					"				and ha_omim.vocab_name='OMIM'";
			fillTempTable(mpToDiseaseQuery);
			createTempIndex(this.phenotypeToDiseaseViaGenotype, "hdp_annotation_key");
			analyze(this.phenotypeToDiseaseViaGenotype);
			logger.info("done creating " + this.phenotypeToDiseaseViaGenotype + Timer.getElapsedMessage());
		}
		return this.phenotypeToDiseaseViaGenotype;
	}

	/* returns the name of the table with the mapping from each phenotype annotation
	 * to other phenotype annotations for the same genocluster
	 */
	public String getPhenotypeToPhenotype() {
		if (this.phenotypeToPhenotype == null) {
			// need to build the temp table and index it appropriately

			Timer.reset();
			String crossTable = this.getAnnotationCrossProductTable();
			this.phenotypeToPhenotype = "tmp_mp_to_mp";
			logger.info("creating " + this.phenotypeToPhenotype + "(phenotypes to phenotypes via genocluster)");

			String mpToMpQuery = "select hc.term_id1 mp_id, \n" +
					"        	hc.term1 mp_term, \n" +
					"			hc.ha_key2 hdp_annotation_key\n" +
					"		into temp " + this.phenotypeToPhenotype + " \n" +
					"			from " + crossTable + " hc \n" +
					"			where hc.vocab1='Mammalian Phenotype' \n" +
					"				and hc.vocab2='Mammalian Phenotype'";
			fillTempTable(mpToMpQuery);
			createTempIndex(this.phenotypeToPhenotype, "hdp_annotation_key");
			analyze(this.phenotypeToPhenotype);
			logger.info("done creating " + this.phenotypeToPhenotype + Timer.getElapsedMessage());
		}
		return this.phenotypeToDiseaseViaGenotype;
	}

	/*------------------------------------------------*/
	/*--- methods relating to annotation retrieval ---*/
	/*------------------------------------------------*/

	/* get the maximum annotation_key from the hdp_annotation table (to use in stepping
	 * through chunks of annotations)
	 */
	public int getMaxAnnotationKey() throws SQLException {
		ResultSet rs_tmp = ex.executeProto("select max(hdp_annotation_key) as max_hdp_key from hdp_annotation", cursorLimit);
		rs_tmp.next();
		int i = rs_tmp.getInt("max_hdp_key");
		rs_tmp.close();
		logger.info("Got max annotation key: " + i);
		return i;
	}

	/* get a ResultSet with annotation data, from the given start key (exclusive) to
	 * the given end key (inclusive).  See SQL included below for list of columns.
	 */
	public ResultSet getAnnotations(int startKey, int endKey) throws SQLException {
		String cmd = "select ha.hdp_annotation_key, " +
				"ha.marker_key, " +
				"ha.genotype_key, " +
				"ha.term, " +
				"ha.term_id, " +
				"ha.vocab_name, " +
				"ha.genotype_type, " +
				"ha.header as term_header, " +
				"ha.qualifier_type, " +
				"ha.term_seq, " +
				"ha.term_depth, " +
				"m.organism, " +
				"m.symbol as marker_symbol, " +
				"m.primary_id as marker_id, " +
				"m.name as marker_name, " +
				"m.marker_subtype as feature_type, " +
				"m.location_display, " +
				"m.coordinate_display, " +
				"m.build_identifier, " +
				"msqn.by_organism, " +
				"msqn.by_symbol, " +
				"msqn.by_marker_subtype, " +
				"msqn.by_location, " +
				"mc.reference_count, " +
				"mc.disease_relevant_reference_count, " +
				"gcm.hdp_gridcluster_key, " +
				"gcg.hdp_genocluster_key, " +
				"gsn.by_hdp_rules by_genocluster "+
				"from hdp_annotation ha " +
				"left outer join hdp_gridcluster_marker gcm on gcm.marker_key=ha.marker_key " +
				"left outer join hdp_genocluster_genotype gcg on gcg.genotype_key=ha.genotype_key " +
				"left outer join genotype_sequence_num gsn on gsn.genotype_key=ha.genotype_key, " +
				"marker m, " +
				"marker_sequence_num msqn, " +
				"marker_counts mc " +
				"where m.marker_key=ha.marker_key " +
				"and m.marker_key=msqn.marker_key " +
				"and m.marker_key=mc.marker_key " +
				"and ha.hdp_annotation_key > " + startKey +
				" and ha.hdp_annotation_key <= " + endKey;

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.info("  - Got annotations for keys " + (startKey + 1) + " to " + endKey);
		return rs;
	}

	/* fill the set of caches needed to populate the standard search fields (in the
	 * addStandardFields method).
	 */
	protected void fillStandardCaches(int startKey, int endKey) throws Exception {
		getHomologyMap();
		getFeatureTypeMap();
	}

	/* add the standard search fields to the given Solr document, using the
	 * current record in the given ResultSet as its source data.  This method should
	 * be called by the buildDocument method of each subclass.  See getAnnotations()
	 * method for column names in 'rs'.
	 */
	protected void addStandardFields(ResultSet rs, SolrInputDocument doc) throws SQLException {
		// local variables to cache values used multiple times
		String vocabName = rs.getString("vocab_name");
		String term = rs.getString("term");
		String termID = rs.getString("term_id");
		String termHeader = rs.getString("term_header");
		String qualifier = rs.getString("term_qualifier");
		Integer markerKey = rs.getInt("marker_key");
		Integer gridClusterKey = rs.getInt("hdp_gridcluster_key");

		// tweaks for the local variables
		if (qualifier == null) { qualifier = ""; }

		if (homologyMap.containsKey(markerKey)) {
			// override with hybrid homology-based cluster key
			gridClusterKey = homologyMap.get(markerKey);
		}

		if ("OMIM".equalsIgnoreCase(vocabName) && (termHeader == null || "".equals(termHeader))) {
			termHeader = term;
		}

		// document-identification fields
		uniqueKey += 1;
		doc.addField(DiseasePortalFields.UNIQUE_KEY, uniqueKey);

		// term-related fields
		doc.addField(DiseasePortalFields.TERM, term);
		doc.addField(DiseasePortalFields.TERM_ID, termID);
		doc.addField(DiseasePortalFields.BY_TERM_NAME, getTermSequenceNum(term));
		doc.addField(DiseasePortalFields.TERM_HEADER, termHeader);
		doc.addField(DiseasePortalFields.TERM_TYPE, vocabName);
		addAllFromLookup(doc, DiseasePortalFields.TERM_SYNONYM, termID, termSynonymMap);

		// marker-related fields
		if ((markerKey != null) && (markerKey >= 0)) {
			doc.addField(DiseasePortalFields.MARKER_KEY, markerKey);
			doc.addField(DiseasePortalFields.MARKER_SYMBOL, rs.getString("marker_symbol"));
			doc.addField(DiseasePortalFields.MARKER_NAME, rs.getString("marker_name"));
		}

		// gridcluster-related fields
		if ((gridClusterKey != null) && (gridClusterKey >= 0)) {
			doc.addField(DiseasePortalFields.GRID_CLUSTER_KEY, gridClusterKey);
			addAllFromLookup(doc, DiseasePortalFields.FILTERABLE_FEATURE_TYPES, gridClusterKey.toString(), featureTypeMap);
		}

		// other fields
		doc.addField(DiseasePortalFields.TERM_QUALIFIER, qualifier);
	}

	/* populate any memory caches needed to process annotations from the given start
	 * key (exclusive) to the given end key (inclusive).  Override this method in
	 * subclasses, if any memory caches are necessary.
	 */
	public void fillCachesForAnnotations(int startKey, int endKey) throws SQLException {}

	/* build and return a SolrInputDocument based on the current record in the
	 * given ResultSet.  Override this method in subclasses.
	 */
	public SolrInputDocument buildDocument(ResultSet rs) throws SQLException { return null; }

	/* main method for processing disease and phenotype annotations, including
	 * walking through the results in chunks and using a method that can be 
	 * overridden in each subclass for specialized document creation.
	 */
	public void processAnnotations() throws Exception {
		int maxAnnotationKey = this.getMaxAnnotationKey();
		int start = 0;					// start annotation key for the current chunk
		int end = start + dbChunkSize;	// end annotation key for the current chunk

		logger.info("Processing annotations 1 to " + maxAnnotationKey);
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		while (start < maxAnnotationKey) {
			fillStandardCaches(start, end);
			fillCachesForAnnotations(start, end);
			ResultSet rs = getAnnotations(start, end);

			while (rs.next()) {
				docs.add(buildDocument(rs));
				if (docs.size() >= solrBatchSize) {
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>();
					logger.info("  - sent batch of docs to Solr");
				}
			}

			rs.close();
			start = end;
			end = end + dbChunkSize;
		}

		writeDocs(docs);
		commit();
		logger.info("Done processing annotations 1 to " + maxAnnotationKey);
	}

	/*---------------------------------------------------------------------*/
	/*--- methods dealing with "basic search units" (BSUs) for the grid ---*/
	/*---------------------------------------------------------------------*/

	/* cache the human and mouse marker data for each grid cluster
	 */
	protected void cacheGridClusterMarkers() throws Exception {
		if (gcToHumanMarkers != null) { return; }

		logger.info("retrieving gridcluster markers");
		Timer.reset();

		gcToHumanMarkers = new HashMap<Integer,String>();
		gcToMouseMarkers = new HashMap<Integer,String>();

		String markerQuery = "select gcm.hdp_gridcluster_key, m.organism, m.symbol, "
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
				humanGM.add(new GridMarker(symbol, accId, name, markerType));
			} else {
				mouseGM.add(new GridMarker(symbol, accId, name, markerSubType));
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
			allelePairs.put(rs.getInt("hdp_genocluster_key"), rs.getString("combination_1"));
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

//	protected Map<Integer,Set<Integer>> omimToHpo = null;		// OMIM term key -> set of HPO term keys
//	protected Map<Integer,Set<Integer>> hpoHeaderToMp = null;	// HPO term key -> set of MP header keys
	
	/* populate the caches of OMIM terms to HPO terms and of HPO header terms to MP header terms
	 */
	protected void cacheHpoMaps() throws Exception {
		// get the mapping from OMIM term to HPO terms

		logger.info("retrieving HPO mappings");
		Timer.reset();

		String omimToHpoQuery = "select term_key_1 as omim_key, term_key_2 as hpo_key "
			+ "from term_to_term tt "
			+ "where tt.relationship_type = 'OMIM to HPO'";
		
		omimToHpo = new HashMap<Integer,Set<Integer>>();

		ResultSet rs = ex.executeProto(omimToHpoQuery, cursorLimit);
		while (rs.next()) {
			Integer omimKey = rs.getInt("omim_key");
			Integer hpoKey = rs.getInt("hpo_key");
			
			if (!omimToHpo.containsKey(omimKey)) {
				omimToHpo.put(omimKey, new HashSet<Integer>());
			}
			omimToHpo.get(omimKey).add(hpoKey);
		}
		rs.close();
		logger.info(" - got HPO terms for " + omimToHpo.size() + " OMIM terms " + Timer.getElapsedMessage());
		
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

	/* get the term keys for the HPO terms associated with the given OMIM term key;
	 * null if there are none.
	 */
	protected Set<Integer> getHpoTermKeys(Integer omimTermKey) throws Exception {
		if (omimToHpo == null) { cacheHpoMaps(); }
		if (omimToHpo.containsKey(omimTermKey)) { return omimToHpo.get(omimTermKey); }
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

	/* get the terms for the MP headers that are associated with the high-level HPO terms
	 * that are ancestors of the given HPO term (including the term itself). returns empty
	 * set if none.
	 */
/*	protected Set<String> getMpHeaders(Integer hpoTermKey) throws Exception {
		Set<String> headers = new HashSet<String>();
		for (Integer termKey : getAncestorMpHeaderKeys(hpoTermKey)) {
			String header = getTerm(termKey);
			if (header != null) { headers.add(header); }
		}
		return headers;
	}
*/
	/* get the IDs for the MP headers that are associated with the high-level HPO terms
	 * that are ancestors of the given HPO term (including the term itself). returns empty
	 * set if none.
	 */
/*	protected Set<String> getMpHeaderIds(Integer hpoTermKey) throws Exception {
		Set<String> ids = new HashSet<String>();
		for (Integer termKey : getAncestorMpHeaderKeys(hpoTermKey)) {
			String id = getTermId(termKey);
			if (id != null) { ids.add(id); }
		}
		return ids;
	}
*/
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
				+ "  and ha.annotation_type = 1006) "
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
			+ "left outer join hdp_genocluster gc on (gg.hdp_genocluster_key = gc.hdp_genocluster_key) "
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

		private BSU() {}

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
