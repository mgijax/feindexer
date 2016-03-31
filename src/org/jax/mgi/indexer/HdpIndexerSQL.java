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
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;
import org.jax.mgi.shr.fe.query.SolrLocationTranslator;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

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
	
	protected int cursorLimit = 10000;				// number of records to retrieve at once
	
	protected int uniqueKey = 0;					// (incremental) unique key for index documents

	protected String omim = "OMIM";					// vocab name for disease terms
	protected String mp = "Mammalian Phenotype";	// vocab name for mouse phenotype terms
	protected String hpo = "HPO";					// vocab name for human phenotype terms
	
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

			String termIdQuery="select t.primary_id term_id, ti.acc_id alt_id "
				+ "from term t, term_id ti "
				+ "where t.term_key = ti.term_key "
				+ "  and t.vocab_name in ('Mammalian Phenotype', 'OMIM') ";

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
				"and t.vocab_name in ('OMIM','Mammalian Phenotype') ";
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
				"where vocab_name in ('OMIM','Mammalian Phenotype') ";
		ResultSet rs = ex.executeProto(query, cursorLimit);
		while(rs.next()) {
			termsToSort.add(rs.getString("term"));
		}
		rs.close();
		logger.debug("  - collected data in list");

		//sort the terms using smart alpha
		Collections.sort(termsToSort,new SmartAlphaComparator());
		logger.debug("  - sorted list in smart-alpha order");

		termSortMap = new HashMap<String,Integer>();
		for(int i=0;i<termsToSort.size();i++) {
			termSortMap.put(termsToSort.get(i), i);
		}
		maxTermSeqNum = termSortMap.keySet().size() + 1;
		logger.info("finished calculating sequence numbers for " + (maxTermSeqNum - 1) + " diseases and phenotypes" + Timer.getElapsedMessage());
		
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
			logger.debug("Loading marker IDs");
			Timer.reset();

			// load all marker IDs (for every organism)
			String markerIdQuery="select marker_key, acc_id "
				+ "from marker_id "
				+ "where logical_db not in ('ABA','Download data from the QTL Archive','FuncBase','GENSAT','GEO','HomoloGene','RIKEN Cluster','UniGene') ";
			markerAllIdMap = populateLookup(markerIdQuery,"marker_key","acc_id","marker keys to IDs");
			
			logger.debug("Finished loading IDs for " + markerAllIdMap.size() + " markers" + Timer.getElapsedMessage());
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
	 * get it now.  This pulls hybrid homology data, including HGNC and HomoloGene.
	 * The mapping is from (Integer) marker key to (Integer) grid cluster key.
	 */
	protected Map<Integer,Integer> getHomologyMap() throws SQLException {
		if (homologyMap == null) {
			logger.debug("Retrieving homologyMap data");
			Timer.reset();
			homologyMap = new HashMap<Integer,Integer>();

			String homologyQuery = "select m.marker_key, m.hdp_gridcluster_key, g.source "
					+ "from hdp_gridcluster g, hdp_gridcluster_marker m "
					+ "where g.hdp_gridcluster_key = m.hdp_gridcluster_key "
					+ " and source is not null";

			ResultSet rs = ex.executeProto(homologyQuery, cursorLimit);
			while (rs.next()) {
				homologyMap.put(rs.getInt("marker_key"), rs.getInt("hdp_gridcluster_key"));
			}
			rs.close();
			
			logger.debug("Finished retrieving homologyMap data (" + homologyMap.size() + " markers)" + Timer.getElapsedMessage());
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

	/* If we've not already retrieved the feature type data from the database,
	 * get it now.  This pulls the feature types for all mouse markers in the
	 * various grid clusters.  Mapping is from (String) grid cluster keys to a
	 * Set of (String) feature types.
	 */
	protected Map<String,Set<String>> getFeatureTypeMap() throws Exception {
		if (featureTypeMap == null) {
			logger.debug("Retrieving feature types");
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
			
			logger.debug("Finished retrieving feature types (" + featureTypeMap.size() + " gridclusters)" + Timer.getElapsedMessage());
		}
		return featureTypeMap;
	}

	/* get a mapping from (String) disease ID to a Set of (String) marker keys that are
	 * positively associated with that disease.  (no annotations with a NOT qualifier)
	 */
	protected Map<String,Set<String>> getMarkersPerDisease() throws Exception {
		if (markersPerDisease == null) {
			logger.debug("Retrieving markers per disease");
			Timer.reset();

			// This had used getNonNormalAnnotationsTable() as a source, but optimizing to
			// bring the 'where' clause up into this query and simplify.
			String markerQuery = "select distinct h.term_id, h.marker_key, m.symbol "
				+ "from hdp_annotation h, marker m "
				+ "where h.vocab_name='OMIM' "
				+ "  and h.organism_key = 1 "
				+ "  and h.marker_key = m.marker_key "
				+ "  and h.qualifier_type is null "
				+ "  and (h.genotype_type!='complex' or h.genotype_type is null) "
				+ "order by h.term_id, m.symbol";

			markersPerDisease = populateLookupOrdered(markerQuery,
					"term_id", "marker_key", "diseases to markers");
			
			logger.debug("Finished retrieving markers for (" + markersPerDisease.size() + " diseases)" + Timer.getElapsedMessage());
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
			logger.debug("Retrieving headers per disease");
			Timer.reset();

			String headerQuery = "select distinct term_id, header "
				+ "from hdp_annotation "
				+ "where vocab_name = '" + omim + "' "
				+ "order by term_id, header";
			headersPerDisease = populateLookupOrdered(headerQuery, "term_id", "header", "diseases to headers");
			
			logger.debug("Finished retrieving headers for (" + headersPerDisease.size() + " diseases)" + Timer.getElapsedMessage());
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
		logger.debug("Caching basic marker data");
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
			
		logger.debug("Finished retrieving basic marker data (" + markerIdMap.size() + " markers)" + Timer.getElapsedMessage());
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
		logger.debug("Identifying human and mouse markers");
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
			
		logger.debug("Identified " + mouseMarkers.size() + " mouse and "
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
		logger.debug("Got max annotation key: " + i);
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
		logger.debug("  - Got annotations for keys " + (startKey + 1) + " to " + endKey);
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
		
		logger.debug("Processing annotations 1 to " + maxAnnotationKey);
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
					logger.debug("  - sent batch of docs to Solr");
				}
			}

			rs.close();
			start = end;
			end = end + dbChunkSize;
		}

		if (!docs.isEmpty()) {
			server.add(docs);
			logger.debug("  - sent final batch of docs to Solr");
		}
		server.commit();
		logger.debug("Done processing annotations 1 to " + maxAnnotationKey);
	}
}
