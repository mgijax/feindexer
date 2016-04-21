package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.DistinctSolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;

/* Is: an indexer that builds the index supporting the Grid tab of the HMDC summary
 *		page.  Each document in the index represents a basic unit of HMDC searching:
 *			* a genocluster, for mouse data, or
 *			* a marker/disease pair for human data.
 *		The gridKey field uniquely identifies the document, and it is this field that
 *		is used to identify the corresponding annotations in the grid annotation index.
 */
public class HdpGridIndexerSQL extends HdpIndexerSQL {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	// genocluster key -> [ marker key 1, marker key 2, ... ]
	protected Map<Integer,List<Integer>> markersPerGenocluster = null;

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public HdpGridIndexerSQL() {
		super("index.url.diseasePortalGrid");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* cache the markers associated with each genocluster
	 */
	protected void cacheMarkersPerGenocluster() throws Exception {
		if (markersPerGenocluster != null) { return; }

		logger.info("retrieving markers per genocluster");
		Timer.reset();

		String markerQuery = "select hdp_genocluster_key, marker_key "
				+ "from hdp_genocluster";

		markersPerGenocluster = new HashMap<Integer,List<Integer>>();

		ResultSet rs = ex.executeProto(markerQuery, cursorLimit);
		while (rs.next()) {
			Integer gcKey = rs.getInt("hdp_genocluster_key");

			if (!markersPerGenocluster.containsKey(gcKey)) {
				markersPerGenocluster.put(gcKey, new ArrayList<Integer>());
			}
			markersPerGenocluster.get(gcKey).add(rs.getInt("marker_key"));
		}
		rs.close();

		logger.info("finished retrieving markers for " + markersPerGenocluster.size() + " genoclusters " + Timer.getElapsedMessage());
	}

	/* retrieve the marker keys associated with the given genocluster key
	 */
	protected List<Integer> getMarkers(int genoclusterKey) throws Exception {
		if (markersPerGenocluster == null) { cacheMarkersPerGenocluster(); }
		if (markersPerGenocluster.containsKey(genoclusterKey)) {
			return markersPerGenocluster.get(genoclusterKey);
		}
		return null;
	}

	/* add data for the given marker key to the Solr document
	 */
	protected void addMarkerData(DistinctSolrInputDocument doc, int markerKey) throws Exception {
		// add data for this marker
		doc.addDistinctField(DiseasePortalFields.MARKER_KEY, markerKey);
		doc.addDistinctField(DiseasePortalFields.MARKER_SYMBOL, getMarkerSymbol(markerKey));
		doc.addDistinctField(DiseasePortalFields.MARKER_NAME, getMarkerName(markerKey));
		doc.addAllDistinct(DiseasePortalFields.MARKER_ID, getMarkerIds(markerKey));
		doc.addAllDistinct(DiseasePortalFields.MARKER_SYNONYM, getMarkerSynonyms(markerKey));

		if (this.isHuman(markerKey)) {
			doc.addAllDistinct(DiseasePortalFields.HUMAN_COORDINATE, getMarkerCoordinates(markerKey));
		} else {
			doc.addAllDistinct(DiseasePortalFields.MOUSE_COORDINATE, getMarkerCoordinates(markerKey));
		}

	}

	/* look up the grid cluster for the given marker and add its associated data to the
	 * Solr document
	 */
	protected void addGridClusterData(DistinctSolrInputDocument doc, Integer markerKey, Integer gridclusterKey) throws Exception {
		// if given the grid cluster key, go with it.  if not, look it up based on marker.
		Integer gck;
		if (gridclusterKey != null) { gck = gridclusterKey; }
		else if (markerKey != null) { gck = getGridClusterKey(markerKey); }
		else { return; }

		doc.addDistinctField(DiseasePortalFields.GRID_CLUSTER_KEY, gck);

		// only update the homology cluster key (single-valued) if we've not yet defined one
		doc.addDistinctField(DiseasePortalFields.HOMOLOGY_CLUSTER_KEY, gck);

		// add feature types for all markers in the gridcluster (if there is one) or
		// for just this marker (if not)
		Set<String> featureTypes = null;
		if (gck != null) {
			// only add symbols if not already defined
			if (!(doc.containsKey(DiseasePortalFields.GRID_HUMAN_SYMBOLS) ||
					doc.containsKey(DiseasePortalFields.GRID_MOUSE_SYMBOLS))) {
				featureTypes = getFeatureTypes(gck);
				doc.addField(DiseasePortalFields.GRID_HUMAN_SYMBOLS, getHumanMarkers(gck));
				doc.addField(DiseasePortalFields.GRID_MOUSE_SYMBOLS, getMouseMarkers(gck));
			}
		} else {
			featureTypes = getMarkerFeatureTypes(markerKey);
		}
		doc.addAllDistinct(DiseasePortalFields.FILTERABLE_FEATURE_TYPES, featureTypes);
	}

	/* look up orthology data for the given marker and add it to the Solr document
	 */
	protected void addOrthologyData(DistinctSolrInputDocument doc, int markerKey) throws Exception {
		// add data for orthologs of this marker
		Set<Integer> orthologousMarkerKeys = getMarkerOrthologs(markerKey);
		if (orthologousMarkerKeys != null) {

			// collect the orthologs' data in sets to avoid duplication in the index
			Set<String> orthologNomen = new HashSet<String>();
			Set<String> orthologIds = new HashSet<String>();

			for (Integer orthoMarkerKey : orthologousMarkerKeys) {
				doc.addDistinctField(DiseasePortalFields.ORTHOLOG_NOMEN, getMarkerSymbol(orthoMarkerKey));
				doc.addDistinctField(DiseasePortalFields.ORTHOLOG_NOMEN, getMarkerName(orthoMarkerKey));
				doc.addAllDistinct(DiseasePortalFields.ORTHOLOG_NOMEN, getMarkerSynonyms(orthoMarkerKey));
				doc.addAllDistinct(DiseasePortalFields.ORTHOLOG_ID, getMarkerIds(orthoMarkerKey));
			}
		}
	}

	/* add data for the given term key to the Solr document
	 */
	protected void addTermData(DistinctSolrInputDocument doc, Integer termKey, String termType) throws Exception {
		if (termKey == null) { return; }
		doc.addDistinctField(DiseasePortalFields.TERM, getTerm(termKey));
		doc.addDistinctField(DiseasePortalFields.TERM_ID, getTermId(termKey));
		doc.addDistinctField(DiseasePortalFields.TERM_TYPE, termType); 
		doc.addAllDistinct(DiseasePortalFields.TERM_SYNONYM, getTermSynonyms(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_ANCESTOR_ID, getTermAncestorIDs(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_ANCESTOR_TEXT, getTermAncestorText(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_HEADER, getHeadersByDisease(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_ALT_ID, getAlternateTermIds(termKey));
	}

	/* retrieve the human marker/disease annotations and write the appropriate data
	 * to the grid index
	 */
	protected void processHumanData() throws Exception {
		// We need to order our queries such that annotations for the same BSU are
		// returned together.  (This is so our document for the BSU can be built at
		// one time, rather than requiring all BSUs to be cached in memory for the
		// entire run of the indexer.

		logger.info("processing human annotations");

		String humanQuery = "select distinct marker_key, term_key, qualifier_type "
				+ "from hdp_annotation "
				+ "where annotation_type = 1006 "
				+ "order by marker_key, term_key";

		int lastBsuKey = -1;		// last BSU key that was saved as a document

		DistinctSolrInputDocument doc = null;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		ResultSet rs = ex.executeProto(humanQuery, cursorLimit);
		while (rs.next()) {
			Integer markerKey = rs.getInt("marker_key");
			Integer termKey = rs.getInt("term_key");
			BSU bsu = this.getHumanBsu(markerKey, termKey);

			if (lastBsuKey != bsu.bsuKey) {
				if (lastBsuKey >= 0) {
					// need to save this document; write to the server if our queue is big enough
					docs.add(doc);
					if (docs.size() >= solrBatchSize) {
						writeDocs(docs);
						docs = new ArrayList<SolrInputDocument>();
					}
				}
				lastBsuKey = bsu.bsuKey;

				// need to start a new document...
				doc = new DistinctSolrInputDocument();
				doc.addField(DiseasePortalFields.UNIQUE_KEY, bsu.bsuKey);
				doc.addField(DiseasePortalFields.GRID_KEY, bsu.bsuKey);
				doc.addField(DiseasePortalFields.IS_CONDITIONAL, 0);

				addMarkerData(doc, markerKey);
				addGridClusterData(doc, markerKey, null);
				addOrthologyData(doc, markerKey);
			}

			// fields to add to the current document (whether new or continuing to fill
			// the document for the same BSU as before)
			addTermData(doc, termKey, omim);
		}

		// need to push final documents to the server
		if (doc != null) { docs.add(doc); }
		writeDocs(docs);
		rs.close();

		logger.info("finished processing human annotations");
	}

	/* retrieve the mouse genocluster disease and phenotype annotations, and write the
	 * appropriate data to the grid index
	 */
	protected void processMouseData() throws Exception {
		// We need to order our queries such that annotations for the same BSU are
		// returned together.  (This is so our document for the BSU can be built at
		// one time, rather than requiring all BSUs to be cached in memory for the
		// entire run of the indexer.

		logger.info("processing mouse annotations");

		String mouseQuery = "select a.hdp_genocluster_key, a.term_key, a.annotation_type, "
				+ "  a.qualifier_type, a.term_type "
				+ "from hdp_genocluster_annotation a "
				+ "order by a.hdp_genocluster_key";

		int lastGenoclusterKey = -1;		// last GC key that was saved as a document

		DistinctSolrInputDocument doc = null;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		ResultSet rs = ex.executeProto(mouseQuery, cursorLimit);
		while (rs.next()) {
			Integer genoclusterKey = rs.getInt("hdp_genocluster_key");
			Integer termKey = rs.getInt("term_key");

			BSU bsu = getMouseBsu(genoclusterKey);
			if (bsu == null) {
				logger.info("null BSU for genocluster key: " + genoclusterKey);
			}

			// assume MP annotation, as those are more common; correct if needed
			String annotationType = mp;
			if (1005 == rs.getInt("annotation_type")) { annotationType = omim; }

			/* a genocluster can be associated with multiple gridclusters, which means that
			 * we need to handle multiple gridcluster keys in the BSU.
			 */
			if (lastGenoclusterKey != genoclusterKey) {
				if (lastGenoclusterKey >= 0) {
					// need to save this document; write to the server if our queue is big enough
					docs.add(doc);
					if (docs.size() >= solrBatchSize) {
						writeDocs(docs);
						docs = new ArrayList<SolrInputDocument>();
					}
				}
				lastGenoclusterKey = genoclusterKey;

				// need to start a new document...
				doc = new DistinctSolrInputDocument();
				doc.addField(DiseasePortalFields.UNIQUE_KEY, bsu.bsuKey);
				doc.addField(DiseasePortalFields.GRID_KEY, bsu.bsuKey);
				doc.addField(DiseasePortalFields.GENO_CLUSTER_KEY, genoclusterKey);
				doc.addField(DiseasePortalFields.ALLELE_PAIRS, getAllelePairs(genoclusterKey));

				if (isConditional(genoclusterKey)) {
					doc.addField(DiseasePortalFields.IS_CONDITIONAL, 1);
				} else {
					doc.addField(DiseasePortalFields.IS_CONDITIONAL, 0);
				}

				List<Integer> markerKeys = getMarkers(genoclusterKey);
				if (markerKeys != null) {
					for (Integer markerKey : markerKeys) {
						addMarkerData(doc, markerKey); 
						addOrthologyData(doc, markerKey);
					}
				}
			}

			// fields to add to the current document (whether new or continuing to fill
			// the document for the same BSU as before)

			addTermData(doc, termKey, annotationType);
			for (Integer gridclusterKey : bsu.gridclusterKeys) {
				addGridClusterData(doc, null, gridclusterKey);
			}
		}

		// need to push final documents to the server
		if (doc != null) { docs.add(doc); }
		writeDocs(docs);
		rs.close();

		logger.info("finished processing mouse annotations");
	}

	/* walk through the basic units for searching (genoclusters for mouse data,
	 * marker/disease pairs for human data), collate the data for each, and send
	 * them to the index.
	 */
	protected void processGridData() throws Exception {
		processHumanData();
		processMouseData();
	}

	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		processGridData();
		commit();
	}
}
