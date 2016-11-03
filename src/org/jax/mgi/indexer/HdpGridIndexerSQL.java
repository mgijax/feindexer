package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.DistinctSolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;
import org.jax.mgi.shr.jsonmodel.GridGenocluster;
import org.jax.mgi.shr.jsonmodel.GridGenoclusterAllele;

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

	// genocluster key -> sequence number
	protected Map<Integer,Integer> genoclusterSeqNum = null;

	// genocluster key -> encoded JSON string with IMSR-related allele and marker data for the genocluster
	protected Map<Integer,String> imsrAlleles = null;

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public HdpGridIndexerSQL() {
		super("index.url.diseasePortalGrid");
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
		doc.addAllDistinct(DiseasePortalFields.MARKER_SYNONYM_SINGLE_TOKEN, getMarkerSingleTokenSynonyms(markerKey, true));
		addMarkerCoordinates(doc, markerKey, true);
		addExpressedComponents(doc, markerKey);
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
				
				String humanGM = getHumanMarkers(gck);
				if ((humanGM != null) && (humanGM.length() > 0)) {
					doc.addField(DiseasePortalFields.GRID_HUMAN_SYMBOLS, humanGM);
				}

				String mouseGM = getMouseMarkers(gck);
				if ((mouseGM != null) && (mouseGM.length() > 0)) {
					doc.addField(DiseasePortalFields.GRID_MOUSE_SYMBOLS, mouseGM);
				}
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
			for (Integer orthoMarkerKey : orthologousMarkerKeys) {
				doc.addDistinctField(DiseasePortalFields.ORTHOLOG_SYMBOL, getMarkerSymbol(orthoMarkerKey));
				doc.addDistinctField(DiseasePortalFields.ORTHOLOG_NOMEN, getMarkerName(orthoMarkerKey));
				doc.addAllDistinct(DiseasePortalFields.ORTHOLOG_SYNONYM, getMarkerSynonyms(orthoMarkerKey));
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
		doc.addAllDistinct(DiseasePortalFields.TERM_HEADER, getHeadersPerTerm(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_ALT_ID, getAlternateTermIds(termKey));
		
		// if the term is an DO term, then we also need to add the MP headers with which its
		// HPO terms area associated (to facilitate lookup of data for the phenotype popups)
		
		if (disease.equals(getVocabulary(termKey))) {
			List<Integer> hpoKeys = this.getHpoTermKeys(termKey);
			if (hpoKeys != null) {
				for (Integer hpoKey : hpoKeys) {
					for (Integer mpHeaderKey : getMpHeaderKeys(hpoKey)) {
						String mpHeader = getMpHeaderDisplay(mpHeaderKey);
						if (mpHeader == null) { mpHeader = getTerm(mpHeaderKey); }
						doc.addDistinctField(DiseasePortalFields.TERM_HEADER, mpHeader);
					}
				}
			}
		}
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
				+ "where annotation_type = 1022 "
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
					// only save this document if it can be tied to a gridcluster
					if (doc.containsKey(DiseasePortalFields.GRID_CLUSTER_KEY)) {
						// save this document and write to the server if our queue is big enough
						docs.add(doc);
						if (docs.size() >= solrBatchSize) {
							writeDocs(docs);
							docs = new ArrayList<SolrInputDocument>();
						}
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
			addTermData(doc, termKey, disease);
			addHpoData(doc, termKey);
		}

		// save the final doc, if it can be tied to a gridcluster
		// need to push final documents to the server
		if ((doc != null) && (doc.containsKey(DiseasePortalFields.GRID_CLUSTER_KEY))) {
			docs.add(doc); 
		}
		writeDocs(docs);
		rs.close();

		logger.info("finished processing human annotations");
	}

	/* look up the precomputed sequence number for each genocluster (drawn from its genotypes).  For
	 * multiple genotypes, keep the lowest sequence number.
	 */
	protected void cacheGenotypeSequenceNum() throws Exception {
		if (genoclusterSeqNum != null) { return; }
		
		Timer.reset();
		genoclusterSeqNum = new HashMap<Integer,Integer>();
		
		String gcQuery = "select gg.hdp_genocluster_key, min(gsn.by_hdp_rules) as by_hdp_rules "
			+ "from hdp_genocluster_genotype gg, genotype_sequence_num gsn "
			+ "where gg.genotype_key = gsn.genotype_key "
			+ "group by 1";
		
		ResultSet rs = ex.executeProto(gcQuery, cursorLimit);
		
		while (rs.next()) {
			genoclusterSeqNum.put(rs.getInt("hdp_genocluster_key"), rs.getInt("by_hdp_rules"));
		}
		rs.close();

		logger.info("finished retrieving seq num for " + genoclusterSeqNum.size() + " genoclusters " + Timer.getElapsedMessage());
	}
	
	/* retrieve a precomputed sequence number for the given genocluster (ordering based on conditional flag,
	 * pair state, allele symbols)
	 */
	protected int getGenoclusterSequenceNum (int genoclusterKey) throws Exception {
		if (genoclusterSeqNum == null) { cacheGenotypeSequenceNum(); }
		if (!genoclusterSeqNum.containsKey(genoclusterKey)) {
			// should not happen, but just in case, we'll look up the current max and go higher
			int maxSeqNum = 0;
			for (Integer gcKey : genoclusterSeqNum.keySet()) {
				if (genoclusterSeqNum.get(gcKey) > maxSeqNum) {
					maxSeqNum = genoclusterSeqNum.get(gcKey);
				}
			}
			genoclusterSeqNum.put(genoclusterKey, maxSeqNum + 1);
		}
		return genoclusterSeqNum.get(genoclusterKey);
	}
	
	/* cache the IMSR-related data for each genocluster's alleles and markers
	 */
	protected void cacheImsrAlleles() throws Exception {
		if (imsrAlleles != null) { return; }

		Timer.reset();
		imsrAlleles = new HashMap<Integer,String>();
		
		// exclude wild-type alleles, as they should not appear in the IMSR popups
		String query = "select distinct gg.hdp_genocluster_key, a.primary_id as allele_id, "
			+ " a.symbol as allele_symbol, c.strain_count, m.primary_id as marker_id, m.symbol as marker_symbol, "
			+ "  c.count_for_marker, s.by_symbol "
			+ "from hdp_genocluster_genotype gg, allele_to_genotype ag, allele a, allele_sequence_num s, "
			+ "  allele_imsr_counts c, marker_to_allele ma, marker m "
			+ "where gg.genotype_key = ag.genotype_key "
			+ "  and ag.allele_key = a.allele_key " 
			+ "  and ag.allele_key = s.allele_key "
			+ "  and ag.allele_key = c.allele_key "
			+ "  and ag.allele_key = ma.allele_key "
			+ "  and ma.marker_key = m.marker_key "
			+ "  and a.is_wild_type = 0 "
			+ "order by gg.hdp_genocluster_key, s.by_symbol";
		
		ResultSet rs = ex.executeProto(query, cursorLimit);
		
		List<GridGenoclusterAllele> alleles = null;
		int lastGenoclusterKey = -1;

		ObjectMapper mapper = new ObjectMapper();		// converts objects to JSON

		// only keep one genocluster's alleles in memory at a time to save memory
		while (rs.next()) {
			int genoclusterKey = rs.getInt("hdp_genocluster_key");
			if (genoclusterKey != lastGenoclusterKey) {
				if ((alleles != null) && (alleles.size() > 0)) {
					imsrAlleles.put(lastGenoclusterKey, mapper.writeValueAsString(new GridGenocluster(lastGenoclusterKey, alleles)));
				}
				alleles = new ArrayList<GridGenoclusterAllele>();
				lastGenoclusterKey = genoclusterKey;
			}
			alleles.add(new GridGenoclusterAllele(rs.getString("allele_id"), rs.getString("allele_symbol"), rs.getInt("strain_count"),
				rs.getString("marker_id"), rs.getString("marker_symbol"), rs.getInt("count_for_marker")));
		}
		rs.close();
		
		if ((alleles != null) && (alleles.size() > 0)) {
			imsrAlleles.put(lastGenoclusterKey, mapper.writeValueAsString(new GridGenocluster(lastGenoclusterKey, alleles)));
		}

		logger.info("finished retrieving IMSR allele data for " + imsrAlleles.size() + " genoclusters " + Timer.getElapsedMessage());
	}

	/* retrieve an encoded JSON string that represents the markers and alleles for the given genocluster
	 */
	protected String getImsrAlleles (int genoclusterKey) throws Exception {
		if (imsrAlleles == null) { cacheImsrAlleles(); }
		if (imsrAlleles.containsKey(genoclusterKey)) {
			return imsrAlleles.get(genoclusterKey);
		}
		return null;
	}
	
	/* add data specific to the given genocluster, including counts for IMSR for its alleles and markers
	 */
	protected void addGenoClusterData(DistinctSolrInputDocument doc, int genoclusterKey) throws Exception {
		doc.addField(DiseasePortalFields.ALLELE_PAIRS, getAllelePairs(genoclusterKey));
		doc.addField(DiseasePortalFields.GENO_CLUSTER_KEY, genoclusterKey);
		doc.addField(DiseasePortalFields.BY_GENOCLUSTER, getGenoclusterSequenceNum(genoclusterKey));
		if (isConditional(genoclusterKey)) {
			doc.addField(DiseasePortalFields.IS_CONDITIONAL, 1);
		} else {
			doc.addField(DiseasePortalFields.IS_CONDITIONAL, 0);
		}
		
		String imsrAlleles = getImsrAlleles(genoclusterKey);
		if (imsrAlleles != null) {
			doc.addField(DiseasePortalFields.IMSR_ALLELES, imsrAlleles);
		}
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

		String mouseQuery = "select a.hdp_genocluster_key, gc.marker_key, "
				+ "  a.term_key, a.annotation_type, a.qualifier_type, a.term_type "
				+ "from hdp_genocluster_annotation a, "
				+ "  hdp_genocluster gc "
				+ "where a.hdp_genocluster_key = gc.hdp_genocluster_key "
				+ "order by a.hdp_genocluster_key, gc.marker_key";

		int lastBsuKey = -1;		// last BSU key that was saved as a document

		DistinctSolrInputDocument doc = null;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		ResultSet rs = ex.executeProto(mouseQuery, cursorLimit);
		while (rs.next()) {
			Integer genoclusterKey = rs.getInt("hdp_genocluster_key");
			Integer markerKey = rs.getInt("marker_key");
			Integer termKey = rs.getInt("term_key");

			// Note that we skip any genoclusters that cannot be tied to a gridcluster.
			Integer gridclusterKey = getGridClusterKey(markerKey);
			if (gridclusterKey == null) { continue; }

			BSU bsu = getMouseBsu(genoclusterKey, gridclusterKey);
			if (bsu == null) { continue; }

			// assume MP annotation, as those are more common; correct if needed
			String annotationType = mp;
			if (1020 == rs.getInt("annotation_type")) { annotationType = disease; }

			if (lastBsuKey != bsu.bsuKey) {
				if (lastBsuKey >= 0) {
					// save this document and write to the server if our queue is big enough
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
				addGenoClusterData(doc, genoclusterKey);
				addGridClusterData(doc, null, gridclusterKey);

				addMarkerData(doc, markerKey); 
				addOrthologyData(doc, markerKey);
			}

			// fields to add to the current document (whether new or continuing to fill
			// the document for the same BSU as before)

			addTermData(doc, termKey, annotationType);
		}

		// add the final doc, if it can be tied to a gridcluster
		if ((doc != null) && (doc.containsKey(DiseasePortalFields.GRID_CLUSTER_KEY))) {
			docs.add(doc); 
		}
		// need to push final documents to the server
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
