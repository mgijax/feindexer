package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.DistinctSolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

/* Is: an indexer that builds the index with the annotations for the Grid tab of the
 * 		HMDC summary page.  Each document in the index represents a single annotation
 * 		and is tied to a single basic unit of HMDC searching:
 *			* a genocluster, for mouse data, or
 *			* a marker/disease pair for human data.
 *		The gridKey field acts as a foreign key to the grid index, identifying the
 *		document (the basic search unit) to which the annotation is associated.
 */
public class HdpGridAnnotationIndexerSQL extends HdpIndexerSQL {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	Map<Integer,Integer> genotypeToGenocluster = null;		// genotype key -> genocluster key
	Map<String,Integer> headerSequenceNum = null;			// header -> sequence num
	Map<Integer,String> doTermToOmimID = null;				// maps DO term key to OMIM ID (where only 1 ID per term)

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public HdpGridAnnotationIndexerSQL() {
		super("diseasePortalGridAnnotation");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* cache the set of OMIM IDs for DO terms, where each DO term as exactly one associated OMIM ID
	 */
	protected void cacheSoloOmimIDs() throws Exception {
		if (doTermToOmimID != null) { return; }
		
		logger.info("Collecting solo OMIM IDs for DO terms");
		Timer.reset();

		String soloOmimQuery = "with do_one_omim as ( "
				+ "select o.term_key "
				+ "from term t, term_id o "
				+ "where t.vocab_name = 'Disease Ontology' "
				+ " and t.term_key = o.term_key "
				+ " and o.logical_db = 'OMIM' "
				+ "group by 1 "
				+ "having count(distinct o.acc_id) = 1 "
				+ ") "
			+ "select t.term_key, t.acc_id "
			+ "from term_id t, do_one_omim o "
			+ "where t.term_key = o.term_key "
			+ " and t.logical_db = 'OMIM'";
		
		doTermToOmimID = new HashMap<Integer,String>();

		ResultSet rs = ex.executeProto(soloOmimQuery, cursorLimit);
		while (rs.next()) {
			doTermToOmimID.put(rs.getInt("term_key"), rs.getString("acc_id"));
		}
		rs.close();

		logger.info("Finished collecting data for " + doTermToOmimID.size()
				+ " solo OMIM IDs " + Timer.getElapsedMessage());
	}

	/* get the OMIM ID associated with the given DO term key, if there is only one
	 */
	protected String getSoloOmimID(Integer termKey) throws Exception {
		if (doTermToOmimID == null) { cacheSoloOmimIDs(); }
		if (doTermToOmimID.containsKey(termKey)) {
			return doTermToOmimID.get(termKey);
		}
		return null;
	}

	/* cache necessary data for genotypes, including which genocluster each is
	 * associated with
	 */
	protected void cacheGenotypeData() throws Exception {
		if (genotypeToGenocluster != null) { return; }

		logger.info("Collecting genotype data");
		Timer.reset();

		String genotypeQuery = "select genotype_key, hdp_genocluster_key "
				+ "from hdp_genocluster_genotype";

		genotypeToGenocluster = new HashMap<Integer,Integer>();

		ResultSet rs = ex.executeProto(genotypeQuery, cursorLimit);
		while (rs.next()) {
			genotypeToGenocluster.put(rs.getInt("genotype_key"), rs.getInt("hdp_genocluster_key"));
		}
		rs.close();

		logger.info("Finished collecting data for " + genotypeToGenocluster.size()
				+ " genotypes " + Timer.getElapsedMessage());
	}

	/* get the database key for the genocluster that the specified genotype is
	 * associated with
	 */
	protected Integer getGenocluster(Integer genotypeKey) throws Exception {
		if (genotypeToGenocluster == null) { cacheGenotypeData(); }
		if (genotypeToGenocluster.containsKey(genotypeKey)) {
			return genotypeToGenocluster.get(genotypeKey);
		}
		return null;
	}

	/* compute and cache the sequence numbers for headers
	 */
	protected void cacheHeaderData() throws Exception {
		if (headerSequenceNum != null) { return; }

		logger.info("Collecting header data");
		Timer.reset();

		String headerQuery = "select distinct header from hdp_annotation order by header";

		ArrayList<String> termsToSort = new ArrayList<String>();

		ResultSet rs = ex.executeProto(headerQuery);
		while (rs.next()) {
			termsToSort.add(rs.getString("header"));
		}
		rs.close();
		logger.info("  - collected data in list");

		headerSequenceNum = new HashMap<String,Integer>();

		//sort the terms using smart alpha
		Collections.sort(termsToSort,new SmartAlphaComparator());
		logger.info("  - sorted list in smart-alpha order");

		for(int i=0;i<termsToSort.size();i++) {
			headerSequenceNum.put(termsToSort.get(i), i);
		}
		logger.info("finished collecting  " + (maxTermSeqNum - 1) + " headers " + Timer.getElapsedMessage());
	}

	/* get the sequence number for the given header
	 */
	protected int getHeaderSequenceNum(String header) throws Exception {
		if (headerSequenceNum == null) { cacheHeaderData(); }
		if (headerSequenceNum.containsKey(header)) {
			return headerSequenceNum.get(header);
		}
		return headerSequenceNum.size() + 1;
	}

	/* build and return a solr document for the given data
	 */
	protected SolrInputDocument buildDocument(BSU bsu, Integer termKey,
			String header, String qualifier, Integer humanMarkerKey, Integer sourceTermKey,
			Integer backgroundSensitive) throws Exception {

		uniqueKey += 1;	
		String term = getTerm(termKey);

		// need to start a new document...
		DistinctSolrInputDocument doc = new DistinctSolrInputDocument();
		doc.addField(DiseasePortalFields.UNIQUE_KEY, uniqueKey);
		doc.addField(DiseasePortalFields.GRID_KEY, bsu.bsuKey);
		doc.addField(DiseasePortalFields.GRID_CLUSTER_KEY, bsu.gridclusterKey);
		doc.addField(DiseasePortalFields.TERM, term);
		doc.addField(DiseasePortalFields.TERM_ID, getTermId(termKey));
		doc.addField(DiseasePortalFields.TERM_HEADER, header);
		doc.addField(DiseasePortalFields.TERM_TYPE, getVocabulary(termKey));
		doc.addField(DiseasePortalFields.TERM_QUALIFIER, qualifier);
		doc.addField(DiseasePortalFields.BY_TERM_NAME, getTermSequenceNum(term));
		doc.addField(DiseasePortalFields.BY_TERM_HEADER, getHeaderSequenceNum(header));
		doc.addField(DiseasePortalFields.BY_TERM_DAG, getDagSequenceNum(termKey));

		// add fields to help with highlighting
		doc.addAllDistinct(DiseasePortalFields.TERM_ALT_ID, getAlternateTermIds(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_SYNONYM, getTermSynonyms(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_ANCESTOR_ID, getTermAncestorIDs(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_ANCESTOR_TEXT, getTermAncestorText(termKey));
		
		// optional fields:
		// 1. human marker symbol + ID, for DO annotations to human markers
		// 2. source term + ID, for the DO terms from which HPO annotations were derived (human data)
		// 3. background sensitive, for MP annotations to mouse genotypes
		
		if (humanMarkerKey != null) {
			doc.addField(DiseasePortalFields.MARKER_SYMBOL, this.getMarkerSymbol(humanMarkerKey));
			doc.addField(DiseasePortalFields.MARKER_MGI_ID, this.getMarkerID(humanMarkerKey));
		}
		
		if (sourceTermKey != null) {
			doc.addField(DiseasePortalFields.SOURCE_TERM, this.getTerm(sourceTermKey));
			doc.addField(DiseasePortalFields.SOURCE_TERM_ID, this.getSoloOmimID(sourceTermKey));
		}
		
		if ((backgroundSensitive != null) && (backgroundSensitive >= 1)) {
			doc.addField(DiseasePortalFields.BACKGROUND_SENSITIVE, 1);
		}
		return doc;
	}

	/* retrieve the human marker/disease annotations and write the appropriate data
	 * to the grid annotations index
	 */
	protected void processHumanData() throws Exception {
		logger.info("processing human annotations");
		int mouseCount = uniqueKey;

		String humanQuery = "select marker_key, term_key, header, qualifier_type "
				+ "from hdp_annotation "
				+ "where annotation_type = 1022";		// only human marker/disease annotations

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		ResultSet rs = ex.executeProto(humanQuery, cursorLimit);
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			String qualifier = rs.getString("qualifier_type");
			Integer markerKey = rs.getInt("marker_key");
			BSU bsu = this.getHumanBsu(markerKey, termKey);

			// need to save this document; write to the server if our queue is big enough
			docs.add(buildDocument(bsu, termKey, rs.getString("header"), qualifier, markerKey, null, null));

			List<Integer> hpoTermKeys = getHpoTermKeys(termKey);
			if (hpoTermKeys != null) {
				for (Integer hpoTermKey : hpoTermKeys) {
					for (Integer mpHeaderKey : this.getMpHeaderKeys(hpoTermKey)) {
						String headerDisplay = getMpHeaderDisplay(mpHeaderKey);
						if (headerDisplay == null) { headerDisplay = getTerm(mpHeaderKey); }
						docs.add(buildDocument(bsu, hpoTermKey, headerDisplay, qualifier, markerKey, termKey, null));
					}
				}
			}

			if (docs.size() >= solrBatchSize) {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		// need to push final documents to the server
		writeDocs(docs);
		rs.close();

		logger.info("finished processing " + (uniqueKey - mouseCount) + " human annotations");
	}

	/* retrieve the mouse genocluster disease and phenotype annotations, and write the
	 * appropriate data to the grid annotation index
	 */
	protected void processMouseData() throws Exception {

		logger.info("processing mouse annotations");
		int humanCount = uniqueKey;

		// note that the genocluster tables already exclude annotation type 1022 (human
		// marker/disease annotations)
		String mouseQuery = "select distinct m.marker_key, gg.hdp_genocluster_key, ga.term_key, ga.qualifier_type, "
			+ "  ha.header, ga.genotermref_count, ga.has_backgroundnote "
			+ "from hdp_genocluster_marker m, "
			+ "  hdp_genocluster_genotype gg, "
			+ "  hdp_genocluster_annotation ga, "
			+ "  hdp_annotation ha "
			+ "where m.hdp_genocluster_key = gg.hdp_genocluster_key "
			+ "  and gg.hdp_genocluster_key = ga.hdp_genocluster_key "
			+ "  and ga.term_key = ha.term_key "
			+ "  and gg.genotype_key = ha.genotype_key";

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		ResultSet rs = ex.executeProto(mouseQuery, cursorLimit);
		while (rs.next()) {
			Integer markerKey = rs.getInt("marker_key");
			Integer genoclusterKey = rs.getInt("hdp_genocluster_key");
			Integer refCount = rs.getInt("genotermref_count");

			Integer gridclusterKey = getGridClusterKey(markerKey);
			
			// if we can't find a corresponding genocluster or gridcluster, skip this one
			if ((gridclusterKey == null) || (genoclusterKey == null)) { continue; }

			// if we can't find a corresponding BSU, skip this one
			BSU bsu = getMouseBsu(genoclusterKey, gridclusterKey);
			if (bsu == null) { continue; }

			// To account for multiple references (and their influences as multiple annotations
			// on the cell coloring in the grid), we add a unique document for each
			// annotation/reference pair.
			for (int i = 0; i < refCount; i++) {
				// need to save this document; write to the server if our queue is big enough
				docs.add(this.buildDocument(bsu, rs.getInt("term_key"), rs.getString("header"),
					rs.getString("qualifier_type"), null, null, rs.getInt("has_backgroundnote")));
			}

			if (docs.size() >= solrBatchSize) {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		// need to push final documents to the server
		writeDocs(docs);
		rs.close();

		logger.info("finished processing " + (uniqueKey - humanCount) + " mouse annotations");
	}

	/* walk through the basic units for searching (genoclusters for mouse data,
	 * marker/disease pairs for human data), collate the data for each, and send
	 * their annotations to the index.
	 */
	protected void processAnnotationData() throws Exception {
		processHumanData();
		processMouseData();
	}

	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		processAnnotationData();
		commit();
	}
}
