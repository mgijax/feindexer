package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.QSAccIDFormatter;
import org.jax.mgi.shr.QSAccIDFormatterFactory;
import org.jax.mgi.shr.fe.IndexConstants;

/* Is: an indexer that builds the index supporting the quick search's other ID bucket (aka- bucket 3).
 * 		Each document in the index represents data for a single object (e.g.- probe, clone, sequence, etc.)
 * 		that is not represented in one of the other QS buckets.
 */
public class QSOtherBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	// weights to prioritize different types of search terms / IDs
	private static int PRIMARY_ID_WEIGHT = 1000;
	private static int SECONDARY_ID_WEIGHT = 950;

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int gcLimit = 250000;					// number of data objects to process before collecting garbage
	private int gcCount = 0;						// data objects processed since last garbage collection
	
	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	private long uniqueKey = 0;						// ascending counter of documents created
	private long seqNum = 0;						// sort order for display of objects to users (absent boosting in fewi)
	private QSAccIDFormatterFactory idFactory = new QSAccIDFormatterFactory();

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSOtherBucketIndexerSQL() {
		super("qsOtherBucket");
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
	private SolrInputDocument buildDoc(DocBuilder builder, String exactTerm, String searchTermDisplay,
			String searchTermType, Integer searchTermWeight, String primaryID, Long sequenceNum) {

		SolrInputDocument doc = builder.getNewDocument();
		if (exactTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM_EXACT, exactTerm); }

		doc.addField(IndexConstants.QS_SEARCH_TERM_DISPLAY, searchTermDisplay);
		doc.addField(IndexConstants.QS_SEARCH_TERM_TYPE, searchTermType);
		doc.addField(IndexConstants.QS_SEARCH_TERM_WEIGHT, searchTermWeight);
		doc.addField(IndexConstants.QS_SEQUENCE_NUM, sequenceNum);
		doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
		return doc;
	}
	
	/* convenience wrapper, both creating the document and adding it to the index.
	 */
	private void buildAndAddDocument (DocBuilder builder, String exactTerm, String searchTermDisplay,
			String searchTermType, Integer searchTermWeight, String primaryID, Long sequenceNum) {
		
		this.addDoc(this.buildDoc(builder, exactTerm, searchTermDisplay, searchTermType,
			searchTermWeight, primaryID, sequenceNum));
	}
	
	/* Run garbage collection if we've hit the limit since the last time we did.
	 */
	private void gc() {
		this.gcCount++;
		if (this.gcCount >= this.gcLimit) {
			System.gc();
			this.gcCount = 0;
			this.commit();		// do explicit commits here to let Solr catch up
		}
	}
	
	/* Tweak any logical database values as needed for display.
	 */
	private String cleanLogicalDB (String logicalDB) {
		if ("Sequence DB".equals(logicalDB)) {
			return "GenBank, EMBL, DDBJ";
		}
		return logicalDB;
	}
	
	/* Add documents to the index for sequences and other objects that should also be returned
	 * with them.  We're currently seeing about 14.8 million IDs for 13.7 million sequences, so
	 * only about 8% secondary IDs.
	 */
	private void indexSequences() throws Exception {
		logger.info(" - indexing sequences");
		
		long startSeqNum = seqNum;
		
		String cmd = "select s.primary_id, s.logical_db as primary_ldb, s.sequence_type, " + 
				"  s.description, i.acc_id as other_id, i.logical_db as other_ldb," + 
				"  n.by_sequence_type " + 
				"from sequence s " + 
				"inner join sequence_sequence_num n on (s.sequence_key = n.sequence_key)" + 
				"left outer join sequence_id i on (s.sequence_key = i.sequence_key" + 
				"  and i.private = 0)" + 
				"where organism = 'mouse' " +
				"order by n.by_sequence_type, s.sequence_key";
		
		String lastPrimaryID = "";
		DocBuilder seq = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		QSAccIDFormatter idf = null;
		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// If we have a new primary ID, then we have a new sequence.  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				seq = new DocBuilder(primaryID, rs.getString("description"), "Sequence",
					rs.getString("sequence_type"), "/sequence/" + primaryID);
				
				// Index the primary ID.
				idf = idFactory.getFormatter("Sequence", cleanLogicalDB(rs.getString("primary_ldb")), primaryID);
				this.buildAndAddDocument(seq, primaryID, idf.getMatchDisplay(), idf.getMatchType(),
					PRIMARY_ID_WEIGHT, primaryID, seqNum++);

				lastPrimaryID = primaryID;
				gc();
			}

			// Also index the other ID if it differs from the primary.
			String otherID = rs.getString("other_id");
			if (!primaryID.equals(otherID)) {
				idf = idFactory.getFormatter("Sequence", cleanLogicalDB(rs.getString("other_ldb")), rs.getString("other_id"));
				this.buildAndAddDocument(seq, otherID, idf.getMatchDisplay(), idf.getMatchType(),
					SECONDARY_ID_WEIGHT, primaryID, seqNum);
			}
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " sequences");
	}

	/* Add documents to the index associating probe IDs with their associated sequences.
	 * We're currently seeing about 6.6 million probe IDs for 2.1 million sequences, so
	 * roughly 3 probe IDs per sequence (for those associated with probes).
	 */
	private void indexProbeIDsForSequences() throws Exception {
		logger.info(" - indexing probe IDs for sequences");
		
		long startSeqNum = seqNum;
		
		String cmd = "select seq.primary_id, seq.description, seq.sequence_type, s.by_sequence_type, " + 
				"  p.acc_id, p.logical_db as other_ldb " + 
				"from sequence seq " + 
				"inner join probe_to_sequence pts on (pts.sequence_key = seq.sequence_key) " + 
				"inner join sequence_sequence_num s on (pts.sequence_key = s.sequence_key) " + 
				"inner join probe_id p on (pts.probe_key = p.probe_key) " + 
				"order by seq.primary_id";
		
		String lastPrimaryID = "";
		DocBuilder seq = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		QSAccIDFormatter idf = null;
		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// If we have a new primary ID, then we have a new sequence.  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				seq = new DocBuilder(primaryID, rs.getString("description"), "Sequence",
					rs.getString("sequence_type"), "/sequence/" + primaryID);
				lastPrimaryID = primaryID;
				gc();
			}

			// Index the associated probe ID.
			String otherID = rs.getString("acc_id");
			idf = idFactory.getFormatter("Sequence", cleanLogicalDB(rs.getString("other_ldb")), otherID);
			this.buildAndAddDocument(seq, otherID, idf.getMatchDisplay(), idf.getMatchType(),
				SECONDARY_ID_WEIGHT, primaryID, seqNum);
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " probe IDs for sequences");
	}

	/* Add documents to the index for mapping experiments. We're currently seeing almost 24,000 experiments.  Of those,
	 * about half have only 1 ID.  Most of the other half have 2, though some do have 3 and 4.
	 */
	private void indexMapping() throws Exception {
		logger.info(" - indexing mapping");
		
		long startSeqNum = seqNum;
		
		// Use the reference's mini-citation for display, but strip out any "et al" strings.
		String cmd = "select s.primary_id, 'MGI' as primary_ldb, s.experiment_type, " + 
			"	replace(r.mini_citation, 'et al.,', '') as description, " + 
			"	i.acc_id as other_id, i.logical_db as other_ldb " + 
			"from mapping_experiment s " + 
			"inner join reference r on (s.reference_key = r.reference_key) " + 
			"left outer join mapping_id i on (s.experiment_key = i.experiment_key and i.private = 0) " + 
			"order by s.primary_id";
		
		String lastPrimaryID = "";
		DocBuilder expt = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// If we have a new primary ID, then we have a new experiment  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				expt = new DocBuilder(primaryID, rs.getString("description"), "Mapping Experiment",
					rs.getString("experiment_type"), "/mapping/" + primaryID);
				
				// Index the primary ID.
				idf = idFactory.getFormatter("Mapping Experiment", rs.getString("primary_ldb"), primaryID);
				this.buildAndAddDocument(expt, primaryID, idf.getMatchDisplay(), idf.getMatchType(),
					PRIMARY_ID_WEIGHT, primaryID, seqNum++);

				lastPrimaryID = primaryID;
				gc();
			}

			// Also index the other ID if it differs from the primary.
			String otherID = rs.getString("other_id");
			if (!primaryID.equals(otherID)) {
				idf = idFactory.getFormatter("Mapping Experiment", rs.getString("other_ldb"), otherID);
				this.buildAndAddDocument(expt, otherID, idf.getMatchDisplay(), idf.getMatchType(), 
					SECONDARY_ID_WEIGHT, primaryID, seqNum);
			}
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " mapping experiments");
	}

	/* Add documents to the index for probes and clones. We're currently seeing almost 2.2 million.
	 * Over 97% have multiple IDs.  Over 2 million have clone IDs (stored in with other IDs).
	 */
	private void indexProbes() throws Exception {
		logger.info(" - indexing probes and clones");
		
		long startSeqNum = seqNum;
		
		String cmd = "select p.primary_id, p.name, p.segment_type, p.logical_db, " +
			"  i.acc_id, i.logical_db as other_ldb, s.by_name " + 
			"from probe p " + 
			"inner join probe_sequence_num s on (p.probe_key = s.probe_key) " + 
			"left outer join probe_id i on (p.probe_key = i.probe_key) " +
			"order by p.primary_id";
		
		String lastPrimaryID = "";
		DocBuilder probe = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// If we have a new primary ID, then we have a new probe.  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				probe = new DocBuilder(primaryID, rs.getString("name"), "Probe/Clone",
					rs.getString("segment_type"), "/probe/" + primaryID);
				
				// Index the primary ID.
				idf = idFactory.getFormatter("Probe/Clone", rs.getString("logical_db"), primaryID);
				this.buildAndAddDocument(probe, primaryID, idf.getMatchDisplay(), idf.getMatchType(),
					PRIMARY_ID_WEIGHT, primaryID, seqNum++);

				lastPrimaryID = primaryID;
				gc();
			}

			// Also index the other ID if it differs from the primary.
			String otherID = rs.getString("acc_id");
			if (!primaryID.equals(otherID)) {
				idf = idFactory.getFormatter("Probe/Clone", rs.getString("other_ldb"), otherID);
				this.buildAndAddDocument(probe, otherID, idf.getMatchDisplay(), idf.getMatchType(),
					SECONDARY_ID_WEIGHT, primaryID, seqNum);
			}
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " probes and clones");
	}

	/* Add documents to the index to associate sequence IDs with related probes and clones.  79% of
	 * probes have at least one sequence associated.  Most (82%) of those have exactly one sequence,
	 * but some have up to fifty.
	 */
	private void indexSequenceIDsForProbes() throws Exception {
		logger.info(" - indexing sequence IDs for probes and clones");
		
		long startSeqNum = seqNum;
		
		String cmd = "select p.primary_id, p.name, p.segment_type, p.logical_db, " + 
				"	  seq.acc_id, seq.logical_db as other_ldb, s.by_name " + 
				"from probe p " + 
				"inner join probe_sequence_num s on (p.probe_key = s.probe_key) " + 
				"inner join probe_to_sequence pts on (p.probe_key = pts.probe_key) " + 
				"inner join sequence_id seq on (pts.sequence_key = seq.sequence_key) " + 
				"order by p.primary_id";
		
		String lastPrimaryID = "";		// primary ID of probes (since we're walking through them)
		DocBuilder seq = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// If we have a new primary ID, then we have a new probe.  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				seq = new DocBuilder(primaryID, rs.getString("name"), "Probe/Clone",
					rs.getString("segment_type"), "/probe/" + primaryID);
				
				lastPrimaryID = primaryID;
				seqNum++;
				gc();
			}

			// Now index the sequence IDs for the sequence.
			String otherID = rs.getString("acc_id");
			idf = idFactory.getFormatter("Sequence", cleanLogicalDB(rs.getString("other_ldb")), otherID);
			this.buildAndAddDocument(seq, otherID, idf.getMatchDisplay(), idf.getMatchType(),
				SECONDARY_ID_WEIGHT, primaryID, seqNum);
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " sequence IDs for probes and clones");
	}

	/* Generate string to describe homology class based on given counts.
	 */
	private String getHomologyClassDescription(String clusterKey, Map<String, String> mouseMarkers,
			int mouseCount, int humanCount, int ratCount, int zebrafishCount) {
		StringBuffer sb = new StringBuffer("Class with ");
		String spacer = "";		// no spacer needed for first present organism
		String comma = ", ";
		
		if (humanCount > 0) {
			sb.append(spacer);
			sb.append(humanCount + " human");
			spacer = comma;
		}
		if (mouseCount > 0) {
			sb.append(spacer);
			sb.append(mouseCount + " mouse");
			spacer = comma;
			
			if (mouseMarkers.containsKey(clusterKey)) {
				sb.append(" (");
				sb.append(mouseMarkers.get(clusterKey));
				sb.append(")");
			}
		}
		if (ratCount > 0) {
			sb.append(spacer);
			sb.append(ratCount + " rat");
			spacer = comma;
		}
		if (zebrafishCount > 0) {
			sb.append(spacer);
			sb.append(zebrafishCount + " zebrafish");
			spacer = comma;
		}

		return sb.toString();
	}
	
	/* Build a cache of mouse marker symbols per homology cluster, mapping from cluster key to String.  Note
	 * that there will be (at most) one symbol per cluster currently, but this is built in anticipation of
	 * clusters coming in that are paralogy-aware (more than one mouse marker per cluster).
	 */
	private Map<String, String> getMarkersForClusters() throws Exception {
		logger.info(" - getting mouse symbols for homology clusters");
		
		Map<String, String> markers = new HashMap<String, String>();
		int markerCount = 0;
		
		String cmd = "select hc.cluster_key, m.symbol "
			+ "from homology_cluster hc, homology_cluster_organism hco, "
			+ "  homology_cluster_organism_to_marker hcom, marker m "
			+ "where hc.source = 'Alliance Direct' "
			+ "  and hc.cluster_key = hco.cluster_key "
			+ "  and hco.organism = 'mouse' "
			+ "  and hco.cluster_organism_key = hcom.cluster_organism_key "
			+ "  and hcom.marker_key = m.marker_key "
			+ "order by 1, 2";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		while (rs.next())  {  
			String clusterKey = rs.getString("cluster_key");
			String symbol = rs.getString("symbol");
			
			if (!markers.containsKey(clusterKey)) {
				markers.put(clusterKey, symbol);
			} else {
				markers.put(clusterKey, markers.get(clusterKey) + ", " + symbol);
			}
		}

		rs.close();
		logger.info("found " + markerCount + " mouse markers for " + markers.size() + " homology clusters");
		
		return markers;
	}
	
	/* Get a cache of facets (high-level slim terms) for various filterable fields.
	 */
	private Map<String,Set<String>> getFacetValues(String dagAbbrev) throws Exception {
		Map<String,Set<String>> keyToFacets = new HashMap<String,Set<String>>();
		String cmd = null;

		// Maps from homology cluster key to the feature types associated with the mouse marker
		// in the cluster.  If a cluster has no mouse markers, it will not be considered.  Assumes
		// each cluster has (at most) one mouse marker.
		if ("Feature Type".equals(dagAbbrev)) {
			cmd = "with ancestors as (select a.ancestor_term, t.term  " + 
					"  from term t, term_ancestor a " + 
					"  where t.vocab_name = 'Marker Category' " + 
					"    and t.term_key = a.term_key " + 
					"    and a.ancestor_term != 'other feature type' " + 
					"    and a.ancestor_term != 'other genome feature' " + 
					"    and a.ancestor_term != 'all feature types' " + 
					"), " + 
					"mouse_markers as (select hc.cluster_key, m.marker_key " + 
					"  from marker m, homology_cluster_organism_to_marker otm, " + 
					"    homology_cluster_organism o, homology_cluster hc " + 
					"  where m.organism = 'mouse' " + 
					"    and m.marker_key = otm.marker_key " + 
					"    and otm.cluster_organism_key = o.cluster_organism_key " + 
					"    and o.cluster_key = hc.cluster_key " + 
					"    and hc.source = 'Alliance Direct' " +
					") " +
					"select mm.cluster_key as object_key, m.marker_subtype as term " + 
					"from marker m, mouse_markers mm " + 
					"where m.organism = 'mouse' " + 
					"  and m.status != 'withdrawn' " + 
					"  and m.marker_key = mm.marker_key " +
					"union " + 
					"select mm.cluster_key as object_key, a.ancestor_term as term " + 
					"from marker m, ancestors a, mouse_markers mm " + 
					"where m.marker_subtype = a.term " + 
					"  and m.organism = 'mouse' " + 
					"  and m.marker_key = mm.marker_key " +
					"  and m.status != 'withdrawn'";
		}
		
		if (cmd != null) {
			ResultSet rs = ex.executeProto(cmd, cursorLimit);
			while (rs.next()) {
				String key = rs.getString("object_key");
				
				if (!keyToFacets.containsKey(key)) {
					keyToFacets.put(key, new HashSet<String>());
				}
				keyToFacets.get(key).add(rs.getString("term"));
			}
			rs.close();
			logger.info("Collected " + dagAbbrev + " facets for " + keyToFacets.size() + " objects");
		}
		return keyToFacets;
	}
	
	/* Add documents to the index for homology clusters. There are currently almost 21,000 of these.  These have
	 * no IDs of their own, but should be returned by non-mouse marker IDs.  For OMIM IDs, we should index both
	 * with and without the OMIM prefix.
	 */
	private void indexHomologyClasses() throws Exception {
		logger.info(" - indexing homology clusters");
		
		Map<String, String> mouseMarkers = this.getMarkersForClusters();
		Map<String, Set<String>> markerTypes = this.getFacetValues("Feature Type");

		long startSeqNum = seqNum;
		
		String cmd = "select c.cluster_key, ct.mouse_marker_count, ct.human_marker_count, " + 
				"  ct.rat_marker_count, ct.zebrafish_marker_count, i.acc_id, i.logical_db, hco.organism " + 
				"from homology_cluster c, homology_cluster_counts ct, marker_id i, " + 
				"  homology_cluster_organism hco, homology_cluster_organism_to_marker hm " + 
				"where c.source = 'Alliance Direct' " + 
				"  and c.cluster_key = ct.cluster_key " + 
				"  and c.cluster_key = hco.cluster_key " + 
				"  and hco.organism in ('rat', 'human', 'zebrafish') " + 
				"  and hco.cluster_organism_key = hm.cluster_organism_key " + 
				"  and hm.marker_key = i.marker_key " + 
				"  and (i.private = 0 or i.logical_db = 'Rat Genome Database')" +
				"order by c.cluster_key, hco.organism, i.acc_id";
		
		String lastPrimaryID = "";
		String description = "";
		DocBuilder cluster = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("cluster_key");
			String organism = rs.getString("organism");
			
			// If we have a new primary ID, then we have a new homology class  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				description = this.getHomologyClassDescription(primaryID, mouseMarkers, rs.getInt("mouse_marker_count"),
					rs.getInt("human_marker_count"), rs.getInt("rat_marker_count"), rs.getInt("zebrafish_marker_count"));
				cluster = new DocBuilder(primaryID, description, "Homology", null, "/homology/cluster/key/" + primaryID);
				
				// If we have a mouse marker, then we can filter this cluster by mouse Feature Type.  Look it up and
				// add it to the DocBuilder.
				if (markerTypes.containsKey(primaryID)) {
					cluster.setMarkerTypeFacets(markerTypes.get(primaryID));
				}
				
				seqNum++;
				lastPrimaryID = primaryID;
				gc();
			}

			// Index the organism's ID.
			String accID = rs.getString("acc_id");
			
			idf = idFactory.getFormatter("Homology", rs.getString("logical_db"), accID, organism);
			this.buildAndAddDocument(cluster, accID, idf.getMatchDisplay(), idf.getMatchType(), PRIMARY_ID_WEIGHT, primaryID, seqNum);
			
			// OMIM IDs also get a version without the prefix.
			if (accID.startsWith("OMIM:")) {
				accID = accID.replace("OMIM:", "");
				idf = idFactory.getFormatter("Homology", rs.getString("logical_db"), accID, organism);
				this.buildAndAddDocument(cluster, accID, idf.getMatchDisplay(), idf.getMatchType(), PRIMARY_ID_WEIGHT, primaryID, seqNum);
			}
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " homology clusters");
	}

	/* Add documents to the index for AMA terms.  There are currently over 3200 AMA terms, with
	 * roughly 99% having only 1 ID.
	 */
	private void indexAdultMouseAnatomy() throws Exception {
		logger.info(" - indexing Adult Mouse Anatomy");
		
		long startSeqNum = seqNum;
		
		String cmd = "select t.primary_id, t.term, i.acc_id, t.vocab_name, s.by_dfs " + 
				"from term t, term_id i, term_sequence_num s " + 
				"where t.vocab_name = 'Adult Mouse Anatomy' " + 
				"and t.term_key = i.term_key " + 
				"and t.term_key = s.term_key " + 
				"order by s.by_dfs";
		
		String lastPrimaryID = "";
		DocBuilder term = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// If we have a new primary ID, then we have a new term.  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				term = new DocBuilder(primaryID, rs.getString("term"), "MA Browser Detail", null,
					"/vocab/gxd/ma_ontology/" + primaryID);
				
				// Index the primary ID.
				idf = idFactory.getFormatter("Adult Mouse Anatomy", "Adult Mouse Anatomy", primaryID);
				this.buildAndAddDocument(term, primaryID, idf.getMatchDisplay(), idf.getMatchType(),
					PRIMARY_ID_WEIGHT, primaryID, seqNum++);

				lastPrimaryID = primaryID;
				gc();
			}

			// Also index the other ID if it differs from the primary.
			String otherID = rs.getString("acc_id");
			if (!primaryID.equals(otherID)) {
				idf = idFactory.getFormatter("Adult Mouse Anatomy", "Adult Mouse Anatomy", otherID);
				this.buildAndAddDocument(term, otherID, idf.getMatchDisplay(), idf.getMatchType(),
					SECONDARY_ID_WEIGHT, primaryID, seqNum);
			}
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " AMA terms");
	}

	/* Add documents to the index for references.  There are currently almost 300k references, with
	 * over 1m references.
	 */
	private void indexReferences() throws Exception {
		logger.info(" - indexing references");
		
		long startSeqNum = seqNum;
		
		// description has a couple tweaks to remove "et al." and to insert "()" between ";:"
		String cmd = "select r.jnum_id as primary_id, i.logical_db, i.acc_id, s.by_primary_id, " + 
				"  replace(replace(r.mini_citation, 'et al.,', ''), ';:', ';():') as description " + 
				"from reference r, reference_sequence_num s, reference_id i " + 
				"where r.reference_key = s.reference_key " + 
				"  and r.reference_key = i.reference_key " + 
				"order by s.by_primary_id";
		
		String lastPrimaryID = "";
		DocBuilder ref = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// If we have a new primary ID, then we have a new reference.  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				ref = new DocBuilder(primaryID, rs.getString("description"), "Reference", null,
					"/reference/" + primaryID);
				
				// Index the primary ID.
				idf = idFactory.getFormatter("Reference", "MGI", primaryID);
				this.buildAndAddDocument(ref, primaryID, idf.getMatchDisplay(), idf.getMatchType(), PRIMARY_ID_WEIGHT, primaryID, seqNum++);

				lastPrimaryID = primaryID;
				gc();
			}

			// Also index the other ID if it differs from the primary.
			String otherID = rs.getString("acc_id");
			if (!primaryID.equals(otherID)) {
				idf = idFactory.getFormatter("Reference", rs.getString("logical_db"), rs.getString("acc_id"));
				this.buildAndAddDocument(ref, otherID, idf.getMatchDisplay(), idf.getMatchType(), SECONDARY_ID_WEIGHT, primaryID, seqNum);
			}
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " references");
	}

	/* Add documents to the index for genotypes with phenotype and/or disease data.  There are currently 
	 * over 61k of such genotypes.  All have only 1 ID.
	 */
	private void indexGenotypes() throws Exception {
		logger.info(" - indexing genotypes");
		
		long startSeqNum = seqNum;
		
		String cmd = "select g.primary_id " + 
				"from genotype g " + 
				"where exists (select 1 from genotype_to_annotation r where g.genotype_key = r.genotype_key) "+
				"order by g.primary_id";
		
		DocBuilder term = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			term = new DocBuilder(primaryID, primaryID, "Genotype", null, "/accession/" + primaryID);
				
			idf = idFactory.getFormatter("Genotype", "MGI", primaryID);
			this.buildAndAddDocument(term, primaryID, idf.getMatchDisplay(), idf.getMatchType(), PRIMARY_ID_WEIGHT, primaryID, seqNum++);
			gc();
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " genotypes");
	}

	/* Add documents to the index for antibodies.  There are currently about 12k, each with only 1 ID.
	 */
	private void indexAntibodies() throws Exception {
		logger.info(" - indexing antibodies");
		
		long startSeqNum = seqNum;
		
		String cmd = "select primary_id, name " + 
				"from antibody " + 
				"order by primary_id";
		
		DocBuilder antibody = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			antibody = new DocBuilder(primaryID, rs.getString("name"), "Antibody", null, "/antibody/" + primaryID);
				
			idf = idFactory.getFormatter("Antibody", "MGI", primaryID);
			this.buildAndAddDocument(antibody, primaryID, idf.getMatchDisplay(), idf.getMatchType(), PRIMARY_ID_WEIGHT, primaryID, seqNum++);

			gc();
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " antibodies");
	}

	/* Add documents to the index for Expression and Phenotype images (not molecular images).
	 * There are currently about 270k expression and 8,700 phenotype images.  Many have multiple IDs.
	 */
	private void indexImages() throws Exception {
		logger.info(" - indexing expression and phenotype images");
		
		long startSeqNum = seqNum;
		
		// Primary ID will be the MGI ID; any non-MGI IDs will come back in acc_id field.  (null if
		// no non-MGI IDs)  Note that there is--at most--one secondary ID for each image.
		String cmd = "select replace(i.image_class, 'Phenotypes', 'Phenotype') as image_type, " +
			"  i.mgi_id, d.logical_db, d.acc_id, s.by_default " + 
			"from image i " +
			"inner join image_sequence_num s on (i.image_key = s.image_key) " + 
			"left outer join image_id d on (i.image_key = d.image_key " + 
			"  and d.logical_db not like 'MGI%') " + 
			"where i.image_class in ('Expression', 'Phenotypes') " + 
			"order by s.by_default, d.acc_id";
		
		String lastPrimaryID = "";
		DocBuilder image = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String mgiID = rs.getString("mgi_id");
			String secondaryID = rs.getString("acc_id");
			
			// If we have a new MGI ID, then we have a new image.  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(mgiID)) {
				if (secondaryID == null) {
					image = new DocBuilder(mgiID, mgiID, rs.getString("image_type") + " Image", null,
						"/image/" + mgiID);
				} else {
					image = new DocBuilder(mgiID, secondaryID, rs.getString("image_type") + " Image", null,
						"/image/" + mgiID);
				}
				
				// Index the primary ID.
				idf = idFactory.getFormatter("Image", "MGI", mgiID);
				this.buildAndAddDocument(image, mgiID, idf.getMatchDisplay(), idf.getMatchType(),
					PRIMARY_ID_WEIGHT, mgiID, seqNum++);

				lastPrimaryID = mgiID;
				gc();
			}

			// Also index the other ID if non-null.
			if (secondaryID != null) {
				idf = idFactory.getFormatter("Image", rs.getString("logical_db"), secondaryID);
				this.buildAndAddDocument(image, secondaryID, idf.getMatchDisplay(), idf.getMatchType(),
					SECONDARY_ID_WEIGHT, mgiID, seqNum);
				
				// For GenePaint IDs, we also want to index them without the pane number after a slash.  And,
				// since GenePaint IDs are not primary IDs, so we can just do it here in the secondary ID section.
				// (They are also the only image IDs containing slashes.)
				if ((secondaryID.indexOf('/') >= 0) && ("GenePaint".equals(rs.getString("logical_db")))) {
					String gpID = secondaryID.split("/")[0];
					idf = idFactory.getFormatter("Image", rs.getString("logical_db"), gpID);
					this.buildAndAddDocument(image, gpID, idf.getMatchDisplay(), idf.getMatchType(),
						SECONDARY_ID_WEIGHT, mgiID, seqNum);
				}
			}
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " expression and phenotype images");
	}

	/* Add documents to the index for classical GXD assays, of which there are currently over 106k.  Each has 1 ID.
	 */
	private void indexClassicalGxdAssays() throws Exception {
		logger.info(" - indexing classical expression assays");
		
		long startSeqNum = seqNum;
		
		String cmd = "select a.primary_id, a.marker_symbol, a.marker_name from expression_assay a";
		
		DocBuilder assay = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			assay = new DocBuilder(primaryID, rs.getString("marker_symbol") + ", " + rs.getString("marker_name"),
				"Expression Assay", null, "/assay/" + primaryID);
				
			// Index the primary ID.
			idf = idFactory.getFormatter("Expression Assay", "MGI", primaryID);
			this.buildAndAddDocument(assay, primaryID, idf.getMatchDisplay(), idf.getMatchType(), PRIMARY_ID_WEIGHT, primaryID, seqNum++);

			gc();
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " classical expression assays");
	}

	/* Add documents to the index for high-throughput expression assays, of which there are over 3,100.  Many have
	 * multiple IDs.
	 */
	private void indexHighThroughputGxdAssays() throws Exception {
		logger.info(" - indexing high-throughput expression assays");
		
		long startSeqNum = seqNum;
		
		String cmd = "select e.primary_id, e.name, i.acc_id, i.logical_db " + 
			"from expression_ht_experiment e, expression_ht_experiment_id i " + 
			"where e.experiment_key = i.experiment_key " + 
			"order by e.primary_id";
		
		String lastPrimaryID = "";
		DocBuilder experiment = null;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());
		QSAccIDFormatter idf = null;

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// If we have a new primary ID, then we have a new experiment.  We'll need a new DocBuilder.
			if (!lastPrimaryID.equals(primaryID)) {
				experiment = new DocBuilder(primaryID, rs.getString("name"), "RNA-Seq/Array Experiment", null,
					"/gxd/htexp_index/summary?arrayExpressID=" + primaryID);
				
				lastPrimaryID = primaryID;
				gc();
			}

			String otherID = rs.getString("acc_id");
			idf = idFactory.getFormatter("RNA-Seq/Array Experiment", rs.getString("logical_db"), otherID);
			this.buildAndAddDocument(experiment, otherID, idf.getMatchDisplay(), idf.getMatchType(), SECONDARY_ID_WEIGHT, primaryID, seqNum);
		}

		rs.close();
		logger.info("done with " + (seqNum - startSeqNum) + " high-throughput expression assays");
	}

	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		logger.info("beginning other bucket");
		
		indexSequences();
		indexSequenceIDsForProbes();
		indexProbes();
		indexProbeIDsForSequences();
		indexMapping();
		indexHomologyClasses();
		indexAdultMouseAnatomy();
		indexReferences();
		indexGenotypes();
		indexAntibodies();
		indexImages();
		indexClassicalGxdAssays();
		indexHighThroughputGxdAssays();

		// any leftover docs to send to the server?  (likely yes)
		if (docs.size() > 0) { writeDocs(docs); }

		// commit all the changes to Solr
		commit(false);

		logger.info("finished other bucket");
	}

	// Private class for helping generate Solr documents.  Instantiate with the current object type being worked on.
	private class DocBuilder {
		private String primaryID;
		private String name;
		private String objectType;
		private String objectSubType;
		private String detailUri;
		public Set<String> markerTypeFacets;
		
		private DocBuilder (String primaryID, String name, String objectType, String objectSubType, String detailUri) {
			this.primaryID = primaryID;
			this.name = name;
			this.objectType = objectType;
			this.objectSubType = objectSubType;
			this.detailUri = detailUri;
		}
		
		public void setMarkerTypeFacets(Set<String> markerTypeFacets) {
			this.markerTypeFacets = markerTypeFacets;
		}

		// compose and return a new SolrInputDocument including the fields for this feature
		public SolrInputDocument getNewDocument() {
			SolrInputDocument doc = new SolrInputDocument();

			if (this.primaryID != null) { doc.addField(IndexConstants.QS_PRIMARY_ID, this.primaryID); }
			if (this.name != null) { doc.addField(IndexConstants.QS_NAME, this.name); }
			if (this.objectType != null) { doc.addField(IndexConstants.QS_OBJECT_TYPE, this.objectType); }
			if (this.objectSubType != null) { doc.addField(IndexConstants.QS_OBJECT_SUBTYPE, this.objectSubType); }
			if (this.detailUri != null) { doc.addField(IndexConstants.QS_DETAIL_URI, this.detailUri); }
			if (this.markerTypeFacets != null) { doc.addField(IndexConstants.QS_MARKER_TYPE_FACETS, this.markerTypeFacets); }

			return doc;
		}
	}
}