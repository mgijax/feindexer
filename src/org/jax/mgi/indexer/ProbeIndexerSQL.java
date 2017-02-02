package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.mgi.shr.jsonmodel.MolecularProbe;
import org.jax.mgi.shr.jsonmodel.MolecularProbeMarker;

/* Is: an indexer that builds the index supporting the molecular probe summary page (reachable from the
 * 		marker detail page and the reference summary/detail page).  Each document in the index represents
 * 		data for a single molecular probe, and each probe is represented in only one document.
 * Notes: The index is intended to support searches by a marker ID, reference ID, and segmenType.  Details
 * 		of the probes are encapsulated in a JSON object in the probe field.
 */
public class ProbeIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Map<Integer,List<MolecularProbeMarker>> markerCache = null;		// displayed markers per probe key
	private Map<Integer,List<String>> searchableMarkers = null;		// searchable marker IDs per probe key
	private Map<Integer,List<String>> searchableReferences = null;	// searchable reference IDs per probe key
	private Map<Integer,List<String>> collectionCache = null;		// collections per probe key

	private ObjectMapper mapper = new ObjectMapper();				// converts objects to JSON

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public ProbeIndexerSQL() {
		super("probe");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* gather the clone collections from the database and cache them in the global collectionCache
	 */
	private void cacheCollections() throws Exception {
		logger.info("caching collections");
		String cmd = "select distinct c.probe_key, c.collection "
			+ "from probe_clone_collection c "
			+ "order by 1, 2";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished clone collection query in " + ex.getTimestamp());

		int i = 0;
		collectionCache = new HashMap<Integer,List<String>>();

		SmartAlphaComparator smComparator = new SmartAlphaComparator();
		
		while (rs.next()) {
			i++;
			Integer probeKey = rs.getInt("probe_key");
			String collection = rs.getString("collection");
			if (collectionCache.containsKey(probeKey)) {
				collectionCache.get(probeKey).add(collection);
				Collections.sort(collectionCache.get(probeKey), smComparator);
			} else {
				collectionCache.put(probeKey, new ArrayList<String>());
				collectionCache.get(probeKey).add(collection);
			}
		}
		rs.close();
		logger.info("  - done caching " + i + " collection/probe pairs");
	}
	
	/* retrieve the clone collections for the given probe key.  Assumes cacheCollections()
	 * has been called.
	 */
	private List<String> getCollections(int probeKey) throws Exception {
		if (collectionCache.containsKey(probeKey)) {
			return collectionCache.get(probeKey);
		}
		return null;
	}
	
	/* retrieve the markers associated with each probe and store them in the global markerCache
	 */
	private void cacheMarkers() throws Exception {
		logger.info("caching marker locations");
		
		// ordered to prefer centimorgans to cytogenetic band to coordinates
		String cmd1 = "select distinct ml.marker_key, ml.chromosome, ml.cm_offset, "
			+ "  ml.cytogenetic_offset, ml.start_coordinate "
			+ "from marker_to_probe p, marker_location ml, marker m "
			+ "where p.marker_key = m.marker_key "
			+ "  and m.organism = 'mouse' "
			+ "  and m.status = 'official' "
			+ "  and m.marker_key = ml.marker_key "
			+ "order by ml.marker_key, ml.cm_offset, ml.cytogenetic_offset, ml.start_coordinate";
		
		Map<Integer,String> locations = new HashMap<Integer,String>();
		DecimalFormat formatter = new DecimalFormat("#.00", DecimalFormatSymbols.getInstance(Locale.US));

		ResultSet rs1 = ex.executeProto(cmd1, cursorLimit);
		while (rs1.next()) {
			Integer markerKey = rs1.getInt("marker_key");

			if (!locations.containsKey(markerKey)) {
				String chromosome = rs1.getString("chromosome");
				Double cmOffset = rs1.getDouble("cm_offset");

				// if cM offset, show it
				// else if cytogenetic band, show it
				// else just show chromosome
				if ((cmOffset == null) || (cmOffset <= 0.0)) {
					String cytoband = rs1.getString("cytogenetic_offset");
					if ((cytoband != null) && (cytoband.length() > 0)) {
						locations.put(markerKey, chromosome + " (" + cytoband + ")");
					} else {
						locations.put(markerKey, chromosome);
					}
				} else {
					locations.put(markerKey, chromosome + " (" + formatter.format(cmOffset) + " cM)");
				}
			}
		}
		rs1.close();
		logger.info(" - cached " + locations.size() + " marker locations");

		logger.info("caching markers");
		String cmd = "select distinct mtp.probe_key, m.symbol, m.primary_id as marker_id, "
			+ " s.by_symbol, mtp.qualifier, m.marker_key "
			+ "from marker_to_probe mtp, marker m, marker_sequence_num s "
			+ "where mtp.marker_key = m.marker_key "
			+ " and m.marker_key = s.marker_key "
			+ "order by s.by_symbol";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished marker query in " + ex.getTimestamp());

		int i = 0;
		markerCache = new HashMap<Integer,List<MolecularProbeMarker>>();
		searchableMarkers = new HashMap<Integer,List<String>>();
		
		while (rs.next()) {
			Integer probeKey = rs.getInt("probe_key");
			String qualifier = rs.getString("qualifier");
			String markerID = rs.getString("marker_id");
			Integer markerKey = rs.getInt("marker_key");
			i++;

			MolecularProbeMarker marker = new MolecularProbeMarker();
			marker.setSymbol(rs.getString("symbol"));
			marker.setPrimaryID(markerID);
			if ("P".equals(qualifier)) {
				marker.setIsPutative(true);
			}
			if (locations.containsKey(markerKey)) {
				marker.setLocation(locations.get(markerKey));
			}
			
			// any markers associated with the probe go into the cache of markers for display
			if (!markerCache.containsKey(probeKey)) {
				markerCache.put(probeKey, new ArrayList<MolecularProbeMarker>());
			}
			markerCache.get(probeKey).add(marker);
			
			if (!searchableMarkers.containsKey(probeKey)) {
				searchableMarkers.put(probeKey, new ArrayList<String>());
			}
			searchableMarkers.get(probeKey).add(markerID);
		}
		rs.close();
		logger.info("  - done caching " + i + " marker/probe pairs");
		logger.info("  - found " + searchableMarkers.size() + " probes that can be retrieved by marker ID");
	}
	
	/* retrieve the markers associated with the given probe for display purposes.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<MolecularProbeMarker> getMarkers(int probeKey) throws Exception {
		if (markerCache.containsKey(probeKey)) {
			return markerCache.get(probeKey);
		}
		return null;
	}

	/* retrieve the markers IDs that can be used to retrieve a given probe.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<String> getSearchableMarkers(int probeKey) throws Exception {
		if (searchableMarkers.containsKey(probeKey)) {
			return searchableMarkers.get(probeKey);
		}
		return null;
	}
	
	/* cache the set of reference IDs that can be used to retrieve each probe. (MGI, PubMed)
	 */
	private void cacheReferences() throws Exception {
		logger.info("caching references");
		searchableReferences = new HashMap<Integer,List<String>>();

		String cmd = "select p.probe_key, r.acc_id "
			+ "from reference_id r, probe_to_reference p "
			+ "where r.logical_db in ('PubMed', 'MGI') "
			+ " and r.reference_key = p.reference_key";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		
		int ct = 0;
		while (rs.next()) {
			ct++;
			Integer probeKey = rs.getInt("probe_key");
			if (!searchableReferences.containsKey(probeKey)) {
				searchableReferences.put(probeKey, new ArrayList<String>());
			}
			searchableReferences.get(probeKey).add(rs.getString("acc_id"));
		}
		rs.close();
		logger.info(" - cached " + ct + " reference IDs for " + searchableReferences.size() + " probes");
	}
	
	/* retrieve the reference IDs that can be used to retrieve a given probe.  Assumes cacheReferences()
	 * has been called.
	 */
	private List<String> getReferenceIDs(int probeKey) throws Exception {
		if (searchableReferences.containsKey(probeKey)) {
			return searchableReferences.get(probeKey);
		}
		return null;
	}
	
	/* get a segment type that can be used to restrict the query results.  This includes:
	 * 	genomic, cDNA, primer, and other
	 */
	private String getSearchableSegmentType(String segmentType) {
		if ("genomic".equalsIgnoreCase(segmentType)) { return segmentType; }
		else if ("cdna".equalsIgnoreCase(segmentType)) { return segmentType; }
		else if ("primer pair".equalsIgnoreCase(segmentType)) { return "primer"; }
		else if ("primer".equalsIgnoreCase(segmentType)) { return "primer"; }
		return "other";
	}

	/* retrieve probes from the database, build Solr docs, and write them to the server.
	 */
	private void processProbes() throws Exception {
		logger.info("loading probes");
		
		String cmd = "select p.probe_key, p.name, p.primary_id, p.segment_type, s.by_name, s.by_type "
				+ "from probe p, probe_sequence_num s "
				+ "where p.probe_key = s.probe_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished main probe query in " + ex.getTimestamp());

		int i = 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			Integer probeKey = rs.getInt("probe_key");

			// start building the object that we will store as JSON in the probe field
			// of the index
			MolecularProbe probe = new MolecularProbe();
			probe.setName(rs.getString("name"));
			probe.setPrimaryID(rs.getString("primary_id"));
			probe.setSegmentType(rs.getString("segment_type"));
			
			// start building the solr document (will have only four fields total)
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.PRB_KEY, probeKey);
			doc.addField(IndexConstants.PRB_BY_NAME, rs.getInt("by_name"));
			doc.addField(IndexConstants.PRB_BY_TYPE, rs.getInt("by_type"));
			doc.addField(IndexConstants.PRB_SEGMENT_TYPE, getSearchableSegmentType(rs.getString("segment_type")));
			
			// if probe has associated markers, add them to the probe object
			List<MolecularProbeMarker> markers = getMarkers(probeKey);
			if (markers != null) {
				probe.setMarkers(markers);
			}

			// and add the marker IDs to the searchable marker ID field in the index
			List<String> searchableMarkerIDs = getSearchableMarkers(probeKey);
			if (searchableMarkerIDs != null) {
				for (String markerID : searchableMarkerIDs) {
					doc.addField(IndexConstants.PRB_MARKER_ID, markerID);
				}
			}

			// add the reference IDs to the searchable reference ID field in the index
			List<String> searchableReferenceIDs = getReferenceIDs(probeKey);
			if (searchableReferenceIDs != null) {
				for (String referenceID : searchableReferenceIDs) {
					doc.addField(IndexConstants.PRB_REFERENCE_ID, referenceID);
				}
			}

			// if any clone collections, add them to the probe object
			List<String> collections = getCollections(probeKey);
			if (collections != null) {
				probe.setCollections(collections);
			}

			doc.addField(IndexConstants.PRB_PROBE, mapper.writeValueAsString(probe));
			
			// Add this doc to the batch we're collecting.  If the stack hits our
			// threshold, send it to the server and reset it.
			docs.add(doc);
			if (docs.size() >= solrBatchSize)  {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		// any leftover docs to send to the server?  (likely yes)
		writeDocs(docs);
		commit();
		rs.close();

		logger.info("done processing " + i + " molecular probes");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// collect various mappings needed for data lookup
		cacheCollections();
		cacheMarkers();
		cacheReferences();
		processProbes();
	}
}
