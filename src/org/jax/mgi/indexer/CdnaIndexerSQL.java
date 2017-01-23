package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.mgi.shr.jsonmodel.Clone;
import org.jax.mgi.shr.jsonmodel.CloneMarker;

/* Is: an indexer that builds the index supporting the cDNA summary page (reachable from the
 * 		marker detail page).  Each document in the index represents data for a single cDNA clone,
 * 		and each clone is represented in only one document.
 * Notes: The index is intended to support searches only by a marker ID.  Details of the clones 
 * 		are encapsulated in a JSON object in the clone field.
 */
public class CdnaIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Map<Integer,List<CloneMarker>> markerCache = null;	// displayed markers per clone key
	private Map<Integer,List<String>> searchableMarkers = null;	// searchable marker IDs per clone key
	private Map<Integer,List<String>> collectionCache = null;	// collections per clone key

	private ObjectMapper mapper = new ObjectMapper();			// converts objects to JSON

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public CdnaIndexerSQL() {
		super("cdna");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* gather the clone collections from the database and cache them in the global collectionCache
	 */
	private void cacheCollections() throws Exception {
		logger.info("caching collections");
		String cmd = "select distinct p.probe_key, c.collection "
			+ "from probe_cdna p, probe_clone_collection c "
			+ "where p.probe_key = c.probe_key "
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
		logger.info("  - done caching " + i + " collection/clone pairs");
	}
	
	/* retrieve the clone collections for the given clone key.  Assumes cacheCollections()
	 * has been called.
	 */
	private List<String> getCollections(int cloneKey) throws Exception {
		if (collectionCache.containsKey(cloneKey)) {
			return collectionCache.get(cloneKey);
		}
		return null;
	}
	
	/* retrieve the markers associated with each clone and store them in the global markerCache
	 */
	private void cacheMarkers() throws Exception {
		logger.info("caching markers");
		String cmd = "select distinct p.probe_key, m.symbol, m.primary_id as marker_id, "
			+ " s.by_symbol, mtp.qualifier "
			+ "from probe_cdna p, marker_to_probe mtp, marker m, marker_sequence_num s "
			+ "where p.probe_key = mtp.probe_key "
			+ " and mtp.qualifier in ('E', 'P', 'H') "
			+ " and mtp.marker_key = m.marker_key "
			+ " and m.marker_key = s.marker_key "
			+ "order by s.by_symbol";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished marker query in " + ex.getTimestamp());

		int i = 0;
		markerCache = new HashMap<Integer,List<CloneMarker>>();
		searchableMarkers = new HashMap<Integer,List<String>>();
		
		while (rs.next()) {
			Integer probeKey = rs.getInt("probe_key");
			String qualifier = rs.getString("qualifier");
			String markerID = rs.getString("marker_id");
			i++;

			CloneMarker marker = new CloneMarker();
			marker.setSymbol(rs.getString("symbol"));
			marker.setPrimaryID(markerID);
			if ("P".equals(qualifier)) {
				marker.setIsPutative(true);
			}
			
			// any markers associated with the probe go into the cache of markers for display
			if (!markerCache.containsKey(probeKey)) {
				markerCache.put(probeKey, new ArrayList<CloneMarker>());
			}
			markerCache.get(probeKey).add(marker);
			
			// only markers associated via (E)ncodes or (P)utative relationships to a probe are
			// included in the list of marker IDs that can be used to retrieve that probe
			if ("E".equals(qualifier) || "P".equals(qualifier)) {
				if (!searchableMarkers.containsKey(probeKey)) {
					searchableMarkers.put(probeKey, new ArrayList<String>());
				}
				searchableMarkers.get(probeKey).add(markerID);
			}
		}
		rs.close();
		logger.info("  - done caching " + i + " marker/clone pairs");
		logger.info("  - found " + searchableMarkers.size() + " clones that can be retrieved by marker ID");
	}
	
	/* retrieve the markers associated with the given clone for display purposes.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<CloneMarker> getMarkers(int cloneKey) throws Exception {
		if (markerCache.containsKey(cloneKey)) {
			return markerCache.get(cloneKey);
		}
		return null;
	}

	/* retrieve the markers IDs that can be used to retrieve a given clone.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<String> getSearchableMarkers(int cloneKey) throws Exception {
		if (searchableMarkers.containsKey(cloneKey)) {
			return searchableMarkers.get(cloneKey);
		}
		return null;
	}
	
	/* retrieve clones from the database, build Solr docs, and write them to the server.
	 */
	private void processClones() throws Exception {
		logger.info("loading clones");
		
		// assumes that all the cDNA clones we want to index have records in the probe_cdna
		// table, which is generated in femover to be those with (E)ncodes or (P)utative
		// relationships with markers.
		String cmd = "select p.probe_key as clone_key, p.name, p.primary_id, c.age, "
				+ " c.tissue, c.cell_line, c.sequence_num "
				+ "from probe_cdna c, probe p "
				+ "where p.probe_key = c.probe_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished main clone query in " + ex.getTimestamp());

		int i = 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			Integer cloneKey = rs.getInt("clone_key");

			// start building the object that we will store as JSON in the clone field
			// of the index
			Clone clone = new Clone();
			clone.setAge(rs.getString("age"));
			clone.setCellLine(rs.getString("cell_line"));
			clone.setName(rs.getString("name"));
			clone.setPrimaryID(rs.getString("primary_id"));
			clone.setTissue(rs.getString("tissue"));
			
			// start building the solr document (will have only four fields total)
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.CDNA_KEY, cloneKey);
			doc.addField(IndexConstants.CDNA_SEQUENCE_NUM, rs.getInt("sequence_num"));
			
			// if clone has associated markers (and it must), add them to the clone object
			List<CloneMarker> markers = getMarkers(cloneKey);
			if (markers != null) {
				clone.setMarkers(markers);
			}

			// and add the IDs to the searchable field in the index
			List<String> searchableMarkerIDs = getSearchableMarkers(cloneKey);
			if (searchableMarkerIDs != null) {
				for (String markerID : searchableMarkerIDs) {
					doc.addField(IndexConstants.CDNA_MARKER_ID, markerID);
				}
			}

			// if any clone collections, add them to the clone object
			List<String> collections = getCollections(cloneKey);
			if (collections != null) {
				clone.setCollections(collections);
			}

			doc.addField(IndexConstants.CDNA_CLONE, mapper.writeValueAsString(clone));
			
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

		logger.info("done processing " + i + " cDNA clones");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// collect various mappings needed for data lookup
		cacheCollections();
		cacheMarkers();
		processClones();
	}
}
