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
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.mgi.shr.jsonmodel.AntibodyJ;

import com.fasterxml.jackson.databind.ObjectMapper;

/* Is: an indexer that builds the index supporting the antibody summary page (reachable from the
 * 		marker detail page and the reference summary/detail page).  Each document in the index represents
 * 		data for a single antibody, and each antibody represented in only one document.
 * Notes: The index is intended to support searches by a marker ID and reference ID.  Details
 * 		of the antibodies are encapsulated in a JSON object in the antibody field.
 */
public class AntibodyIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Map<Integer,List<String>> markerCache = null;		// displayed markers per antibody key
	private Map<Integer,List<String>> searchableMarkers = null;	// searchable marker IDs per antibody key
	private Map<Integer,List<String>> searchableReferences = null;	// searchable reference IDs per antibody key

	private ObjectMapper mapper = new ObjectMapper();		// converts objects to JSON

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public AntibodyIndexerSQL() {
		super("antibody");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* retrieve the markers associated with each antibody and store them in the global markerCache
	 */
	private void cacheMarkers() throws Exception {
		logger.info("caching markers");
		String cmd = "select distinct mta.antibody_key, m.symbol, m.primary_id as marker_id, "
			+ " s.by_symbol, m.marker_key "
			+ "from marker_to_antibody mta, marker m, marker_sequence_num s "
			+ "where mta.marker_key = m.marker_key "
			+ " and m.marker_key = s.marker_key "
			+ "order by s.by_symbol";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished marker query in " + ex.getTimestamp());

		int i = 0;
		markerCache = new HashMap<Integer,List<String>>();
		searchableMarkers = new HashMap<Integer,List<String>>();
		
		while (rs.next()) {
			Integer antibodyKey = rs.getInt("antibody_key");
			String markerID = rs.getString("marker_id");
			Integer markerKey = rs.getInt("marker_key");
			String symbol = rs.getString("symbol");
			i++;

			if (!markerCache.containsKey(antibodyKey)) {
				markerCache.put(antibodyKey, new ArrayList<String>());
			}
			markerCache.get(antibodyKey).add(symbol);
			
			if (!searchableMarkers.containsKey(antibodyKey)) {
				searchableMarkers.put(antibodyKey, new ArrayList<String>());
			}
			searchableMarkers.get(antibodyKey).add(markerID);
		}
		rs.close();
		logger.info("  - done caching " + i + " marker/antibody pairs");
		logger.info("  - found " + searchableMarkers.size() + " antibodies that can be retrieved by marker ID");
	}
	
	/* retrieve the marker symbols associated with the given antibody for display purposes.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<String> getMarkers(int antibodyKey) throws Exception {
		if (markerCache.containsKey(antibodyKey)) {
			return markerCache.get(antibodyKey);
		}
		return null;
	}

	/* retrieve the markers IDs that can be used to retrieve a given antibody.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<String> getSearchableMarkers(int antibodyKey) throws Exception {
		if (searchableMarkers.containsKey(antibodyKey)) {
			return searchableMarkers.get(antibodyKey);
		}
		return null;
	}
	
	/* cache the set of reference IDs that can be used to retrieve each antibody. (MGI, PubMed)
	 */
	private void cacheReferences() throws Exception {
		logger.info("caching references");
		searchableReferences = new HashMap<Integer,List<String>>();

		String cmd = "select a.antibody_key, r.acc_id "
			+ "from reference_id r, antibody_to_reference a "
			+ "where r.logical_db in ('PubMed', 'MGI') "
			+ " and r.reference_key = a.reference_key";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		
		int ct = 0;
		while (rs.next()) {
			ct++;
			Integer antibodyKey = rs.getInt("antibody_key");
			if (!searchableReferences.containsKey(antibodyKey)) {
				searchableReferences.put(antibodyKey, new ArrayList<String>());
			}
			searchableReferences.get(antibodyKey).add(rs.getString("acc_id"));
		}
		rs.close();
		logger.info(" - cached " + ct + " reference IDs for " + searchableReferences.size() + " antibodies");
	}
	
	/* retrieve the reference IDs that can be used to retrieve a given antibody.  Assumes cacheReferences()
	 * has been called.
	 */
	private List<String> getReferenceIDs(int antibodyKey) throws Exception {
		if (searchableReferences.containsKey(antibodyKey)) {
			return searchableReferences.get(antibodyKey);
		}
		return null;
	}
	
	/* retrieve antibodies from the database, build Solr docs, and write them to the server.
	 */
	private void processAntibodies() throws Exception {
		logger.info("loading antibodies");
		
		String cmd = "" +
			" with refCounts as (select antibody_key, count(*) as referenceCount from antibody_to_reference group by antibody_key) " +
			" select a.antibody_key, a.name, a.primary_id, a.antibody_type, a.host, c.referenceCount, " +
				"asn.by_name, asn.by_gene, asn.by_ref_count " +
				"from antibody a " +
				"left join refCounts c on a.antibody_key = c.antibody_key " +
				"left join antibody_sequence_num asn on a.antibody_key = asn.antibody_key ";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished main antibody query in " + ex.getTimestamp());

		int i = 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			Integer antibodyKey = rs.getInt("antibody_key");

			// start building the object that we will store as JSON in the antibody field
			// of the index
			AntibodyJ antibody = new AntibodyJ();
			antibody.setName(rs.getString("name"));
			antibody.setPrimaryID(rs.getString("primary_id"));
			antibody.setType(rs.getString("antibody_type"));
			antibody.setSpecies(rs.getString("host"));
			Integer refCount = rs.getInt("referenceCount");
			antibody.setReferenceCount(refCount == null ? 0 : refCount);
			

			// start building the solr document (will have only four fields total)
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.ANTIBODY_KEY, antibodyKey);
			doc.addField(IndexConstants.ANTIBODY_BY_NAME, rs.getInt("by_name"));
			doc.addField(IndexConstants.ANTIBODY_BY_GENE, rs.getInt("by_gene"));
			doc.addField(IndexConstants.ANTIBODY_BY_REF_COUNT, rs.getInt("by_ref_count"));
			
			// if antibody has associated markers, add them to the antibody object
			List<String> markers = getMarkers(antibodyKey);
			if (markers != null) {
				antibody.setMarkers(markers);
			}

			// and add the marker IDs to the searchable marker ID field in the index
			List<String> searchableMarkerIDs = getSearchableMarkers(antibodyKey);
			if (searchableMarkerIDs != null) {
				for (String markerID : searchableMarkerIDs) {
					doc.addField(IndexConstants.ANTIBODY_MARKER_ID, markerID);
				}
			}

			// add the reference IDs to the searchable reference ID field in the index
			List<String> searchableReferenceIDs = getReferenceIDs(antibodyKey);
			if (searchableReferenceIDs != null) {
				for (String referenceID : searchableReferenceIDs) {
					doc.addField(IndexConstants.ANTIBODY_REFERENCE_ID, referenceID);
				}
				refCount = searchableReferenceIDs.size();
			}

			doc.addField(IndexConstants.ANTIBODY_JSON, mapper.writeValueAsString(antibody));
			
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

		logger.info("done processing " + i + " molecular antibodies");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// collect various mappings needed for data lookup
		cacheMarkers();
		cacheReferences();
		processAntibodies();
	}
}
