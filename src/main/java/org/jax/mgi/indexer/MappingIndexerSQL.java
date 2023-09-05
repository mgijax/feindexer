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
import org.jax.mgi.shr.jsonmodel.MappingExperimentSummary;

/* Is: an indexer that builds the index supporting the genetic mapping experiment summary page (reachable from the
 * 		marker detail page and the reference summary/detail page).  Each document in the index represents
 * 		data for a single mapping experiment, and each experiment is represented in only one document.
 * Notes: The index is intended to support searches by a marker ID and reference ID.  Details of the
 * 		experiments are encapsulated in a JSON object in the experiment field.
 */
public class MappingIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Map<Integer,List<String>> markerIDs = null;		// searchable marker IDs per experiment key
	private Map<Integer,List<String>> referenceIDs = null;	// searchable reference IDs per experiment key
	private Map<Integer,List<String>> detailsCache = null;	// detail snippets per experiment key

	private ObjectMapper mapper = new ObjectMapper();				// converts objects to JSON

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public MappingIndexerSQL() {
		super("mapping");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* gather the extra "detail" strings from the database and cache them in the global detailsCache
	 */
	private void cacheDetails() throws Exception {
		logger.info("caching details");
		String cmd = "select experiment_key, 'Band: ' || band as detail "
			+ "from mapping_fish "
			+ "where band is not null and trim(band) != '' "

			+ "union "
			+ "select experiment_key, 'Band: ' || band "
			+ "from mapping_insitu "
			+ "where band is not null and trim(band) != '' "

			+ "union "
			+ "select experiment_key, 'RI/RC Set: ' || designation "
			+ "from mapping_rirc "
			+ "where designation is not null and trim(designation) != '' "
				
			+ "union "
			+ "select experiment_key, 'Cross Type: ' || regexp_replace(cross_type, ',.*', '') "
			+ "from mapping_cross "
			+ "where cross_type is not null and trim(cross_type) != '' "
				
			+ "union "
			+ "select experiment_key, 'Mapping Panel: ' || panel_name "
			+ "from mapping_cross "
			+ "where panel_name is not null and trim(panel_name) != '' "
			
			+ "order by 1, 2";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished experiment details query in " + ex.getTimestamp());

		int i = 0;
		detailsCache = new HashMap<Integer,List<String>>();

		SmartAlphaComparator smComparator = new SmartAlphaComparator();
		
		while (rs.next()) {
			i++;
			Integer experimentKey = rs.getInt("experiment_key");
			String detail = rs.getString("detail");
			if (detailsCache.containsKey(experimentKey)) {
				detailsCache.get(experimentKey).add(detail);
				Collections.sort(detailsCache.get(experimentKey), smComparator);
			} else {
				detailsCache.put(experimentKey, new ArrayList<String>());
				detailsCache.get(experimentKey).add(detail);
			}
		}
		rs.close();
		logger.info("  - done caching " + i + " details/experiment pairs");
	}
	
	/* retrieve the "detail" strings for the given experiment key.  Assumes cacheDetails()
	 * has been called.
	 */
	private List<String> getDetails(int experimentKey) throws Exception {
		if (detailsCache.containsKey(experimentKey)) {
			return detailsCache.get(experimentKey);
		}
		return null;
	}
	
	/* retrieve the marker IDs associated with each experiment and store them in the global markerIDs
	 */
	private void cacheMarkers() throws Exception {
		logger.info("caching marker IDs for searching");
		
		String cmd = "select distinct mtm.experiment_key, m.primary_id as marker_id "
			+ "from mapping_to_marker mtm, marker m "
			+ "where mtm.marker_key = m.marker_key ";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished marker ID query in " + ex.getTimestamp());

		int i = 0;
		markerIDs = new HashMap<Integer,List<String>>();
		
		while (rs.next()) {
			Integer experimentKey = rs.getInt("experiment_key");
			i++;

			if (!markerIDs.containsKey(experimentKey)) {
				markerIDs.put(experimentKey, new ArrayList<String>());
			}
			markerIDs.get(experimentKey).add(rs.getString("marker_id"));
		}
		rs.close();
		logger.info("  - done caching " + i + " marker/experiment pairs");
		logger.info("  - found " + markerIDs.size() + " experiments that can be retrieved by marker ID");
	}
	
	/* retrieve the marker IDs associated with the given experiment for searching.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<String> getMarkers(int experimentKey) throws Exception {
		if (markerIDs.containsKey(experimentKey)) {
			return markerIDs.get(experimentKey);
		}
		return null;
	}

	/* cache the set of reference IDs that can be used to retrieve each experiment. (MGI, PubMed)
	 */
	private void cacheReferences() throws Exception {
		logger.info("caching reference IDs");
		referenceIDs = new HashMap<Integer,List<String>>();

		String cmd = "select p.experiment_key, r.acc_id "
			+ "from reference_id r, mapping_experiment p "
			+ "where r.logical_db in ('PubMed', 'MGI') "
			+ " and r.reference_key = p.reference_key";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		
		int ct = 0;
		while (rs.next()) {
			ct++;
			Integer experimentKey = rs.getInt("experiment_key");
			if (!referenceIDs.containsKey(experimentKey)) {
				referenceIDs.put(experimentKey, new ArrayList<String>());
			}
			referenceIDs.get(experimentKey).add(rs.getString("acc_id"));
		}
		rs.close();
		logger.info(" - cached " + ct + " reference IDs for " + referenceIDs.size() + " experiments");
	}
	
	/* retrieve the reference IDs that can be used to retrieve a given experiment.  Assumes cacheReferences()
	 * has been called.
	 */
	private List<String> getReferenceIDs(int experimentKey) throws Exception {
		if (referenceIDs.containsKey(experimentKey)) {
			return referenceIDs.get(experimentKey);
		}
		return null;
	}
	
	/* retrieve mapping experiments from the database, build Solr docs, and write them to the server.
	 */
	private void processExperiments() throws Exception {
		logger.info("loading mapping experiments");
		
		String cmd = "select e.experiment_key, e.primary_id, r.jnum_id, r.short_citation, "
			+ "  e.experiment_type, e.chromosome, case "
			+ "  when e.chromosome in ('1', '2', '3', '4', '5', '6', '7', '8', '9') then '0' || e.chromosome "
			+ "  else e.chromosome end as sortable_chromosome "
			+ "from mapping_experiment e, reference r, reference_sequence_num n "
			+ "where e.reference_key = r.reference_key "
			+ "  and r.reference_key = n.reference_key "
			+ "order by e.experiment_type, n.by_author, r.short_citation, sortable_chromosome";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished main experiment query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for experiments
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			Integer experimentKey = rs.getInt("experiment_key");

			// start building the object that we will store as JSON in the experiment field
			// of the index
			MappingExperimentSummary experiment = new MappingExperimentSummary();
			experiment.setPrimaryID(rs.getString("primary_id"));
			experiment.setChromosome(rs.getString("chromosome"));
			experiment.setCitation(rs.getString("short_citation"));
			experiment.setJnumID(rs.getString("jnum_id"));
			experiment.setType(rs.getString("experiment_type"));
			experiment.setDetails(getDetails(experimentKey));
			
			// start building the solr document (will have only four fields total)
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.MLD_EXPERIMENT_KEY, experimentKey);
			doc.addField(IndexConstants.BY_DEFAULT, i);
			
			// and add the marker IDs to the searchable marker ID field in the index
			List<String> searchableMarkerIDs = getMarkers(experimentKey);
			if (searchableMarkerIDs != null) {
				for (String markerID : searchableMarkerIDs) {
					doc.addField(IndexConstants.MRK_ID, markerID);
				}
			}

			// add the reference IDs to the searchable reference ID field in the index
			List<String> searchableReferenceIDs = getReferenceIDs(experimentKey);
			if (searchableReferenceIDs != null) {
				for (String referenceID : searchableReferenceIDs) {
					doc.addField(IndexConstants.MLD_REFERENCE_ID, referenceID);
				}
			}

			doc.addField(IndexConstants.MLD_EXPERIMENT, mapper.writeValueAsString(experiment));
			
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

		logger.info("done processing " + i + " mapping experiments");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// collect various mappings needed for data lookup
		cacheDetails();
		cacheMarkers();
		cacheReferences();
		processExperiments();
	}
}
