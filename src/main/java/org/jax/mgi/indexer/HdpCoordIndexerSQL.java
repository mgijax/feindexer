package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/* Is: an indexer that builds the index used by the HMDC to serach for markers by genomic location (coordinates).
 * 		Each document in the index represents a single location for a single marker.  A given marker will have
 * 		a document for its own coordinates and can have one or more documents for its orthologs' coordinates
 * 		(one document for each ortholog).
 */
public class HdpCoordIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/


	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;					// number of records to retrieve at once
	protected int solrBatchSize = 10000;				// number of docs to send to solr in each batch

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	private long uniqueKey = 0;						// ascending counter of documents created

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public HdpCoordIndexerSQL() {
		super("diseasePortalCoords");
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
	private SolrInputDocument buildDoc(DocBuilder builder, String coordType, String chromosome,
			Integer startCoord, Integer endCoord) { 

		SolrInputDocument doc = builder.getNewDocument();
		doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);

		doc.addField(IndexConstants.START_COORD, startCoord);
		doc.addField(IndexConstants.END_COORD, endCoord);
		doc.addField(IndexConstants.CHROMOSOME, chromosome);
		doc.addField(IndexConstants.COORD_TYPE, coordType);
		return doc;
	}
	
	/* Add documents to the index for non-mouse markers in homology clusters. These IDs for non-mouse markers should also
	 * return a non-mouse marker line (in addition to the Homology class line), which will give symbol and name.
	 * For OMIM IDs, we should index both with and without the OMIM prefix.
	 */
	private void indexCoords() throws Exception {
		logger.info(" - indexing coordinates for markers");
		
		String cmd = "select m.marker_key, m.primary_id, m.organism,  " + 
				"  l.chromosome, l.start_coordinate, l.end_coordinate, 'marker' as coord_type " + 
				"from marker m, marker_location l " + 
				"where m.organism in ('human', 'mouse') " + 
				"  and m.marker_key = l.marker_key " + 
				"  and l.location_type = 'coordinates' " + 
				"  and m.status = 'official' " +
				"union  " + 
				"select m1.marker_key, m1.primary_id, m1.organism,  " + 
				"  loc.chromosome, loc.start_coordinate, loc.end_coordinate, 'ortholog' as coord_type " + 
				"from marker m1 " + 
				"inner join homology_cluster_organism_to_marker tm1 on (m1.marker_key = tm1.marker_key) " + 
				"inner join homology_cluster_organism o1 on (tm1.cluster_organism_key = o1.cluster_organism_key) " + 
				"inner join homology_cluster_organism o2 on (o1.cluster_key = o2.cluster_key) " + 
				"inner join homology_cluster_organism_to_marker tm2 on (o2.cluster_organism_key = tm2.cluster_organism_key) " + 
				"inner join marker m2 on (tm2.marker_key = m2.marker_key " + 
				"  and m2.organism in ('mouse', 'human') and m1.organism != m2.organism) " + 
				"inner join marker_location loc on (m2.marker_key = loc.marker_key and loc.location_type = 'coordinates') " + 
				"where m1.organism in ('human', 'mouse') " + 
				"order by 1, 7";
		
		Integer lastKey = null;
		DocBuilder marker = null;
		int markerCount = 0;
		int coordCount = 0;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		while (rs.next())  {  
			Integer markerKey = rs.getInt("marker_key");
			if ((lastKey == null) || (!lastKey.equals(markerKey))) {
				marker = new DocBuilder(rs.getString("organism"), markerKey, rs.getString("primary_id"));
				lastKey = markerKey;
				markerCount++;
			}

			addDoc(buildDoc(marker, rs.getString("coord_type"), rs.getString("chromosome"), 
				rs.getInt("start_coordinate"), rs.getInt("end_coordinate")));
			coordCount++;
		}

		rs.close();
		logger.info("indexed " + coordCount + " coordinates for " + markerCount + " markers");
	}

	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		logger.info("beginning other bucket");
		
		indexCoords();

		// any leftover docs to send to the server?  (likely yes)
		if (docs.size() > 0) { writeDocs(docs); }

		// commit all the changes to Solr
		commit(false);

		logger.info("finished other bucket");
	}

	/*---------------------*/
	/*--- private class ---*/
	/*---------------------*/

	// Private class for helping generate Solr documents.  Instantiate with the current object type being worked on.
	private class DocBuilder {
		private String organism;
		private Integer markerKey;
		private String markerID;
		
		private DocBuilder (String organism, Integer markerKey, String markerID) {
			this.organism = organism;
			this.markerKey = markerKey;
			this.markerID = markerID;
		}
		
		// compose and return a new SolrInputDocument including the fields for this feature
		public SolrInputDocument getNewDocument() {
			SolrInputDocument doc = new SolrInputDocument();

			if (this.organism != null) { doc.addField(IndexConstants.ORGANISM, this.organism); }
			if (this.markerKey != null) { doc.addField(IndexConstants.MRK_KEY, this.markerKey); }
			if (this.markerID != null) { doc.addField(IndexConstants.MRK_ID, this.markerID); }

			return doc;
		}
	}
}