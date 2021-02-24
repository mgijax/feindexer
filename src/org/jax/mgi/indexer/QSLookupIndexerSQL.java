package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/* Is: an indexer that builds the index supporting the quick search's lookups for marker and allele locations.
 * 		Each document in the index represents the location display data for a given marker or allele (identified by ID).
 */
public class QSLookupIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	// For these three Maps:
	//  1. For alleles with markers, look up using the marker ID.
	//  2. For alleles with no markers, look up using the allele ID.
	public static Map<String,String> chromosomeCache = new HashMap<String,String>();	// object ID : chromosome
	public static Map<String,String> locationCache = new HashMap<String,String>();		// object ID : location for display
	public static Map<String,String> strandCache = new HashMap<String,String>();		// object ID : strand
	
	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch
	
	private NumberFormat nf = new DecimalFormat("#");		// no decimal places (coordinates)
	private NumberFormat cmf = new DecimalFormat("#.##");	// two decimal places (cM location)

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSLookupIndexerSQL() {
		super("qsLookup");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	// Compose a SolrInputDocument with the following fields and send it toward the index.
	private void addDoc(String accID, String symbol, String name, String chromosome, String location, String strand) {
		SolrInputDocument doc = new SolrInputDocument();

		if (accID != null) { doc.addField(IndexConstants.QS_PRIMARY_ID, accID); }
		if (symbol != null) { doc.addField(IndexConstants.QS_SYMBOL, symbol); }
		if (name != null) { doc.addField(IndexConstants.QS_NAME, name); }
		if (chromosome != null) { doc.addField(IndexConstants.QS_CHROMOSOME, chromosome); }
		if (location != null) { doc.addField(IndexConstants.QS_LOCATION, location); }
		if (strand != null) { doc.addField(IndexConstants.QS_STRAND, strand); }
		
		docs.add(doc);
		if (docs.size() >= solrBatchSize)  {
			writeDocs(docs);
			docs = new ArrayList<SolrInputDocument>();
		}
	}

	// Compose and return the location string based on the input parameters.  Default is syntenic.
	private String getLocation (String locationType, Double startCoord, Double endCoord, Float cmOffset, String band) {
		String location = "syntenic";
		
		if ("coordinates".equals(locationType)) {
			if ((startCoord != null) && (endCoord != null)) {
				return nf.format(startCoord) + "-" + nf.format(endCoord);
			}

		} else if ("centimorgans".equals(locationType)) {
			if ((cmOffset != null) && (cmOffset >= 0.0)) {
				location = cmf.format(cmOffset) + " cM";
			}
				
		} else {	// cytogenetic band
			if (band != null) {
				location = "cytoband " + band;
			}
		}
		return location;
	}
	
	// Look up marker data (ID, symbol, name, location data) and add it to the index.  Also caches location data
	// for use by alleles.  Of note, markers can have multiple types of locations (coordinates, centimorgans,
	// or cytogenetic).  A sequenceNum field gives order of preference for each marker; so looking for all
	// records with sequenceNum = 1 will yield the preferred location for each marker.
	private void indexMarkers() throws Exception {
		// Get the preferred location for each current mouse marker.
		String cmd = "select m.primary_ID, m.symbol, m.name, ml.location_type, ml.chromosome, ml.cm_offset, " +
			" ml.cytogenetic_offset, ml.start_coordinate, ml.end_coordinate, ml.strand " + 
			"from marker_location ml, marker m " + 
			"where ml.marker_key = m.marker_key " + 
			" and m.organism = 'mouse' " + 
			" and m.status != 'withdrawn' " + 
			" and ml.sequence_num = 1";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished marker query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		while (rs.next())  {  
			String markerID = rs.getString("primary_ID");
			String chromosome = rs.getString("chromosome");
			String strand = rs.getString("strand");
			
			String location = getLocation(rs.getString("location_type"), rs.getDouble("start_coordinate"),
				rs.getDouble("end_coordinate"), rs.getFloat("cm_offset"), rs.getString("cytogenetic_offset"));
			
			chromosomeCache.put(markerID, chromosome);
			locationCache.put(markerID, location);
			strandCache.put(markerID, strand);
			
			addDoc(markerID, rs.getString("symbol"), rs.getString("name"), chromosome, location, strand);
			i++;
		}
		
		rs.close();

		logger.info("done with basic data for " + i + " markers");
	}

	// For those alleles that have no markers and have a sequence with only one good hit, update the caches
	// with those locations.
	private void cacheAlleleSequenceLocations() throws Exception {
		String cmd = "select a.primary_id, sl.chromosome, sl.start_coordinate, sl.end_coordinate, sl.strand " + 
			"from allele a, allele_to_sequence t, sequence_gene_trap gt, sequence_location sl " + 
			"where a.allele_key = t.allele_key " + 
			"and t.sequence_key = gt.sequence_key " + 
			"and gt.good_hit_count = 1 " + 
			"and t.sequence_key = sl.sequence_key " + 
			"and sl.location_type = 'coordinates' " + 
			"and sl.chromosome is not null " + 
			"and not exists (select 1 from marker_to_allele m where a.allele_key = m.allele_key)";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished allele/sequence query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		while (rs.next())  {  
			String alleleID = rs.getString("primary_ID");
			String chromosome = rs.getString("chromosome");
			String strand = rs.getString("strand");
			
			String location = getLocation("coordinates", rs.getDouble("start_coordinate"),
				rs.getDouble("end_coordinate"), null, null);
			
			chromosomeCache.put(alleleID, chromosome);
			locationCache.put(alleleID, location);
			strandCache.put(alleleID, strand);
			i++;
		}
		
		rs.close();

		logger.info("done caching " + i + " allele/sequence locations");
	}

	// Look up allele data (ID, symbol, name, location data) and add it to the index. An allele can draw its
	// location data from its marker (if it has one) or from the allele's sequence (if it has only one good hit).
	private void indexAlleles() throws Exception {
		cacheAlleleSequenceLocations();
		
		// Get basic allele info.
		String cmd = "select a.primary_id, a.symbol, a.name, m.name as marker_name, m.primary_id as marker_id " + 
			"from allele a " + 
			"left outer join marker_to_allele mta on (a.allele_key = mta.allele_key) " + 
			"left outer join marker m on (mta.marker_key = m.marker_key) " + 
			"where a.is_wild_type = 0";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished allele query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		while (rs.next())  {  
			String alleleID = rs.getString("primary_id");
			String markerID = rs.getString("marker_id");
			String alleleName = rs.getString("name");
			String markerName = rs.getString("marker_name");
			
			if ((markerName != null) && (markerName.length() > 0)) {
				alleleName = markerName + "; " + alleleName;
			}

			String lookupID = null;
			if (locationCache.containsKey(markerID)) {
				lookupID = markerID;
			} else if (locationCache.containsKey(alleleID)) {
				lookupID = alleleID;
			}
			
			addDoc(alleleID, rs.getString("symbol"), alleleName, chromosomeCache.get(lookupID), locationCache.get(lookupID),
				strandCache.get(lookupID));
			i++;
		}
		
		rs.close();

		logger.info("done with basic data for " + i + " alleles");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		indexMarkers();
		indexAlleles();

		// send any remaining documents and commit all the changes to Solr
		if (docs.size() > 0) {
			writeDocs(docs);
		}
		commit();
	}
}