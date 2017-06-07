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
import org.jax.mgi.shr.jsonmodel.SimpleSequence;
import org.jax.mgi.shr.jsonmodel.AccessionID;
import org.jax.mgi.shr.jsonmodel.GenomicLocation;
import org.jax.mgi.shr.jsonmodel.SimpleMarker;

/* Is: an indexer that builds the index supporting the sequence summary page (reachable from the
 * 		marker and reference detail pages).  Each document in the index represents data for a single
 * 		sequence, and each sequence is represented in only one document.
 * Notes: The index is intended to support searches by a marker ID or a reference ID, with other fields
 * 		used for filtering.  Details of each sequence are encapsulated in a JSON object in the sequence field.
 */
public class SequenceIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch
	private int sequenceChunkSize = 225000;			// number of sequence keys to handle on each iteration
	
	private List<String> emptyList = new ArrayList<String>();	// shared empty string for simpler code
	
	private Map<Integer,List<SimpleMarker>> markers;		// sequence key to list of basic marker data
	private Map<Integer,List<String>> markerKeys;			// sequence key to list of marker keys
	private Map<Integer,List<String>> referenceKeys;		// sequence key to list of reference keys
	private Map<Integer,List<String>> collections;			// sequence key to list of clone collection names
	private Map<Integer,List<String>> strains;				// sequence key to list of strain names
	private Map<Integer,Integer> sequenceNum;				// sequence key to sequence num for ordering
	private Map<Integer,GenomicLocation> locations;			// sequence key to primary genomic location
	private Map<Integer,List<AccessionID>> otherIDs;		// sequence key to list of non-preferred IDs
	
	private ObjectMapper mapper = new ObjectMapper();			// converts objects to JSON

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public SequenceIndexerSQL() {
		super("sequence");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* get the maximum sequence key we need to process
	 */
	private int getMaxSequenceKey() throws Exception {
		logger.info("getting max sequence key");
		int maxKey = 1;			// default; should always be overridden

		String cmd = "select max(sequence_key) as max_key from sequence"; 
		ResultSet rs = ex.executeProto(cmd);

		if (rs.next()) {
			maxKey = rs.getInt("max_key");
		} 
		rs.close();

		logger.info(" - max sequence key = " + maxKey);
		return maxKey;
	}
	
	/* gather ordering data for sequences and assign a single sequence number that can be used to order
	 * any subset of sequences correctly according to our rules of precedence:  sequence type, then
	 * sequence provider, then desdending length
	 */
	private void cacheSequenceNum() throws Exception {
		logger.info("computing ordering of sequences");
		
		/* use the database to order the sequences by the rules (above) and just return the sequence keys
		 * for the sake of time & space efficiency.  Note: We could add a clustered index to this table
		 * to boost performance of this query, if needed.
		 */
		String cmd = "select sequence_key "
			+ "from sequence_sequence_num "
			+ "order by by_sequence_type, by_provider, by_length";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished sequence ordering query in " + ex.getTimestamp());

		sequenceNum = new HashMap<Integer,Integer>();
		
		int seqNum = 1;
		while (rs.next()) {
			sequenceNum.put(rs.getInt("sequence_key"), seqNum++);
		}
		rs.close();
		logger.info("  - done computing ordering for " + sequenceNum.size() + " sequences");
	}
	
	/* retrieve the pre-computed sequence number for the specified sequence; assumes cacheSequenceNum has been called.
	 */
	private int getSequenceNum(int sequenceKey) {
		if (sequenceNum.containsKey(sequenceKey)) {
			return sequenceNum.get(sequenceKey);
		}
		return 0;
	}
	
	/* gather genomic locations for each sequence and arbitrarily choose one for each (in case of multiples)
	 */
	private void cacheLocations(int startKey, int endKey) throws Exception {
		logger.debug("caching locations");
		String cmd = "select sequence_key, chromosome, start_coordinate, end_coordinate, strand "
			+ "from sequence_location "
			+ "where sequence_key >= " + startKey
			+ "  and sequence_key < " + endKey
			+ "  and sequence_num = 1";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished location query in " + ex.getTimestamp());

		locations = new HashMap<Integer,GenomicLocation>();
		
		while (rs.next()) {
			locations.put(rs.getInt("sequence_key"), new GenomicLocation(rs.getString("chromosome"),
				rs.getString("start_coordinate"), rs.getString("end_coordinate"), rs.getString("strand")) );
		}
		rs.close();
		logger.debug("  - done locations for " + collections.size() + " sequences");
	}
	
	/* retrieve the location for the given sequence key.  Assumes cacheLocation() has been called.
	 */
	private GenomicLocation getLocation(int sequenceKey) throws Exception {
		if (locations.containsKey(sequenceKey)) {
			return locations.get(sequenceKey);
		}
		return null;
	}
	
	/* gather the clone collections from the database and cache them in a global cache (collections)
	 */
	private void cacheCollections(int startKey, int endKey) throws Exception {
		logger.debug("caching collections");
		String cmd = "select sequence_key, collection "
			+ "from sequence_clone_collection "
			+ "where sequence_key >= " + startKey
			+ "  and sequence_key < " + endKey;

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished clone collection query in " + ex.getTimestamp());

		collections = new HashMap<Integer,List<String>>();
		SmartAlphaComparator smComparator = new SmartAlphaComparator();
		
		while (rs.next()) {
			Integer sequenceKey = rs.getInt("sequence_key");
			String collection = rs.getString("collection");
			if (collections.containsKey(sequenceKey)) {
				collections.get(sequenceKey).add(collection);
				Collections.sort(collections.get(sequenceKey), smComparator);
			} else {
				collections.put(sequenceKey, new ArrayList<String>());
				collections.get(sequenceKey).add(collection);
			}
		}
		rs.close();
		logger.debug("  - done caching collections for " + collections.size() + " sequences");
	}
	
	/* retrieve the clone collections for the given sequence key.  Assumes cacheCollections()
	 * has been called.
	 */
	private List<String> getCollections(int sequenceKey) throws Exception {
		if (collections.containsKey(sequenceKey)) {
			return collections.get(sequenceKey);
		}
		return null;
	}
	
	/* gather the mouse strains from the database and cache them in a global cache (strains)
	 */
	private void cacheStrains(int startKey, int endKey) throws Exception {
		logger.debug("caching strains");
		String cmd = "select s.sequence_key, o.strain "
			+ "from sequence s, sequence_source o "
			+ "where s.sequence_key >= " + startKey
			+ "  and s.sequence_key < " + endKey
			+ "  and s.sequence_key = o.sequence_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished strains query in " + ex.getTimestamp());

		strains = new HashMap<Integer,List<String>>();
		SmartAlphaComparator smComparator = new SmartAlphaComparator();
		
		while (rs.next()) {
			Integer sequenceKey = rs.getInt("sequence_key");
			String strain = rs.getString("strain");
			if (strains.containsKey(sequenceKey)) {
				strains.get(sequenceKey).add(strain);
				Collections.sort(strains.get(sequenceKey), smComparator);
			} else {
				strains.put(sequenceKey, new ArrayList<String>());
				strains.get(sequenceKey).add(strain);
			}
		}
		rs.close();
		logger.debug("  - done caching strains for " + strains.size() + " sequences");
	}
	
	/* retrieve the strains for the given sequence key.  Assumes cacheStrains() has been called.
	 */
	private List<String> getStrains(int sequenceKey) throws Exception {
		if (strains.containsKey(sequenceKey)) {
			return strains.get(sequenceKey);
		}
		return emptyList;
	}
	
	/* retrieve the references associated with each sequence and store their keys in a cache
	 */
	private void cacheReferences(int startKey, int endKey) throws Exception {
		logger.debug("caching references");
		String cmd = "select sequence_key, reference_key "
			+ "from reference_to_sequence "
			+ "where sequence_key >= " + startKey
			+ "  and sequence_key < " + endKey;

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished reference query in " + ex.getTimestamp());

		referenceKeys = new HashMap<Integer,List<String>>();
		
		while (rs.next()) {
			Integer sequenceKey = rs.getInt("sequence_key");
			
			// any markers associated with the sequence go into the cache of markers for display
			if (!referenceKeys.containsKey(sequenceKey)) {
				referenceKeys.put(sequenceKey, new ArrayList<String>());
			}
			referenceKeys.get(sequenceKey).add(rs.getString("reference_key"));
		}
		rs.close();
		logger.debug("  - found reference keys for " + markers.size() + " sequences");
	}

	/* retrieve the reference keys that can be used to retrieve a given sequence.  Assumes cacheReferences() has
	 * been called.
	 */
	private List<String> getReferenceKeys(int sequenceKey) throws Exception {
		if (referenceKeys.containsKey(sequenceKey)) {
			return referenceKeys.get(sequenceKey);
		}
		return emptyList;
	}
	
	/* retrieve the other (non-preferred) IDs associated with each sequence
	 */
	private void cacheIDs(int startKey, int endKey) throws Exception {
		logger.debug("caching other IDs");
		String cmd = "select i.sequence_key, i.acc_id, i.logical_db "
			+ "from sequence_id i, sequence s "
			+ "where i.sequence_key = s.sequence_key "
			+ "  and (i.acc_id != s.primary_id or i.logical_db != s.logical_db) "
			+ "  and i.private = 0 "
			+ "  and i.sequence_key >= " + startKey
			+ "  and i.sequence_key < " + endKey;

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished other ID query in " + ex.getTimestamp());

		otherIDs = new HashMap<Integer,List<AccessionID>>();
		
		while (rs.next()) {
			Integer sequenceKey = rs.getInt("sequence_key");
			
			// any IDs associated with the sequence go into the cache of IDs for display
			if (!otherIDs.containsKey(sequenceKey)) {
				otherIDs.put(sequenceKey, new ArrayList<AccessionID>());
			}
			otherIDs.get(sequenceKey).add(new AccessionID(rs.getString("acc_id"), rs.getString("logical_db")));
		}
		rs.close();
		logger.debug("  - found other IDs keys for " + markers.size() + " sequences");
	}

	/* retrieve the other (non-preferred) accession IDs for the given sequence.  Assumes cacheIDs() has
	 * been called.
	 */
	private List<AccessionID> getOtherIDs(int sequenceKey) throws Exception {
		if (otherIDs.containsKey(sequenceKey)) {
			return otherIDs.get(sequenceKey);
		}
		return null;
	}
	
	/* retrieve the markers associated with each sequence and store them in the cache of markers and 
	 * in the cache of marker keys
	 */
	private void cacheMarkers(int startKey, int endKey) throws Exception {
		logger.debug("caching markers");
		String cmd = "select s.sequence_key, m.symbol, m.primary_id, n.by_symbol, s.marker_key "
			+ "from marker_to_sequence s, marker m, marker_sequence_num n "
			+ "where s.sequence_key >= " + startKey
			+ "  and s.sequence_key < " + endKey
			+ "  and s.marker_key = m.marker_key "
			+ "  and s.marker_key = n.marker_key "
			+ "order by 4";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished marker query in " + ex.getTimestamp());

		markers = new HashMap<Integer,List<SimpleMarker>>();
		markerKeys = new HashMap<Integer,List<String>>();
		
		while (rs.next()) {
			Integer sequenceKey = rs.getInt("sequence_key");
			SimpleMarker marker = new SimpleMarker(rs.getString("symbol"), rs.getString("primary_id"));
			
			// any markers associated with the sequence go into the cache of markers for display
			if (!markers.containsKey(sequenceKey)) {
				markers.put(sequenceKey, new ArrayList<SimpleMarker>());
				markerKeys.put(sequenceKey, new ArrayList<String>());
			}
			markers.get(sequenceKey).add(marker);
			markerKeys.get(sequenceKey).add(rs.getString("marker_key"));
		}
		rs.close();
		logger.debug("  - found markers and keys for " + markers.size() + " sequences");
	}
	
	/* retrieve the markers associated with the given sequence.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<SimpleMarker> getMarkers(int sequenceKey) throws Exception {
		if (markers.containsKey(sequenceKey)) {
			return markers.get(sequenceKey);
		}
		return null;
	}

	/* retrieve the markers keys that can be used to retrieve a given sequence.  Assumes cacheMarkers() has
	 * been called.
	 */
	private List<String> getMarkerKeys(int sequenceKey) throws Exception {
		if (markerKeys.containsKey(sequenceKey)) {
			return markerKeys.get(sequenceKey);
		}
		return emptyList;
	}
	
	/* get the provider name by which we can search by to find a given sequence, as some of them
	 * require customizations.
	 */
	private String getSearchableProvider(String provider) {
		if ("SWISS-PROT".equals(provider) || "TrEMBL".equals(provider)) {
			return "UniProt";
		}
		return provider;
	}
	/* retrieve sequences from the database, build Solr docs, and write them to the server.
	 */
	private void processSequences(int startKey, int endKey) throws Exception {
		logger.debug("loading sequences");
		
		String cmd = "select s.sequence_key, s.sequence_type, s.provider, s.length, s.description, s.primary_id, "
			+ "  s.organism, i.acc_id as genbank_id "
			+ "from sequence s "
			+ "left outer join sequence_id i on (s.sequence_key = i.sequence_key "
			+ "  and i.logical_db = 'Sequence DB' "
			+ "  and i.preferred = 1) "
			+ "where s.sequence_key >= " + startKey
			+ "  and s.sequence_key < " + endKey;

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished main sequence query in " + ex.getTimestamp());

		int i = 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			Integer sequenceKey = rs.getInt("sequence_key");

			// start building the object that we will store as JSON in the sequence field of the index
			SimpleSequence seq = new SimpleSequence(sequenceKey, rs.getString("primary_id"), rs.getString("provider"),
				rs.getString("sequence_type"), rs.getString("length"), rs.getString("organism"),
				rs.getString("description"));

			// collect and assign items from memory caches
			
			GenomicLocation location = getLocation(sequenceKey);
			if (location != null) {
				seq.setLocation(location);
			}

			String genbankID = rs.getString("genbank_id");
			if (genbankID != null) {
				seq.setPreferredGenbankID(genbankID);
			}

			List<SimpleMarker> myMarkers = this.getMarkers(sequenceKey);
			if (myMarkers != null) {
				seq.setMarkers(myMarkers);
			}
			
			List<String> myCollections = this.getCollections(sequenceKey);
			if (myCollections != null) {
				seq.setCloneCollections(myCollections);
			}
			
			List<AccessionID> myOtherIDs = this.getOtherIDs(sequenceKey);
			if (myOtherIDs != null) {
				seq.setOtherIDs(myOtherIDs);
			}

			// start building the solr document
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.SEQ_KEY, sequenceKey);
			doc.addField(IndexConstants.BY_DEFAULT, this.getSequenceNum(sequenceKey));
			doc.addField(IndexConstants.SEQ_PROVIDER, getSearchableProvider(seq.getProvider()));
			doc.addField(IndexConstants.SEQ_TYPE, seq.getSequenceType());
			
			for (String markerKey : this.getMarkerKeys(sequenceKey)) {
				doc.addField(IndexConstants.MRK_KEY, markerKey);
			}
			
			for (String referenceKey : this.getReferenceKeys(sequenceKey)) {
				doc.addField(IndexConstants.REF_KEY, referenceKey);
			}
			
			for (String strain : this.getStrains(sequenceKey)) {
				doc.addField(IndexConstants.SEQ_STRAIN, strain);
				seq.setStrain(strain);
			}

			doc.addField(IndexConstants.SEQ_SEQUENCE, mapper.writeValueAsString(seq));
			
			// Add this doc to the batch we're collecting.  If the stack hits our
			// threshold, send it to the server and reset it.
			docs.add(doc);
			if (docs.size() >= solrBatchSize)  {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}
		rs.close();

		// any leftover docs to send to the server?  (likely yes)
		writeDocs(docs);
		logger.debug("done processing " + i + " sequences");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		int maxSeqKey = getMaxSequenceKey();
		int startKeyInclusive = 1;
		int endKeyExclusive = startKeyInclusive + sequenceChunkSize;
		
		logger.info("Preparing for " + (1 + (maxSeqKey / sequenceChunkSize)) + " batches");

		/* need to cache the sequence numbers for all the sequences, so we can get the proper
		 * order for the whole set, rather than just ordering the batches (retrieved below) internally.
		 */
		cacheSequenceNum();
		
		/* walk through the full set of sequences in blocks of 'sequenceChunkSize' to help keep
		 * memory requirements down.  (In-memory caches are flushed and rebuilt for each chunk.)
		 */
		int batch = 1;
		while (startKeyInclusive < maxSeqKey) {
			// refresh the caches for this slice of sequences
			cacheMarkers(startKeyInclusive, endKeyExclusive);
			cacheReferences(startKeyInclusive, endKeyExclusive);
			cacheCollections(startKeyInclusive, endKeyExclusive);
			cacheStrains(startKeyInclusive, endKeyExclusive);
			cacheIDs(startKeyInclusive, endKeyExclusive);
			cacheLocations(startKeyInclusive, endKeyExclusive);
			
			// caches are filled, so now process this slice of sequences
			processSequences(startKeyInclusive, endKeyExclusive); 
			
			// get ready for the next slice
			startKeyInclusive = endKeyExclusive;
			endKeyExclusive = endKeyExclusive + sequenceChunkSize;
			logger.info("finished batch " + batch++ + " (sequences through key " + (endKeyExclusive - 1) + ")");
		}
		commit();
	}
}
