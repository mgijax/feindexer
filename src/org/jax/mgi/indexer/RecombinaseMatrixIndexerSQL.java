package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

/**
 * RecombinaseMatrixIndexerSQL - generates Solr index for the recombinaseMatrix index, including the recombinase
 *		cells for the anatomy x expression/recombinase matrix.  (Index is structured to support serving up traditional
 *		expression cells, too, if we choose to add them in the future.)
 * Each document represents one recombinase cell in the matrix.  Traditional expression cells are built from data
 * 		retrieved from the existing gxdResult index.
 */
public class RecombinaseMatrixIndexerSQL extends Indexer {

	/***--- inner classes ---***/
	
	// Is: the contents of a cell in the anatomy x allele grid
	private class Cell {
		public int detected = 0;				// count of detected results
		public int notDetected = 0;				// count of not detected results
		public int ambiguous = 0;				// count of ambiguous results
		public int children = 0;				// results from children cells
		
		public void addDetected() {
			this.detected++;
		}
		
		public void addNotDetected() {
			this.notDetected++;
		}
		
		public void addAmbiguous() {
			this.ambiguous++;
		}
		
		public int allResults() {
			return this.detected + this.notDetected + this.ambiguous;
		}
		
		public int anyAmbiguous() {
			if (ambiguous > 0) { return 1; }
			return 0;
		}
		
		public void addChildren() {
			this.children++;
		}
	}

	// Is: a collection of Cells, for easy retrieval of a particular Cell given marker key, object type, object key, structure.
	// 		(object type, object key) allows us to store either wild-type expression data for the same marker or recombinase
	//		data for alleles driven by the marker.
	private class CellBlock {
		// cells[marker key][object type][object key][structure key] = Cell
		Map<Integer,Map<String,Map<Integer,Map<Integer,Cell>>>> cells = new HashMap<Integer,Map<String,Map<Integer,Map<Integer,Cell>>>>();

		public Cell getCell(int markerKey, String objectType, int objectKey, int structureKey) {
			if (!cells.containsKey(markerKey)) {
				cells.put(markerKey, new HashMap<String,Map<Integer,Map<Integer,Cell>>>());
			}
			if (!cells.get(markerKey).containsKey(objectType)) {
				cells.get(markerKey).put(objectType, new HashMap<Integer,Map<Integer,Cell>>());
			}
			if (!cells.get(markerKey).get(objectType).containsKey(objectKey)) {
				cells.get(markerKey).get(objectType).put(objectKey, new HashMap<Integer,Cell>());
			}
			if (!cells.get(markerKey).get(objectType).get(objectKey).containsKey(structureKey)) {
				cells.get(markerKey).get(objectType).get(objectKey).put(structureKey, new Cell());
			}
			return cells.get(markerKey).get(objectType).get(objectKey).get(structureKey);
		}

		public Set<Integer> getMarkerKeys() {
			return cells.keySet();
		}
		
		public Set<String> getObjectTypes(int markerKey) {
			return cells.get(markerKey).keySet();
		}

		public Set<Integer> getObjectKeys(int markerKey, String objectType) {
			return cells.get(markerKey).get(objectType).keySet();
		}

		public Set<Integer> getStructureKeys(int markerKey, String objectType, int objectKey) {
			return cells.get(markerKey).get(objectType).get(objectKey).keySet();
		}
	}

	// Is: a minimal set of column header data, for use in sorting
	private class SortableColumnHeader {
		public String objectType;
		public Integer objectKey;
		public String symbol;
		public String organism;
		public String objectSubtype;
		
		public SortableColumnHeader(String objectType, Integer objectKey, String symbol, String organism,
				String objectSubtype) {
			this.objectKey = objectKey;
			this.objectType = objectType;
			this.objectSubtype = objectSubtype;
			this.symbol = symbol;
			this.organism = organism;
		}
		
		public SortableColumnHeaderComparator getComparator() {
			return new SortableColumnHeaderComparator();
		}
		
		private int getSortableType() {
			if (MARKER.equals(this.objectType)) { return 0; }
			if (ALLELE.equals(this.objectType)) { return 1; }
			return 2;
		}
		
		private int getSortableOrganism() {
			if ("mouse".equals(this.organism)) { return 0; }
			if ("human".equals(this.organism)) { return 1; }
			if ("rat".equals(this.organism)) { return 2; }
			return 3;
		}
		
		private int getSortableSubtype() {
			if ("Targeted".equals(this.objectSubtype)) { return 0; }
			if ("Transgenic".equals(this.objectSubtype)) { return 1; }
			return 2;
		}
		
		public class SortableColumnHeaderComparator implements Comparator<SortableColumnHeader> {
			@Override
			public int compare(SortableColumnHeader a, SortableColumnHeader b) {
				// Sort rules:
				// 1. marker column before allele columns
				// 2. within alleles, mouse drivers before human before rat
				// 3. within those groups, targeted before transgenic
				// 4. within those subgroups, smart-alpha sort on symbol
				int out = Integer.compare(a.getSortableType(), b.getSortableType());
				if (out != 0) { return out; }
				
				out = Integer.compare(a.getSortableOrganism(), b.getSortableOrganism());
				if (out != 0) { return out; }
				
				out = Integer.compare(a.getSortableSubtype(), b.getSortableSubtype());
				if (out != 0) { return out; }
				
				out = smartAlphaComparator.compare(a.symbol, b.symbol);
				if (out != 0) { return out; }

				// should not happen:
				return Integer.compare(a.objectKey, b.objectKey);
			}
		}
	}
	
	/***--- class variables ---***/
	
	public static final String MARKER = "marker";	// object type for markers
	public static final String ALLELE = "allele";	// object type for alleles
  	private static SmartAlphaComparator smartAlphaComparator = new SmartAlphaComparator();
	
	/***--- instance variables ---***/
	
	public int batchSize = 25000;			// how many markers to process in each batch
	public int documentCacheSize = 10000;	// how many Solr docs to cache in memory
	
	// caches across all batches of markers (retrieve once and hold them)

	public Map<Integer,String> anatomyTerm;				// maps from anatomical structure key to structure term
	public Map<Integer,String> anatomyID;				// maps from anatomical structure key to structure term
	public Map<Integer,List<Integer>> anatomyAncestors;	// maps from anatomical structure key to ancestor term IDs
	public Map<Integer,List<String>> anatomyParents;	// maps from anatomical structure key to ancestor structure keys

	// caches of data for this batch of markers

	public Map<Integer,String> markerID;				// maps from marker key to marker ID
	public Map<Integer,String> markerSymbol;			// maps from marker key to marker symbol

	public Map<Integer,String> alleleSymbol;			// maps from allele key to allele symbol (without marker symbol)
	public Map<Integer,String> alleleID;				// maps from allele key to primary ID
	public Map<Integer,String> alleleType;				// maps from allele key to its type
	public Map<Integer,Integer> alleleSeqNum;			// maps from allele key to its sequence number
	public Map<Integer,String> driverOrganism;			// maps from allele key to its driver's organism

	/***--- methods ---***/
	
	// set which index we're going to populate
	public RecombinaseMatrixIndexerSQL () {
		super("recombinaseMatrix");
	}

	// populate the indexer's caches of anatomy term data (IDs, terms, parents, ancestors)
	public void buildAnatomyCaches() throws SQLException {
		this.anatomyTerm = new HashMap<Integer,String>(); 
		this.anatomyID = new HashMap<Integer,String>(); 
		this.anatomyAncestors = new HashMap<Integer,List<Integer>>(); 
		this.anatomyParents = new HashMap<Integer,List<String>>(); 
		
		// cache terms and IDs for anatomy terms
		
		String cmd = "select term_key, primary_id, term "
			+ "from term "
			+ "where vocab_name = 'EMAPA' ";
		
		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			anatomyTerm.put(termKey, rs.getString("term"));
			anatomyID.put(termKey, rs.getString("primary_id"));
		}
		rs.close();
		
		// cache IDs for parents of anatomy terms
		
		String cmd2 = "select tc.child_term_key, p.primary_id as parent_id " + 
				"from term t, term_child tc, term p " + 
				"where t.vocab_name = 'EMAPA' " + 
				" and t.term_key = tc.child_term_key " + 
				" and tc.term_key = p.term_key";

		ResultSet rs2 = ex.executeProto(cmd2);
		while (rs2.next()) {
			Integer termKey = rs2.getInt("child_term_key");
			if (!anatomyParents.containsKey(termKey)) {
				anatomyParents.put(termKey, new ArrayList<String>());
			}
			anatomyParents.get(termKey).add(rs2.getString("parent_id"));
		}
		rs2.close();
		
		// Cache structure keys for all ancestors of anatomy terms.
		
		String cmd3 = "select t.term_key, a.ancestor_term_key, a.ancestor_term, a.ancestor_primary_id " + 
				"from term t, term_ancestor a " + 
				"where t.vocab_name = 'EMAPA' " + 
				"and t.term_key = a.term_key";
		
		ResultSet rs3 = ex.executeProto(cmd3);
		while (rs3.next()) {
			Integer termKey = rs3.getInt("term_key");
			if (!anatomyAncestors.containsKey(termKey)) {
				anatomyAncestors.put(termKey, new ArrayList<Integer>());
			}
			anatomyAncestors.get(termKey).add(rs3.getInt("ancestor_term_key"));
		}
		rs3.close();
		logger.info(" - cached data for " + anatomyTerm.size() + " anatomy terms");
	}
	
	// populate the indexer's caches of marker data for markers with keys >= startMarker and < endMarker
	// (where those marker are also drivers of recombinase alleles)
	public void buildMarkerCaches(int startMarker, int endMarker) throws SQLException {
		this.markerID = new HashMap<Integer,String>(); 
		this.markerSymbol = new HashMap<Integer,String>(); 
		
		String cmd = "select distinct m.marker_key, m.symbol, m.primary_id "
			+ "from marker m, allele a "
			+ "where m.marker_key = a.driver_key "
			+ " and m.marker_key >= " + startMarker
			+ " and m.marker_key < " + endMarker;

		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			Integer markerKey = rs.getInt("marker_key");
			markerID.put(markerKey, rs.getString("primary_id"));
			markerSymbol.put(markerKey, rs.getString("symbol"));
		}
		rs.close();
		logger.info(" - cached data for " + markerID.size() + " markers");
	}
	
	// populate the indexer's caches of allele data for expression results with drivers
	// with keys >= startMarker and < endMarker
	public void buildAlleleCaches(int startMarker, int endMarker) throws SQLException {
		this.alleleSymbol = new HashMap<Integer,String>(); 
		this.alleleID = new HashMap<Integer,String>(); 
		this.alleleType = new HashMap<Integer,String>(); 
		this.alleleSeqNum = new HashMap<Integer,Integer>(); 
		this.driverOrganism = new HashMap<Integer,String>();
		List<SortableColumnHeader> alleleColumns = new ArrayList<SortableColumnHeader>();
		
		// get the allele pairs for each genocluster, but strip out all the markup and just leave
		// the allele symbols
		
		String cmd = "select distinct a.allele_key, a.only_allele_symbol as symbol, a.primary_id, a.allele_type, m.organism "
			+ "from allele a, recombinase_expression e, marker m "
			+ "where a.allele_key = e.allele_key "
			+ " and e.driver_key = m.marker_key"
			+ " and e.driver_key >= " + startMarker
			+ " and e.driver_key < " + endMarker;
		
		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			int alleleKey = rs.getInt("allele_key");
			String symbol = rs.getString("symbol");
			String alleleType = rs.getString("allele_type");
			String organism = rs.getString("organism");
			
			this.alleleSymbol.put(alleleKey, symbol);
			this.alleleID.put(alleleKey, rs.getString("primary_id"));
			this.alleleType.put(alleleKey, alleleType);
			this.driverOrganism.put(alleleKey, organism);
			
			alleleColumns.add(new SortableColumnHeader(ALLELE, alleleKey, symbol, organism, alleleType));
		}
		rs.close();
		
		if (alleleColumns.size() > 0) {
			Collections.sort(alleleColumns, alleleColumns.get(0).getComparator());
			int seqNum = 1;
			for (SortableColumnHeader sgc : alleleColumns) {
				this.alleleSeqNum.put(sgc.objectKey, seqNum++);
			}
		}
		logger.info(" - cached data for " + this.alleleID.size() + " alleles");
	}

	// only would want to expand a cell to see children if there are non-absent expression results
	private void updateCell (Cell cell, String isExpressed, boolean isAncestor) {
		if ("Yes".equals(isExpressed)) {
			cell.addDetected();
			if (isAncestor) { cell.addChildren(); }
		} else if ("No".equals(isExpressed)) {
			cell.addNotDetected();
		} else {
			cell.addAmbiguous();
			if (isAncestor) { cell.addChildren(); }
		}
	}
	
	// main method for the indexer
	public void index() throws Exception
	{
		// We're going to step through markers as we build this index, so get the min and max marker keys
		// that are drivers.
		
		logger.info("Finding min/max marker keys");
		String markerCmd = "select min(driver_key) as min_key, max(driver_key) as max_key from allele";
		ResultSet rsMrk = ex.executeProto(markerCmd);
		if (!rsMrk.next()) {
			throw new Exception("Cannot find marker keys");
		}
		int minMarkerKey = rsMrk.getInt("min_key");
		int maxMarkerKey = rsMrk.getInt("max_key") + 1;		// 1 more to make sure we get the last one
		rsMrk.close();
		logger.info(" - found driver keys " + minMarkerKey + " to " + (maxMarkerKey - 1));

		// step through markers, populate caches, build documents, etc.
		
		int startMarker = minMarkerKey;
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		int uniqueKey = 1;
		buildAnatomyCaches();
				
		while (startMarker < maxMarkerKey) {
			int endMarker = startMarker + this.batchSize;
			logger.info("Processing drivers " + startMarker + " to " + endMarker);
			
			// Build up the memory caches for data related to this batch of markers.
			// (We can bail out early if there are no markers with data.)
			
			buildMarkerCaches(startMarker, endMarker);
			if (this.markerID.size() == 0) {
				logger.info(" - no markers with data; moving on");
				startMarker = endMarker;
				continue;
			}
			buildAlleleCaches(startMarker, endMarker);
			
			/* Initially we are just populating this index with data for the recombinase alleles. A similar
			 * process, however, could be used to also add the wild-type expression data for markers.
			 */
			String cmd = "select allele_key, driver_key, result_key, structure_key, is_detected "
				+ "from recombinase_expression "
				+ "where driver_key >= " + startMarker
				+ " and driver_key < " + endMarker;

			ResultSet rs = ex.executeProto(cmd);
			
			// retrieves cells (creating them when needed) for a given (driver, object type, object, structure) tuple
			CellBlock cellBlock = new CellBlock();
			
			while (rs.next()) {
				int alleleKey = rs.getInt("allele_key");
				int driverKey = rs.getInt("driver_key");
				int structureKey = rs.getInt("structure_key");
				String isDetected = rs.getString("is_detected");
				
				// Otherwise, update data for the corresponding cell.
				Cell cell = cellBlock.getCell(driverKey, ALLELE, alleleKey, structureKey);
				updateCell(cell, isDetected, false);
				
				// And also add the annotation to any of its ancestor cells.  Each annotation is only
				// counted once in each ancestor cell, regardless of how many paths there are to the root.
				if (this.anatomyAncestors.containsKey(structureKey)) {
					for (Integer ancestorKey : this.anatomyAncestors.get(structureKey)) {
						Cell ancestorCell = cellBlock.getCell(driverKey, ALLELE, alleleKey, ancestorKey);
						updateCell(ancestorCell, isDetected, true);
					}
				}
			}

			rs.close();
			logger.info(" - collated data into cells");

			// Then go through the data for each marker, build a Solr document for each cell, and send
			// them to the server in batches.
			
			for (Integer driverKey : cellBlock.getMarkerKeys()) {
				String driverID = this.markerID.get(driverKey);
				
				for (Integer alleleKey : cellBlock.getObjectKeys(driverKey, ALLELE)) {
					for (Integer structureKey : cellBlock.getStructureKeys(driverKey, ALLELE, alleleKey)) {
						String emapaTerm = this.anatomyTerm.get(structureKey);
						String emapaID = this.anatomyID.get(structureKey);
						if (emapaID == null) {
							logger.info("ID null for structure " + structureKey);
						} else if (emapaID.trim().equals("")) {
							logger.info("ID empty string for structure " + structureKey);
						}
						List<String> emapaParents = this.anatomyParents.get(structureKey);
						List<Integer> emapaAncestors = this.anatomyAncestors.get(structureKey);
						Cell cell = cellBlock.getCell(driverKey, ALLELE, alleleKey, structureKey);

						SolrInputDocument doc = new SolrInputDocument();
						doc.addField(IndexConstants.CELL_TYPE, "recombinase");
						doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
						doc.addField(IndexConstants.PARENT_ANATOMY_ID, emapaParents);
						doc.addField(IndexConstants.ANCESTOR_ANATOMY_KEY, emapaAncestors);
						doc.addField(IndexConstants.ANATOMY_TERM, emapaTerm);
						doc.addField(IndexConstants.ANATOMY_ID, emapaID);
						doc.addField(IndexConstants.MRK_ID, driverID);
						doc.addField(IndexConstants.SYMBOL, this.alleleSymbol.get(alleleKey));
						doc.addField(IndexConstants.ORGANISM, this.driverOrganism.get(alleleKey));
						doc.addField(IndexConstants.COLUMN_ID, this.alleleID.get(alleleKey));
						doc.addField(IndexConstants.ALL_RESULTS, cell.allResults());
						doc.addField(IndexConstants.DETECTED_RESULTS, cell.detected);
						doc.addField(IndexConstants.NOT_DETECTED_RESULTS, cell.notDetected);
						doc.addField(IndexConstants.ANY_AMBIGUOUS, cell.anyAmbiguous());
						doc.addField(IndexConstants.CHILDREN, cell.children);
						doc.addField(IndexConstants.BY_COLUMN, this.alleleSeqNum.get(alleleKey));

						docs.add(doc);
						if (docs.size() > this.documentCacheSize) {
							writeDocs(docs);
							docs = new ArrayList<SolrInputDocument>();
						}
					}
				}
			}
			startMarker = endMarker;
		}
		writeDocs(docs);
		logger.info("Finished writing " + uniqueKey + " docs to Solr");
		commit();
	}
}
