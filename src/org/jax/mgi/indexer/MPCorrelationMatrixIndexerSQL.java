package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

/**
 * MPCorrelationMatrixIndexerSQL - generates Solr index for the mpCorrelationMatrix index, including the phenotype
 *		cells for the anatomy x expression/phenotype matrix.
 * Each document represents one phenotype cell in the matrix.  Expression cells are built from data retrieved
 * 		from the existing gxdResult index.
 */
public class MPCorrelationMatrixIndexerSQL extends Indexer {

	/***--- inner classes ---***/
	
	// Is: a log of which annotations we've already seen and handled (to ensure distinctness)
	private class AnnotationLog {
		// handled[marker key][genotype key][mp term key][structure key][qualifier] = set of reference keys
		private	Map<Integer,Map<Integer,Map<Integer,Map<Integer,Map<String,Set<Integer>>>>>> handled = 
				new HashMap<Integer,Map<Integer,Map<Integer,Map<Integer,Map<String,Set<Integer>>>>>>();
		
		public boolean seenIt (int markerKey, int genotypeKey, int mpTermKey, int structureKey, String qualifier, int referenceKey) {
			if (!handled.containsKey(markerKey)) {
				handled.put(markerKey, new HashMap<Integer,Map<Integer,Map<Integer,Map<String,Set<Integer>>>>>());
			}
			if (!handled.get(markerKey).containsKey(genotypeKey)) {
				handled.get(markerKey).put(genotypeKey, new HashMap<Integer,Map<Integer,Map<String,Set<Integer>>>>());
			}
			if (!handled.get(markerKey).get(genotypeKey).containsKey(mpTermKey)) {
				handled.get(markerKey).get(genotypeKey).put(mpTermKey, new HashMap<Integer,Map<String,Set<Integer>>>());
			}
			if (!handled.get(markerKey).get(genotypeKey).get(mpTermKey).containsKey(structureKey)) {
				handled.get(markerKey).get(genotypeKey).get(mpTermKey).put(structureKey, new HashMap<String,Set<Integer>>());
			}
			if (!handled.get(markerKey).get(genotypeKey).get(mpTermKey).get(structureKey).containsKey(qualifier)) {
				handled.get(markerKey).get(genotypeKey).get(mpTermKey).get(structureKey).put(qualifier, new HashSet<Integer>());
			}
			if (!handled.get(markerKey).get(genotypeKey).get(mpTermKey).get(structureKey).get(qualifier).contains(referenceKey)) {
				handled.get(markerKey).get(genotypeKey).get(mpTermKey).get(structureKey).get(qualifier).add(referenceKey);
				return false;
			}
			return true;
		}
	}
	
	// Is: the contents of a cell in the anatomy x genocluster grid
	private class Cell {
		public int abnormals = 0;				// count of non-normal annotations
		public int normals = 0;					// count of normal annotations
		public int backgroundSensitive = 0;		// background sensitive (1) or not (0)?
		public int children = 0;				// count of non-normal annotations in children
		
		public void addChildren() {
			this.children++;
		}
		
		public void addAbnormal() {
			this.abnormals++;
		}
		
		public void addNormal() {
			this.normals++;
		}
		
		public void applySensitivity(int backgroundSensitive) {
			if (this.backgroundSensitive == 0 && backgroundSensitive == 1) {
				this.backgroundSensitive = 1;
			}
		}

		public int isOnlyNormal() {
			if (this.normals > 0 && this.abnormals == 0) {
				return 1;
			}
			return 0;
		}
	}

	// Is: a collection of Cells, for easy retrieval of a particular Cell given marker, genocluster, structure
	private class CellBlock {
		// cells[marker key][genocluster key][structure key] = Cell
		Map<Integer,Map<Integer,Map<Integer,Cell>>> cells = new HashMap<Integer,Map<Integer,Map<Integer,Cell>>>();

		public Cell getCell(int markerKey, int genoclusterKey, int structureKey) {
			if (!cells.containsKey(markerKey)) {
				cells.put(markerKey, new HashMap<Integer,Map<Integer,Cell>>());
			}
			if (!cells.get(markerKey).containsKey(genoclusterKey)) {
				cells.get(markerKey).put(genoclusterKey, new HashMap<Integer,Cell>());
			}
			if (!cells.get(markerKey).get(genoclusterKey).containsKey(structureKey)) {
				cells.get(markerKey).get(genoclusterKey).put(structureKey, new Cell());
			}
			return cells.get(markerKey).get(genoclusterKey).get(structureKey);
		}

		public Set<Integer> getMarkerKeys() {
			return cells.keySet();
		}
		
		public Set<Integer> getGenoclusterKeys(int markerKey) {
			return cells.get(markerKey).keySet();
		}

		public Set<Integer> getStructureKeys(int markerKey, int genoclusterKey) {
			return cells.get(markerKey).get(genoclusterKey).keySet();
		}
	}
	
	// Is: a minimal set of genocluster data, for use in sorting
	private class SortableGenocluster {
		public Integer genoclusterKey;
		public String allelePairs;
		public String genotypeType;
		
		public SortableGenocluster(Integer genoclusterKey, String allelePairs, String genotypeType) {
			this.genoclusterKey = genoclusterKey;
			this.allelePairs = allelePairs;
			this.genotypeType = genotypeType;
		}
		
		public SortableGenoclusterComparator getComparator() {
			return new SortableGenoclusterComparator();
		}
		
		private int getSortableType() {
			if ("hm".equals(genotypeType)) { return 0; }
			if ("ht".equals(genotypeType)) { return 1; }
			if ("cn".equals(genotypeType)) { return 2; }
			if ("cx".equals(genotypeType)) { return 3; }
			if ("tg".equals(genotypeType)) { return 4; }
			if ("ot".equals(genotypeType)) { return 5; }
			return 6;
		}
		
		public class SortableGenoclusterComparator implements Comparator<SortableGenocluster> {
			@Override
			public int compare(SortableGenocluster a, SortableGenocluster b) {
				/* Sort first by preference of genotype types, the smart-alpha on the allele pairs.
				 * Lastly, fall back on genocluster key (shouldn't happen).
				 */
				int out = Integer.compare(a.getSortableType(), b.getSortableType());
				if (out != 0) { return out; }
				
				out = smartAlphaComparator.compare(a.allelePairs, b.allelePairs);
				if (out != 0) { return out; }

				return Integer.compare(a.genoclusterKey, b.genoclusterKey);
			}
		}
	}
	
	/***--- class variables ---***/
	
	public static final String NORMAL = "normal";	// text of the normal qualifier
	private static SmartAlphaComparator smartAlphaComparator = new SmartAlphaComparator();
		
	
	/***--- instance variables ---***/
	
	public int batchSize = 10000;			// how many markers to process in each batch
	public int documentCacheSize = 10000;	// how many Solr docs to cache in memory
	
	// caches across all batches of markers (retrieve once and hold them)

	public Map<Integer,String> anatomyTerm;				// maps from anatomical structure key to structure term
	public Map<Integer,String> anatomyID;				// maps from anatomical structure key to structure term
	public Map<Integer,List<Integer>> anatomyAncestors;	// maps from anatomical structure key to ancestor term IDs
	public Map<Integer,List<String>> anatomyParents;	// maps from anatomical structure key to ancestor structure keys

	// caches of data for this batch of markers

	public Map<Integer,String> allelePairs;				// maps from genocluster key to allele pair string
	public Map<Integer,Integer> genoclusterSeqNum;		// maps from genocluster key to sequence number
	public Map<Integer,String> markerID;				// maps from marker key to marker ID
	public Map<Integer,String> markerSymbol;			// maps from marker key to marker symbol

	/***--- methods ---***/
	
	// set which index we're going to populate
	public MPCorrelationMatrixIndexerSQL () {
		super("mpCorrelationMatrix");
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
	public void buildMarkerCaches(int startMarker, int endMarker) throws SQLException {
		this.markerID = new HashMap<Integer,String>(); 
		this.markerSymbol = new HashMap<Integer,String>(); 
		
		String cmd = "select distinct m.marker_key, m.symbol, m.primary_id "
			+ "from marker m, hdp_genocluster gc "
			+ "where gc.marker_key = m.marker_key "
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
	
	// populate the indexer's caches of genocluster data for annotations rolled up to
	// markers with keys >= startMarker and < endMarker
	public void buildGenoclusterCaches(int startMarker, int endMarker) throws SQLException {
		this.allelePairs = new HashMap<Integer,String>(); 
		this.genoclusterSeqNum = new HashMap<Integer,Integer>();
		List<SortableGenocluster> genoclusters = new ArrayList<SortableGenocluster>();
		
		// get the allele pairs for each genocluster, but strip out all the markup and just leave
		// the allele symbols
		
		String cmd = "select distinct gc.hdp_genocluster_key, g.genotype_type, "
			+ " regexp_replace("
			+ "  regexp_replace(g.combination_1, '\\\\Allele\\([^\\|]*|', '', 'g'), "
			+ "   '\\|\\)?', '', 'g') as allele_pairs "
			+ "from marker m, hdp_genocluster gc, hdp_genocluster_genotype gg, genotype g "
			+ "where gc.marker_key = m.marker_key "
			+ " and gc.hdp_genocluster_key = gg.hdp_genocluster_key "
			+ " and gg.genotype_key = g.genotype_key "
			+ " and m.marker_key >= " + startMarker
			+ " and m.marker_key < " + endMarker;

		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			Integer genoclusterKey = rs.getInt("hdp_genocluster_key");
			String alleles = rs.getString("allele_pairs").trim();
			
			allelePairs.put(genoclusterKey, alleles);
			genoclusters.add(new SortableGenocluster(genoclusterKey, alleles, rs.getString("genotype_type")));
		}
		rs.close();
		
		if (genoclusters.size() > 0) {
			Collections.sort(genoclusters, genoclusters.get(0).getComparator());
			int seqNum = 0;
			for (SortableGenocluster sgc : genoclusters) {
				genoclusterSeqNum.put(sgc.genoclusterKey, seqNum++);
			}
		}
		logger.info(" - cached alleles for " + allelePairs.size() + " genoclusters");
	}
	
	private void updateCell (Cell cell, String qualifier, Integer backgroundSensitive, boolean isAncestor) {
		if (NORMAL.equals(qualifier)) {
			cell.addNormal();
		} else {
			cell.addAbnormal();
			if (isAncestor) {
				cell.addChildren();
			}
		}
		cell.applySensitivity(backgroundSensitive);
	}
	
	// main method for the indexer
	public void index() throws Exception
	{
		// We're going to step through markers as we build this index, so get the min and max marker keys
		// that have genoclusters associated.
		
		logger.info("Finding min/max marker keys");
		String markerCmd = "select min(marker_key) as min_key, max(marker_key) as max_key from hdp_genocluster";
		ResultSet rsMrk = ex.executeProto(markerCmd);
		if (!rsMrk.next()) {
			throw new Exception("Cannot find marker keys");
		}
		int minMarkerKey = rsMrk.getInt("min_key");
		int maxMarkerKey = rsMrk.getInt("max_key") + 1;		// 1 more to make sure we get the last one
		rsMrk.close();
		logger.info(" - found keys " + minMarkerKey + " to " + (maxMarkerKey - 1));

		// step through markers, populate caches, build documents, etc.
		
		int startMarker = minMarkerKey;
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		int uniqueKey = 1;
		buildAnatomyCaches();
				
		while (startMarker < maxMarkerKey) {
			int endMarker = startMarker + this.batchSize;
			logger.info("Processing markers " + startMarker + " to " + endMarker);
			
			// Build up the memory caches for data related to this batch of markers.
			// (We can bail out early if there are no markers with data.)
			
			buildMarkerCaches(startMarker, endMarker);
			if (this.markerID.size() == 0) {
				logger.info(" - no markers with data; moving on");
				startMarker = endMarker;
				continue;
			}
			buildGenoclusterCaches(startMarker, endMarker);
			
			/* For the purposes of this index, we define a unique annotation as a combination of
			 *		(marker key, genotype key, term key, qualifier, reference_key)
			 * However, the HMDC tables do not track references for annotations, so we need to 
			 * find the original, pre-rollup annotations at the genotype level and look at their
			 * references.
			 */
			String cmd = "select distinct g.marker_key, hga.hdp_genocluster_key, geno.genotype_key, "
				+ " map.term_key_2 as structure_key, hga.qualifier_type, hga.has_backgroundnote, "
				+ " r.reference_key, hga.term_key "
				+ "from hdp_genocluster g, hdp_genocluster_annotation hga, term_to_term map, "
				+ " hdp_genocluster_genotype geno, genotype_to_annotation gta, annotation a, "
				+ " annotation_reference r "
				+ "where g.marker_key >= " + startMarker
				+ " and g.marker_key < " + endMarker
				+ " and g.hdp_genocluster_key = geno.hdp_genocluster_key "
				+ " and geno.genotype_key = gta.genotype_key "
				+ " and gta.annotation_key = a.annotation_key "
				+ " and a.term_key = hga.term_key "
				+ " and hga.term != 'no phenotypic analysis' "
				+ " and ((hga.qualifier_type is null and a.qualifier is null) or (hga.qualifier_type = a.qualifier)) "
				+ " and gta.annotation_key = r.annotation_key "
				+ " and g.hdp_genocluster_key = hga.hdp_genocluster_key "
				+ " and hga.term_key = map.term_key_1 "
				+ " and map.relationship_type = 'MP to EMAPA' "
				+ "order by g.marker_key";

			ResultSet rs = ex.executeProto(cmd);
			
			// keeps track of which annotations we've already processed, to eliminate duplication
			AnnotationLog annotLog = new AnnotationLog();
			
			// retrieves cells (creating them when needed) for a given (marker, genocluster, structure) triple
			CellBlock cellBlock = new CellBlock();
			
			while (rs.next()) {
				int markerKey = rs.getInt("marker_key");
				int genoclusterKey = rs.getInt("hdp_genocluster_key");
				int genotypeKey = rs.getInt("genotype_key");
				int structureKey = rs.getInt("structure_key");
				String qualifier = rs.getString("qualifier_type");
				int backgroundSensitive = rs.getInt("has_backgroundnote");
				int referenceKey = rs.getInt("reference_key");
				int mpTermKey = rs.getInt("term_key");
				
				// If we've already processed an annotation matching this one, skip it and move on.
				if (annotLog.seenIt(markerKey, genotypeKey, mpTermKey, structureKey, qualifier, referenceKey)) {
					continue;
				}

				// Otherwise, update data for the corresponding cell.
				Cell cell = cellBlock.getCell(markerKey, genoclusterKey, structureKey);
				updateCell(cell, qualifier, backgroundSensitive, false);
				
				// And also add the annotation to any of its ancestor cells.  Each annotation is only
				// counted once in each ancestor cell, regardless of how many paths there are to the root.
				if (this.anatomyAncestors.containsKey(structureKey)) {
					for (Integer ancestorKey : this.anatomyAncestors.get(structureKey)) {
						Cell ancestorCell = cellBlock.getCell(markerKey, genoclusterKey, ancestorKey);
						updateCell(ancestorCell, qualifier, backgroundSensitive, true);
					}
				}
			}

			rs.close();
			logger.info(" - collated data into cells");

			// Then go through the data for each marker, build a Solr document for each cell, and send
			// them to the server in batches.
			
			for (Integer markerKey : cellBlock.getMarkerKeys()) {
				String geneSymbol = this.markerSymbol.get(markerKey);
				String geneID = this.markerID.get(markerKey);
				
				for (Integer genoclusterKey : cellBlock.getGenoclusterKeys(markerKey)) {
					String alleles = this.allelePairs.get(genoclusterKey);

					for (Integer structureKey : cellBlock.getStructureKeys(markerKey, genoclusterKey)) {
						String emapaTerm = this.anatomyTerm.get(structureKey);
						String emapaID = this.anatomyID.get(structureKey);
						List<String> emapaParents = this.anatomyParents.get(structureKey);
						List<Integer> emapaAncestors = this.anatomyAncestors.get(structureKey);
						Cell cell = cellBlock.getCell(markerKey, genoclusterKey, structureKey);

						SolrInputDocument doc = new SolrInputDocument();
						doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
						doc.addField(IndexConstants.PARENT_ANATOMY_ID, emapaParents);
						doc.addField(IndexConstants.ANCESTOR_ANATOMY_KEY, emapaAncestors);
						doc.addField(IndexConstants.ANATOMY_TERM, emapaTerm);
						doc.addField(IndexConstants.ANATOMY_ID, emapaID);
						doc.addField(IndexConstants.MRK_ID, geneID);
						doc.addField(IndexConstants.MRK_SYMBOL, geneSymbol);
						doc.addField(IndexConstants.GENOCLUSTER_KEY, genoclusterKey);
						doc.addField(IndexConstants.ALLELE_PAIRS, alleles);
						doc.addField(IndexConstants.ANNOTATION_COUNT, cell.normals + cell.abnormals);
						doc.addField(IndexConstants.IS_NORMAL, cell.isOnlyNormal());
						doc.addField(IndexConstants.CHILDREN, cell.children);
						doc.addField(IndexConstants.HAS_BACKGROUND_SENSITIVITY, cell.backgroundSensitive);
						if (this.genoclusterSeqNum.containsKey(genoclusterKey)) {
							doc.addField(IndexConstants.BY_GENOCLUSTER, genoclusterSeqNum.get(genoclusterKey));
						} else {
							doc.addField(IndexConstants.BY_GENOCLUSTER, 0);
						}
						
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
