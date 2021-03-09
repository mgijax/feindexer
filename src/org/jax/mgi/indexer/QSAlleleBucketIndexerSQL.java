package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.VocabTerm;
import org.jax.mgi.shr.VocabTermCache;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.util.EasyStemmer;
import org.jax.mgi.shr.fe.util.StopwordRemover;

/* Is: an indexer that builds the index supporting the quick search's allele bucket (aka- bucket 5).
 * 		Each document in the index represents a single searchable data element (e.g.- symbol, name, synonym, annotated
 * 		term, etc.) for an allele.
 */
public class QSAlleleBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	private static String MARKER = "marker";	// used to indicate we are currently working with markers
	private static String ALLELE = "allele";	// used to indicate we are currently working with alleles

	// weights to prioritize different types of search terms / IDs
	private static int PRIMARY_ID_WEIGHT = 100;
	private static int SECONDARY_ID_WEIGHT = 97;
	private static int SYMBOL_WEIGHT = 95;
	private static int NAME_WEIGHT = 90;
	private static int MARKER_SYMBOL_WEIGHT = 87;
	private static int SYNONYM_WEIGHT = 85;
	private static int MARKER_SYNONYM_WEIGHT = 80;
	private static int TRANSGENE_PART_WEIGHT = 77;
	private static int DISEASE_ID_WEIGHT = 75;
	private static int DISEASE_NAME_WEIGHT = 70;
	private static int DISEASE_SYNONYM_WEIGHT = 67;
	private static int MP_ID_WEIGHT = 60;
	private static int MP_NAME_WEIGHT = 57;
	private static int MP_SYNONYM_WEIGHT = 55;
	
	public static Map<Integer, QSAllele> alleles;			// allele key : QSFeature object

	public static Map<Integer,String> chromosome;			// allele key : chromosome
	public static Map<Integer,String> startCoord;			// allele key : start coordinate
	public static Map<Integer,String> endCoord;				// allele key : end coordinate
	public static Map<Integer,String> strand;				// allele key : strand
	
	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

	private long uniqueKey = 0;						// ascending counter of documents created
	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch
	protected int uncommittedBatchLimit = 1000;		// number of batches to allow before doing a Solr commit
	private int uncommittedBatches = 0;				// number of batches sent to Solr and not yet committed

	private VocabTermCache diseaseOntologyCache;	// cache of data for DO DAG
	private VocabTermCache mpOntologyCache;			// cache of data for MP DAG

	private Map<Integer,Set<Integer>> highLevelTerms;		// maps from a term key to the keys of its high-level ancestors
	
	private EasyStemmer stemmer = new EasyStemmer();
	private StopwordRemover stopwordRemover = new StopwordRemover();
	
	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSAlleleBucketIndexerSQL() {
		super("qsAlleleBucket");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	// To minimize the number of documents created for a given feature, we assume that all queries are ordered
	// by allele, and we keep track of each featureID seen and all the terms indexed for it.  Then
	// in addDoc() we check that we haven't already indexed the current term for the current ID.  This should
	//	prevent duplicates within each data type with one point of change (except for query ordering). 
	Map<String,Set<String>> indexedTerms = new HashMap<String,Set<String>>();
	
	// Reset the cache of indexed terms.
	private void clearIndexedTermCache() {
		logger.info("Clearing cache for " + indexedTerms.size() + " alleles");
		indexedTerms = new HashMap<String,Set<String>>();
	}

	// Add this doc to the batch we're collecting.  If the stack hits our threshold, send it to the server and reset it.
	private void addDoc(SolrInputDocument doc) {
		// See comments above definition of indexedTerms for explanation of logic.
		
		String primaryID = (String) doc.getFieldValue(IndexConstants.QS_PRIMARY_ID);
		if (!indexedTerms.containsKey(primaryID)) {
			indexedTerms.put(primaryID, new HashSet<String>());
		}
		
		String term = null;
		
		if (doc.containsKey(IndexConstants.QS_SEARCH_TERM_EXACT)) {
			term = ((String) doc.getFieldValue(IndexConstants.QS_SEARCH_TERM_EXACT)).toLowerCase();
		}
		if ((term == null) && (doc.containsKey(IndexConstants.QS_SEARCH_TERM_STEMMED))) {
			term = ((String) doc.getFieldValue(IndexConstants.QS_SEARCH_TERM_STEMMED)).toLowerCase();
		}
		
		if ((term != null) && !indexedTerms.get(primaryID).contains(term)) {
			docs.add(doc);
			if (docs.size() >= solrBatchSize)  {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
				uncommittedBatches++;
				if (uncommittedBatches >= uncommittedBatchLimit) {
					commit();
					uncommittedBatches = 0;
				}
			}
			indexedTerms.get(primaryID).add(term);
		}
	}
	
	// Build and return a new SolrInputDocument with the given fields filled in.
	private SolrInputDocument buildDoc(QSAllele allele, String exactTerm, String inexactTerm, String stemmedTerm,
			String searchTermDisplay, String searchTermType, Integer searchTermWeight) {

		SolrInputDocument doc = allele.getNewDocument();
		if (exactTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM_EXACT, exactTerm); }
		if (inexactTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM_INEXACT, inexactTerm); }
		if (stemmedTerm != null) {
			doc.addField(IndexConstants.QS_SEARCH_TERM_STEMMED, stemmer.stemAll(stopwordRemover.remove(stemmedTerm)));
		}
		doc.addField(IndexConstants.QS_SEARCH_TERM_DISPLAY, searchTermDisplay);
		doc.addField(IndexConstants.QS_SEARCH_TERM_TYPE, searchTermType);
		doc.addField(IndexConstants.QS_SEARCH_TERM_WEIGHT, searchTermWeight);
		doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
		return doc;
	}
	
	// For each vocabulary term, we need to know what its high-level ancestors (aka- slim terms) are, so
	// look them up and cache them.
	private void cacheHighLevelTerms() throws SQLException {
		this.highLevelTerms = new HashMap<Integer,Set<Integer>>();
		
		String cmd = "select term_key, header_term_key from term_to_header"; 

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next() ) {
			Integer termKey = rs.getInt("term_key");
			if (!highLevelTerms.containsKey(termKey)) {
				this.highLevelTerms.put(termKey, new HashSet<Integer>());
			}
			this.highLevelTerms.get(termKey).add(rs.getInt("header_term_key"));
		}
		rs.close();
		
		logger.info("Cached ancestors of " + this.highLevelTerms.size() + " terms");
	}
	
	/* Cache the chromosomes, coordinates, and strands for alleles.
	 * NOTE: For efficiency's sake, these are not currently used in the fewi.  Instead, it retrieves them
	 * from the database and caches them.  If we allow searching by coordinates, these will be useful.
	 */
	private void cacheLocations() throws Exception {
		chromosome = new HashMap<Integer,String>();
		startCoord = new HashMap<Integer,String>();
		endCoord = new HashMap<Integer,String>();
		strand = new HashMap<Integer,String>();
	
		List<String> cmds = new ArrayList<String>();
		
		// primarily pick up coordinates from the allele's marker
		cmds.add("select a.allele_key as allele_key, coord.chromosome, coord.start_coordinate, coord.end_coordinate, "
			+ " coord.strand, cm.chromosome as cm_chromosome, cyto.chromosome as cyto_chromosome " + 
			"from allele a " +
			"inner join marker_to_allele m on (a.allele_key = m.allele_key) " + 
			"left outer join marker_location coord on (m.marker_key = coord.marker_key "
			+ " and coord.location_type = 'coordinates') " + 
			"left outer join marker_location cyto on (m.marker_key = cyto.marker_key "
			+ " and cyto.location_type = 'cytogenetic') " +
			"left outer join marker_location cm on (m.marker_key = cm.marker_key "
			+ " and cm.location_type = 'centimorgans')" +
			"where a.is_wild_type = 0");
			
		// secondarily pick up coordinates from an allele's sequence (where only one good hit count)
		cmds.add("select a.allele_key as allele_key, sl.chromosome, sl.start_coordinate, sl.end_coordinate, " +
				" sl.strand, null as cyto_chromosome, null as cm_chromosome " + 
				"from allele a, allele_to_sequence t, sequence_gene_trap gt, sequence_location sl " + 
				"where a.allele_key = t.allele_key " + 
				"and t.sequence_key = gt.sequence_key " + 
				"and gt.good_hit_count = 1 " + 
				"and t.sequence_key = sl.sequence_key " + 
				"and sl.location_type = 'coordinates' " + 
				"and sl.chromosome is not null " + 
				"and not exists (select 1 from marker_to_allele m where a.allele_key = m.allele_key)");
		
		for (String cmd : cmds) { 
			ResultSet rs = ex.executeProto(cmd, cursorLimit);

			while (rs.next()) {
				Integer alleleKey = rs.getInt("allele_key");
				String chrom = rs.getString("chromosome");

				if (chrom == null) {
					chrom = rs.getString("cm_chromosome");
					if (chrom == null) {
						chrom = rs.getString("cyto_chromosome");
					}
				}

				if (!chromosome.containsKey(alleleKey)) { chromosome.put(alleleKey, chrom); }
				if (!startCoord.containsKey(alleleKey)) { startCoord.put(alleleKey, rs.getString("start_coordinate")); }
				if (!endCoord.containsKey(alleleKey)) { endCoord.put(alleleKey, rs.getString("end_coordinate")); }
				if (!strand.containsKey(alleleKey)) { strand.put(alleleKey, rs.getString("strand")); }
			}
			rs.close();
		}

		logger.info(" - cached " + chromosome.size() + " locations for " + chromosome.size() + " alleles");
	}
	
	// Load accession IDs for the alleles and create Solr documents for them.
	private void indexIDs() throws Exception {
		logger.info(" - indexing IDs for alleles");
		if ((alleles == null) || (alleles.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }

		String cmd;
		
		cmd = "select i.allele_key as allele_key, i.acc_id, i.logical_db, 'Allele' as prefix " + 
				"from allele_id i, allele a " + 
				"where i.private = 0 " +
				"and i.allele_key = a.allele_key " +
				"and a.primary_id != i.acc_id " +
				"and a.is_wild_type = 0 " +
				"union " +
				"select allele_key as allele_key, primary_id as acc_id, logical_db, 'Cell Line' as prefix " + 
				"from allele_cell_line " + 
				"where primary_id is not null " + 
				"and logical_db is not null " + 
				"order by 1";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer alleleKey = rs.getInt("allele_key");
			String id = rs.getString("acc_id");
			String logicalDB = rs.getString("logical_db");
			String prefix = rs.getString("prefix");
			String suffix = "";
			
			if ("MGI".contentEquals(logicalDB)) {
				logicalDB = prefix + "ID";
			} else if ("Cell Line".equals(prefix)){
				suffix = " (" + logicalDB + ")";
				logicalDB = prefix + " ID";
			} else {
				logicalDB = logicalDB + " ID";
			}

			if (alleles.containsKey(alleleKey)) {
				QSAllele feature = alleles.get(alleleKey);
				addDoc(buildDoc(feature, id, null, null, id + suffix, logicalDB, SECONDARY_ID_WEIGHT));
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " IDs for alleles");
	}
	
	// Index mouse DO (Disease Ontology) annotations.
	private void indexMouseDiseaseAnnotations () throws Exception {
		String cmd = null;
		
		// Note that we do not consider the roll-up rules or expressed components.  We only consider
		// disease annotations to mouse genotypes.
		cmd = "select distinct m.allele_key as allele_key, dm.disease_id  " + 
			"from allele m, allele_to_genotype mtg, genotype t, disease_model dm " + 
			"where m.allele_key = mtg.allele_key  " + 
			"and mtg.genotype_key = t.genotype_key  " + 
			"and t.is_disease_model = 1  " + 
			"and t.genotype_key = dm.genotype_key  " + 
			"and dm.is_not_model = 0  " + 
			"order by 1";

		logger.info(" - indexing mouse disease annotations for alleles");

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of annotations processed
		while (rs.next()) {
			ct++;
			Integer alleleKey = rs.getInt("allele_key");
			VocabTerm vt = diseaseOntologyCache.getTerm(rs.getString("disease_id"));
			
			if (alleles.containsKey(alleleKey) && (vt != null)) {
				QSAllele feature = alleles.get(alleleKey);
				String term = vt.getTerm();
				
				// For each annotation, we need to index:
				// 1. term name, primary ID, secondary IDs, and synonyms for that term.
				// 2. And for each of its ancestors, we also need to index:
				//    a. term name, primary ID, secondary IDs, and synonyms.

				addDoc(buildDoc(feature, null, null, term, term, "Disease Model", DISEASE_NAME_WEIGHT));

				if (vt.getAllIDs() != null) {
					for (String accID : vt.getAllIDs()) {
						addDoc(buildDoc(feature, accID, null, null, term + " (" + accID + ")", "Disease Model", DISEASE_ID_WEIGHT));
					}
				}
				
				if (vt.getSynonyms() != null) {
					for (String synonym : vt.getSynonyms()) {
						addDoc(buildDoc(feature, null, null, synonym, term + " (synonym: " + synonym +")", "Disease Model", DISEASE_SYNONYM_WEIGHT));
					}
				}
				
				// ancestors of this vocab term
				for (Integer ancestorKey : vt.getAncestorKeys()) {
					VocabTerm ancestor = diseaseOntologyCache.getTerm(ancestorKey);
					String ancTerm = ancestor.getTerm();
					
					addDoc(buildDoc(feature, null, null, ancTerm, term + " (subterm of " + ancTerm + ")", "Disease Model", DISEASE_NAME_WEIGHT));

					if (ancestor.getAllIDs() != null) {
						for (String accID : ancestor.getAllIDs()) {
							addDoc(buildDoc(feature, accID, null, null, term + " (subterm of " + ancestor.getTerm() + ", with ID "+ accID + ")", "Disease Model", DISEASE_ID_WEIGHT));
						}
					}
				
					if (ancestor.getSynonyms() != null) {
						for (String synonym : ancestor.getSynonyms()) {
							addDoc(buildDoc(feature, null, null, synonym, term + " (subterm of " + ancestor.getTerm() + ", with synonym " + synonym +")", "Disease Model", DISEASE_SYNONYM_WEIGHT));
						}
					}
				}
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " mouse disease annotations for alleles");
	}

	/* Cache and return all synonyms for alleles, populating the synonyms object.
	 * Note that this caching method is different from the others in that it does not directly modify the object's
	 * cache.  Instead, the map produced is returned.  This lets us also easily cache marker synonyms when dealing
	 * with alleles.
	 */
	private Map<Integer,List<String>> cacheSynonyms(String featureType) throws Exception {
		logger.info(" - caching synonyms for " + featureType);
		
		HashMap<Integer,List<String>> mySynonyms = new HashMap<Integer,List<String>>();
		
		String cmd;
		if (MARKER.equals(featureType)) {
			// Do not index synonyms for transgene markers, as their Tg alleles already get done.
			cmd = "select s.marker_key as allele_key, s.synonym " + 
					"from marker_synonym s, marker m " +
					"where s.marker_key = m.marker_key " +
					"and m.marker_subtype != 'transgene' " +
					"order by s.marker_key";
		} else {
			cmd = "select s.allele_key as allele_key, s.synonym " + 
					"from allele_synonym s, allele a " +
					"where s.allele_key = a.allele_key " +
					"and a.is_wild_type = 0 " +
					"order by s.allele_key";
		}
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of synonyms processed
		while (rs.next()) {
			ct++;

			Integer alleleKey = rs.getInt("allele_key");
			String synonym = rs.getString("synonym");
			
			if (!mySynonyms.containsKey(alleleKey)) {
				mySynonyms.put(alleleKey, new ArrayList<String>());
			}
			mySynonyms.get(alleleKey).add(synonym);
		}
		rs.close();

		logger.info(" - cached " + ct + " synonyms for " + mySynonyms.size() + " " + featureType + "s");
		return mySynonyms;
	}

	// Retrieve synonyms, create documents, and index them.  Do both marker synonyms and allele synonyms.
	private void indexSynonyms() throws Exception {
		if ((alleles == null) || (alleles.size() == 0)) { throw new Exception("Cache of QSFeatures is empty"); }

		Map<Integer,List<String>> mySynonyms = cacheSynonyms(ALLELE);
		
		// Index directly-associated synonyms.
		for (Integer alleleKey : mySynonyms.keySet()) {
			if (alleles.containsKey(alleleKey)) {
				QSAllele feature = alleles.get(alleleKey);
				for (String synonym : mySynonyms.get(alleleKey)) {
					addDoc(buildDoc(feature, null, synonym, null, synonym, "Synonym", SYNONYM_WEIGHT));
					addDoc(buildDoc(feature, null, null, synonym, synonym, "Synonym", SYNONYM_WEIGHT));
				}
			}
		}
		logger.info("Indexed synonyms for " + mySynonyms.size() + " alleles");
		
		// Alleles must also be indexed for the synonyms of their respective markers.
		Map<Integer,List<String>> markerSynonyms = cacheSynonyms(MARKER);

		String cmd = "select a.allele_key, m.marker_key " + 
			"from allele a, marker_to_allele m " + 
			"where a.allele_key = m.allele_key " + 
			"and a.is_wild_type = 0 " +
			"order by a.allele_key";
	
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			Integer alleleKey = rs.getInt("allele_key");
			Integer markerKey = rs.getInt("marker_key");
			
			if (markerSynonyms.containsKey(markerKey)) {
				QSAllele feature = alleles.get(alleleKey);
				for (String synonym : markerSynonyms.get(markerKey)) {
					addDoc(buildDoc(feature, null, synonym, null, synonym, "Marker Synonym", MARKER_SYNONYM_WEIGHT));
					addDoc(buildDoc(feature, null, null, synonym, synonym, "Marker Synonym", MARKER_SYNONYM_WEIGHT));
				}
			}
		}
		rs.close();
		logger.info("Indexed marker synonyms for alleles");
	}
	
	// Look up annotations and index according to the given SQL command.  Expected field names: allele_key, primary_id.
	// The given SQL command must order by the allele_key field to help identify duplicates (including ancestor terms).
	// VocabTermCache vtc is used to look up data for each vocab term.
	// The 4 weights are for the four different data pieces to be indexed.
	private void indexAnnotations (String dataType, String cmd, VocabTermCache vtc,
		Integer nameWeight, Integer idWeight, Integer synonymWeight) throws SQLException {
		
		logger.info(" - indexing " + dataType + " for alleles");

		// counter of indexed items
		int i = 0;
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next())  {  
			Integer alleleKey = rs.getInt("allele_key");
			String termID = rs.getString("primary_id");
			
			if (alleles.containsKey(alleleKey) && vtc.containsKey(termID)) {
				QSAllele feature = alleles.get(alleleKey);
				VocabTerm term = vtc.getTerm(termID);

				// need to also index ancestors of the term, to handle down-the-DAG searches...
				Set<VocabTerm> toIndex = new HashSet<VocabTerm>();
				toIndex.add(term);

				for (VocabTerm ancestor : vtc.getAncestors(term.getTermKey())) {
					toIndex.add(ancestor);
				}
				
				for (VocabTerm termToIndex : toIndex) {
					String prefix = "";
					int directBoost = 1;	// slight preference for direct annotations over subterms
					if (!term.getPrimaryID().equals(termToIndex.getPrimaryID())) {
						prefix = "Subterm ";
						directBoost = 0;
					}
					
					String name = termToIndex.getTerm();
					if ((nameWeight != null) && (name != null) && (name.length() > 0)) {
						if (!name.equals(term.getTerm())) {
							addDoc(buildDoc(feature, null, null, name, term.getTerm() + " (" + name + ")", prefix + dataType, nameWeight + directBoost));
						} else {
							addDoc(buildDoc(feature, null, null, name, term.getTerm(), prefix + dataType, nameWeight + directBoost));
						}
						i++;
					}
				
					List<String> termIDs = termToIndex.getAllIDs();
					if ((idWeight != null) && (termIDs != null) && (termIDs.size() > 0)) {
						for (String id : termIDs) {
							addDoc(buildDoc(feature, id, null, null, term.getTerm() + " (ID: " + id + ")", prefix + dataType, idWeight + directBoost));
							i++;
						}
					}
				
					List<String> synonyms = termToIndex.getSynonyms();
					if ((synonymWeight != null) && (synonyms != null) && (synonyms.size() > 0)) {
						for (String synonym : synonyms) {
							addDoc(buildDoc(feature, null, null, synonym, term.getTerm() + " (synonym: " + synonym + ")", prefix + dataType, synonymWeight + directBoost));
							i++;
						}
					}
				}
			}
		}
		rs.close();
		
		logger.info(" - done with " + dataType + " for alleles (indexed " + i + " items)");
	}
	
	// Index the MP terms for the given feature type.  Assumes caches are loaded.
	private void indexMP() throws SQLException {
		// Find the allele's genotype's annotations (with null qualifiers).
		String cmd = "select distinct m.allele_key as allele_key, a.term_id as primary_id " + 
			"from allele m, allele_to_genotype mtg, genotype_to_annotation gta, annotation a " + 
			"where m.allele_key = mtg.allele_key " + 
			"and mtg.genotype_key = gta.genotype_key " + 
			"and gta.annotation_key = a.annotation_key " + 
			"and a.qualifier is null " + 
			"and a.annotation_type = 'Mammalian Phenotype/Genotype' " + 
			"order by m.allele_key";

		indexAnnotations("Phenotype", cmd, mpOntologyCache, MP_NAME_WEIGHT, MP_ID_WEIGHT, MP_SYNONYM_WEIGHT);
	}

	/* Get the various parts of the allele symbol that we need to index.
	 */
	private List<String> getAlleleSymbolPieces(String symbol) {
		List<String> out = new ArrayList<String>();
		
		if (symbol != null) {
			// 1. match to full marker<allele> symbol
			out.add(symbol);
			
			if ((symbol.indexOf("<") >= 0) && (symbol.indexOf(">") >= 0)) {
				// 2. match to the markerallele symbol (minus the angle brackets)
				out.add(symbol.replaceAll("<", "").replaceAll(">", ""));

				// 3. match to just allele symbol, ignoring marker symbol and angle brackets
				String justAllele = symbol.replaceAll(".*<", "").replaceAll(">.*", "");
				out.add(justAllele);

				// 4. match delimited parts of the allele symbol (using non-alpha-numerics as delimiters)
				String[] pieces = justAllele.replaceAll("[^A-Za-z0-9]", " ").split(" ");
				if (pieces.length > 1) {
					for (String piece : pieces) {
						out.add(piece);
					}
				}
			}
		}
		
		return out;
	}
	
	/* Load the features of the given type, cache them, generate initial documents and send them to Solr.
	 * Assumes cacheLocations has been run for this featureType.
	 */
	private void buildInitialDocs() throws Exception {
		logger.info(" - loading alleles");

		String prefix = "Allele ";

		alleles = new HashMap<Integer,QSAllele>();
		
		long padding = 0;	// amount of initial padding before sequence numbers should begin
		
		String cmd = "select a.allele_key as allele_key, a.primary_id, a.symbol, a.name, a.allele_type as subtype, " + 
				"m.symbol as marker_symbol, m.name as marker_name, s.by_symbol " +
			"from allele a " +
			"inner join allele_sequence_num s on (a.allele_key = s.allele_key) " +
			"left outer join marker_to_allele mta on (a.allele_key = mta.allele_key) " +
			"left outer join marker m on (mta.marker_key = m.marker_key) " +
			"where a.is_wild_type = 0 " +
			"order by a.allele_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		while (rs.next())  {  
			i++;
			Integer alleleKey = rs.getInt("allele_key");

			//--- prepare the new feature object
			
			QSAllele allele = new QSAllele(alleleKey);
			alleles.put(alleleKey, allele);
			
			allele.primaryID = rs.getString("primary_id");
			allele.symbol = rs.getString("symbol");
			allele.alleleType = rs.getString("subtype");
			allele.sequenceNum = rs.getLong("by_symbol");
			
			// NOTE: The feature name is currently picked up from the db and cached in the fewi, so this value
			// from the index does not get displayed.
			String markerName = rs.getString("marker_name");
			if ((markerName != null) && (!markerName.equals(allele.name))) {
				allele.name = markerName + "; " + rs.getString("name");
			} else {
				allele.name = rs.getString("name");
			}

			//--- index the new feature object in basic ways (primary ID, symbol, name, etc.)
			
			addDoc(buildDoc(allele, allele.primaryID, null, null, allele.primaryID, prefix + "ID", PRIMARY_ID_WEIGHT));
			
			// For alleles, we also need to consider the nomenclature of each one's associated marker. (Marker name
			// is already considered with the allele name.)
			String markerSymbol = rs.getString("marker_symbol");

			if (markerSymbol != null) {
				addDoc(buildDoc(allele, markerSymbol, null, null, markerSymbol, "Marker Symbol", MARKER_SYMBOL_WEIGHT));
			}

			// split allele symbol into relevant pieces that need to be indexed separately
			for (String piece : getAlleleSymbolPieces(allele.symbol)) {
				addDoc(buildDoc(allele, piece, null, null, allele.symbol, "Symbol", SYMBOL_WEIGHT));
			}
				
			// For transgenic alleles, we need to index by the parts of the allele symbol, including those
			// parts appearing in parentheses.
			
			// Strip out parentheses, hyphens, and commas.  Then skip the "Tg" and index the other pieces by exact match.
			String[] tgParts = allele.symbol.replaceAll("\\(", " ").replaceAll("\\)", " ").replaceAll("-", " ").replaceAll(",", " ").replaceAll("/", " ").split(" ");
			for (String part : tgParts) {
				if (!"Tg".equals(part)) {
					addDoc(buildDoc(allele, part, null, null, allele.symbol, "Symbol", TRANSGENE_PART_WEIGHT));
				}
			}

			// feature name
			addDoc(buildDoc(allele, null, null, allele.name, allele.name, "Name", NAME_WEIGHT));
		}

		rs.close();

		logger.info("done with basic data for " + i + " alleles");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		this.setSkipOptimizer(true);
		
		// cache vocabulary term data
		diseaseOntologyCache = new VocabTermCache("Disease Ontology", ex);
		mpOntologyCache = new VocabTermCache("Mammalian Phenotype", ex);
		this.cacheHighLevelTerms();

		logger.info("beginning alleles");
		
		cacheLocations();
		buildInitialDocs();

		indexSynonyms();
		clearIndexedTermCache();		// only clear once all nomen done

		indexIDs();
		clearIndexedTermCache();		// only clear once all IDs done
		
		indexMouseDiseaseAnnotations();
		clearIndexedTermCache();		// only clear once all disease data done
		
		indexMP();
		clearIndexedTermCache();		// only clear once all phenotype data done

		// send any remaining documents and commit all the changes to Solr
		if (docs.size() > 0) {
			writeDocs(docs);
		}

		logger.info("finished alleles");
		commit();
	}
	
	// private class for caching allele data that will be re-used across multiple documents
	private class QSAllele {
		private Integer alleleKey;
		public String alleleType;
		public String symbol;
		public String primaryID;
		public String name;
		public Long sequenceNum;
		public Set<String> goProcessFacets;
		public Set<String> goFunctionFacets;
		public Set<String> goComponentFacets;
		public Set<String> diseaseFacets;
		public Set<String> phenotypeFacets;
		public Set<String> markerTypeFacets;

		// constructor
		public QSAllele(Integer alleleKey) {
			this.alleleKey = alleleKey;
		}
		
		// compose and return a new SolrInputDocument including the fields for this feature
		public SolrInputDocument getNewDocument() {
			SolrInputDocument doc = new SolrInputDocument();

			if (this.primaryID != null) { doc.addField(IndexConstants.QS_PRIMARY_ID, this.primaryID); }

			if (this.alleleType != null) { doc.addField(IndexConstants.QS_FEATURE_TYPE, this.alleleType + " allele"); }
			if (this.sequenceNum != null) { doc.addField(IndexConstants.QS_SEQUENCE_NUM, this.sequenceNum); }

			if (chromosome.containsKey(this.alleleKey)) { doc.addField(IndexConstants.QS_CHROMOSOME, chromosome.get(alleleKey)); }
			if (startCoord.containsKey(this.alleleKey)) { doc.addField(IndexConstants.QS_START_COORD, startCoord.get(alleleKey)); }
			if (endCoord.containsKey(this.alleleKey)) { doc.addField(IndexConstants.QS_END_COORD, endCoord.get(alleleKey)); }
			if (strand.containsKey(this.alleleKey)) { doc.addField(IndexConstants.QS_STRAND, strand.get(alleleKey)); }
				
			if (this.goProcessFacets != null) { doc.addField(IndexConstants.QS_GO_PROCESS_FACETS, this.goProcessFacets); }
			if (this.goFunctionFacets != null) { doc.addField(IndexConstants.QS_GO_FUNCTION_FACETS, this.goFunctionFacets); }
			if (this.goComponentFacets != null) { doc.addField(IndexConstants.QS_GO_COMPONENT_FACETS, this.goComponentFacets); }
			if (this.diseaseFacets != null) { doc.addField(IndexConstants.QS_DISEASE_FACETS, this.diseaseFacets); }
			if (this.phenotypeFacets != null) { doc.addField(IndexConstants.QS_PHENOTYPE_FACETS, this.phenotypeFacets); }
			if (this.markerTypeFacets != null) { doc.addField(IndexConstants.QS_MARKER_TYPE_FACETS, this.markerTypeFacets); }

			return doc;
		}
	}
}