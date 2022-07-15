package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.CreFields;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;

/**
 * CreIndexerSQL
 * @author mhall
 * 
 * This class is responsible for populating the cre index.  This index is 
 * fairly straight forward, with only one sub object relationship.
 * 
 * Note: refactored during 5.x development
 */

public class CreAssayResultIndexerSQL extends Indexer {

	public int BATCH_SIZE = 10000;


	/* state variables */
	private int startResultKey = 0;
	private int endResultKey = 0;
	private int maxResultKey = 0;

	// lookups
	Map<String,List<String>> structureAncestorIdMap;
	Map<String,List<String>> structureSynonymMap;
	Map<String, AlleleSorts> alleleSortsMap;
	Map<String,Integer> byDetected;
	Map<String,Integer> byNotDetected;
	
	// map of *all* systems for the allele
	Map<String, AlleleSystems> alleleSystemsMap;
	// map of systems for a specific assay result
	Map<String, List<CreAlleleSystem>> systemMap;
	
	// mapping from EMAPA structure keys to their respective terms
	private Map<String,String> emapaTerms;
	
	// mapping from EMAPA structure keys to their respective synonyms
	private Map<String,Set<String>> emapaSynonyms;
	
	// mapping from each allele key to the keys of all EMAPA structures (and their ancestors) 
	// where recombinase activity is detected, (Union over activity locations and their ancestors.)
	private Map<String, Set<String>> allStructures;
	
	// mapping from each allele key to the keys of EMAPA structures (and their ancestors) 
	// where ALL detected activity is found. (Intersection over activity locations and their ancestors,)
	private Map<String, Set<String>> exclusiveStructures;
	
	// shared empty set object to make code simpler later on
	private Set<String> emptySet = new HashSet<String>();

	// map from EMAPS term key to its EMAPA ancestor keys (stage-aware)
	Map<String, Set<String>> emapaAncestors;
	
        // map from driver key (ie, a marker key) to list of search strings it should match.
        private Map<String,Set<String>> clusterKey2searchStrings;
        private Map<String,Set<String>> clusterKey2facetStrings;;

	/***--- methods ---***/

	public CreAssayResultIndexerSQL () {
		super("creAssayResult");
	}


        // populate mapping from driver cluster_key to list of search terms (lowercase driver symbols) for that cluster
        private void fillDriverSearchStrings () throws SQLException {
                clusterKey2searchStrings = new HashMap<String,Set<String>>();
                clusterKey2facetStrings = new HashMap<String,Set<String>>();
                ResultSet rs1 = ex.executeProto("select driver_key, driver, cluster_key from driver");
                while (rs1.next()) {
                    String driverKey = rs1.getString("driver_key");
                    String driver = rs1.getString("driver");
                    String clusterKey = rs1.getString("cluster_key");
                    if (!clusterKey2searchStrings.containsKey(clusterKey)) {
                        clusterKey2searchStrings.put(clusterKey, new HashSet<String>());
                        clusterKey2facetStrings.put(clusterKey, new HashSet<String>());
                    }
                    clusterKey2searchStrings.get(clusterKey).add(driver.toLowerCase());
                    clusterKey2facetStrings.get(clusterKey).add(driver);
                }
        }
	// populate the mapping from EMAPA keys to terms
	public void fillEmapaTerms() throws Exception {
		emapaTerms = new HashMap<String,String>();
		
		String cmd = "select term_key, term from term where vocab_name = 'EMAPA'";
		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			emapaTerms.put(rs.getString("term_key"), rs.getString("term"));
		}
		rs.close();
		logger.info("Got " + emapaTerms.size() + " EMAPA terms");
	}

	// populate the mapping from EMAPA keys to synonyms
	public void fillEmapaSynonyms() throws Exception {
		emapaSynonyms = new HashMap<String,Set<String>>();
		
		String cmd = "select s.term_key, s.synonym "
			+ "from term t, term_synonym s "
			+ "where t.vocab_name = 'EMAPA' "
			+ " and t.term_key = s.term_key";
		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			String termKey = rs.getString("term_key");
			if (!emapaSynonyms.containsKey(termKey)) {
				emapaSynonyms.put(termKey, new HashSet<String>());
			}
			emapaSynonyms.get(termKey).add(rs.getString("synonym"));
		}
		rs.close();
		logger.info("Got " + emapaSynonyms.size() + " EMAPA synonyms");
	}
	
	// get the mapping from each EMAPS term key to its EMAPA ancestor terms
	public void fillEmapaAncestors() throws Exception {
		emapaAncestors = new HashMap<String,Set<String>>();
		
		String cmd = "select distinct a.term_key, te.emapa_term_key, e.emapa_term_key as ancestor_term_key "
			+ "from term_ancestor a, term t, term_emap e, term_emap te "
			+ "where a.term_key = t.term_key "
			+ " and a.ancestor_term_key = e.term_key "
			+ " and t.term_key = te.term_key "
			+ " and t.vocab_name = 'EMAPS'";
		
		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			String emapsTermKey = rs.getString("term_key");
			String emapaTermKey = rs.getString("emapa_term_key");
			String ancestorTermKey = rs.getString("ancestor_term_key");

			if (!emapaAncestors.containsKey(emapsTermKey)) {
				// add the initial set for this term, populated by the corresponding EMAPA term and its synonyms
				emapaAncestors.put(emapsTermKey, new HashSet<String>());
				emapaAncestors.get(emapsTermKey).add(emapaTerms.get(emapaTermKey));
				if (emapaSynonyms.containsKey(emapaTermKey)) {
					emapaAncestors.get(emapsTermKey).addAll(emapaSynonyms.get(emapaTermKey));
				}
			}
			// then add the term and its synonyms for the EMAPA ancestor term
			emapaAncestors.get(emapsTermKey).add(emapaTerms.get(ancestorTermKey));
			if (emapaSynonyms.containsKey(ancestorTermKey)) {
				emapaAncestors.get(emapsTermKey).addAll(emapaSynonyms.get(ancestorTermKey));
			}
		}
		rs.close();
		logger.info("Got EMAPA ancestors for " + emapaAncestors.size() + " EMAPS terms");
	}
	
	// get the set of EMAPA structure keys that are ancestors of the given EMAPS structure key,
	// including EMAPA key for 'emapsKey' itself
	public Set<String> getEmapaAncestors(String emapsKey) throws Exception {
		if (emapaAncestors == null) {
			fillEmapaAncestors();
		}
		if (emapaAncestors.containsKey(emapsKey)) {
			return emapaAncestors.get(emapsKey);
		}
		return emptySet;
	}

	// Precompute two structure sets for searching recombinase alleles.
	// 1. exclusiveStructures: identify the exclusive structures for each allele (the set of structures outside which there is
	// no recombinase activity detected for that allele), returning a mapping from:
	//		allele key (String) : set of structure terms (Strings)
	// Because everything traces up the DAG to 'mouse', everything should at least have one exclusive
	// structure returned.
	// Use:  This field is used when the user specifies a structure and checks the "nowhere else" checkbox.
	// 2. allStructures: set of all EMAPA structure keys (and their ancestors) where recombinase activity is detected
	//
	private void findStructures() throws Exception {
		allStructures = new HashMap<String, Set<String>>();
		exclusiveStructures = new HashMap<String, Set<String>>();
		
		int minAlleleKey = 0;	// lowest allele key with recombinase data
		int maxAlleleKey = 0;	// highest allele key with recombinase data
		int chunkSize = 50000;	// number of allele keys to process in a chunk
		
		// identify the lowest and highest allele keys, so we can iterate over them
		String minMaxCmd = "select min(allele_key) as min_key, max(allele_key) as max_key "
			+ "from recombinase_allele_system";
		ResultSet rs = ex.executeProto(minMaxCmd);
		if (rs.next()) {
			minAlleleKey = rs.getInt("min_key");
			maxAlleleKey = rs.getInt("max_key");
		}
		rs.close();
		
		// now walk through the alleles in chunks
		int startAllele = minAlleleKey - 1;
		int endAllele = startAllele + chunkSize;
		
		while (startAllele < maxAlleleKey) {
			// gather the EMAPS structures where recombinase activity was detected
			Map<String,Set<String>> structuresPerAllele = new HashMap<String,Set<String>>();

			String cmd = "select ras.allele_key, rar.structure_key "
				+ "from recombinase_allele_system ras, recombinase_assay_result rar "
				+ "where ras.allele_system_key = rar.allele_system_key "
				+ " and rar.level not in ('Ambiguous', 'Not Specified', 'Absent') "
				+ " and ras.allele_key > " + startAllele
				+ " and ras.allele_key <= " + endAllele
				+ " order by ras.allele_key";
			
			ResultSet rs1 = ex.executeProto(cmd);
			while (rs1.next()) {
				String alleleKey = rs1.getString("allele_key");
				if (!structuresPerAllele.containsKey(alleleKey)) {
					structuresPerAllele.put(alleleKey, new HashSet<String>());
				}
				structuresPerAllele.get(alleleKey).add(rs1.getString("structure_key"));
			}
			rs1.close();
			
			// Now we can compute the set of 'exclusive structures' for each allele by taking the
			// intersection of the EMAPA ancestors for each EMAPS structure with recombinase
			// activity detected.
			
			for (String alleleKey : structuresPerAllele.keySet()) {
				Set<String> allAncestors = null;
				Set<String> commonAncestors = null;
				for (String emapsKey : structuresPerAllele.get(alleleKey)) {
					Set<String> resultAncestors = new HashSet<String>();
					resultAncestors.addAll(this.getEmapaAncestors(emapsKey));
					
					if (allAncestors == null) {
						allAncestors = new HashSet<String>(resultAncestors);
					} else {
						allAncestors.addAll(resultAncestors);
					}

					if (commonAncestors == null) {
						commonAncestors = new HashSet<String>(resultAncestors);
					} else {
						commonAncestors.retainAll(resultAncestors);
					}
				}
				allStructures.put(alleleKey, allAncestors);
				exclusiveStructures.put(alleleKey, commonAncestors);
			}
			startAllele = endAllele;
			endAllele = startAllele + chunkSize;
		}
		logger.info("Got all structures for " + allStructures.size() + " alleles");
		logger.info("Got exclusive structures for " + exclusiveStructures.size() + " alleles");
	}

	/*
	 * Indexes both assay result documents 
	 * 	and the documents for alleles with no assay results (i.e. stubs)
	 */
	public void index() throws Exception
	{   
                fillDriverSearchStrings();
		fillEmapaTerms();
		fillEmapaSynonyms();
		fillSystemSorts();
		findStructures();

		loadAssayResults();
		loadAllelesWithNoData();

		commit();    

	}


	private void loadAssayResults() throws SQLException {


		// Fetch relevant data maps
		this.structureAncestorIdMap = queryStructureAncestorIdMap();
		this.structureSynonymMap = queryStructureSynonymMap();

		// Process Cre Assay Results in batches of this.batchSize

		ResultSet rs = ex.executeProto("select max(result_key) as max_result_key "
				+ "from recombinase_assay_result");
		rs.next();

		this.startResultKey = 0;
		this.maxResultKey = rs.getInt("max_result_key");

		// While it appears that modValue could be one iteration too low (due
		// to rounding down), this is accounted for by using <= in the loop.

		int modValue = this.maxResultKey / this.BATCH_SIZE;

		// Perform the chunking

		logger.info("Getting all assay results and related search criteria");
		logger.info("Max result_key: " + endResultKey + ", chunks: " + (modValue + 1));

		for (int i = 0; i <= modValue; i++) {

			this.startResultKey = i * this.BATCH_SIZE;
			this.endResultKey = this.startResultKey + this.BATCH_SIZE;


			logger.info("Processing cre assay result_key > " + startResultKey + " and <= " + endResultKey);
			String assayResultQuery = "select "
					+ "rar.result_key, "
					+ "rar.structure, "
					+ "rar.level, "
					+ "rar.structure_key, "
					+ "emapa.primary_id as structure_id, "
					+ "ras.allele_id, "
					+ "ras.allele_key, "
					+ "a.driver, "
					+ "a.driver_key, "
                                        + "drv.cluster_key, "
					+ "a.inducible_note, "
					+ "rarsn.by_structure, "
					+ "rarsn.by_age, "
					+ "rarsn.by_level, "
					+ "rarsn.by_pattern, "
					+ "rarsn.by_jnum_id, "
					+ "rarsn.by_assay_type, "
					+ "rarsn.by_reporter_gene, "
					+ "rarsn.by_detection_method, "
					+ "rarsn.by_assay_note, "
					+ "rarsn.by_allelic_composition, "
					+ "rarsn.by_sex, "
					+ "rarsn.by_specimen_note, "
					+ "rarsn.by_result_note "
					+ "from recombinase_allele_system as ras "
					+ "join recombinase_assay_result as rar on "
					+ 	"ras.allele_system_key = rar.allele_system_key " 
					+ "join recombinase_assay_result_sequence_num as rarsn on "
					+ 	"rar.result_key = rarsn.result_key "
					+ "join allele a on "
					+ 	"a.allele_key = ras.allele_key "
					+ "join driver drv on "
                                        +       "drv.driver_key = a.driver_key "
					+ "join term_emap e on rar.structure_key = e.term_key "
					+ "join term emapa on "
					+ 	"e.emapa_term_key = emapa.term_key "
					+ "where rar.result_key > " + startResultKey + " and rar.result_key <= " + endResultKey
					+ " order by rar.result_key ";

			rs = ex.executeProto(assayResultQuery);

			// Retrieve any lookups needed for this batch
			this.alleleSortsMap = queryAlleleSortsMap(startResultKey, endResultKey);
			this.alleleSystemsMap = queryAlleleSystemsMap(startResultKey, endResultKey);
			this.systemMap = querySystemMap(startResultKey, endResultKey);

			logger.info("Parsing Assay Results");
			processResults(rs, ResultType.ASSAY_RESULT);


			// clear batch lookups until next use
			this.alleleSortsMap = null;
			this.alleleSystemsMap = null;
			this.systemMap = null;
		}


		// clear global structure lookups
		this.structureAncestorIdMap = null;
		this.structureSynonymMap = null;

	}

	private void loadAllelesWithNoData() throws SQLException {
		// Process all cre alleles with no cre data
		String alleleQuery = "select a.allele_key, "
				+ "a.primary_id as allele_id,"
				+ "a.driver,"
				+ "a.driver_key,"
                                + "drv.cluster_key,"
				+ "a.inducible_note "
				+ "from allele a "
                                + "join driver drv on "
                                +    "drv.driver_key = a.driver_key "
				+ "where a.driver is not null "
				+ "and not exists (select 1 from recombinase_allele_system ras "
				+ 	"where ras.allele_key = a.allele_key "
				+ ") ";

		ResultSet rs = ex.executeProto(alleleQuery);

		this.alleleSortsMap = queryNoDataAlleleSortsMap();

		logger.info("Parsing Alleles");
		processResults(rs, ResultType.ALLELE);


		// clear allele lookups
		this.alleleSortsMap = null;
	}


	/*
	 * Process the current result set (rs)
	 * 	of resultType (ALLELE or ASSAY_RESULT)
	 * 	and send the documents to Solr
	 */
	private void processResults(ResultSet rs, ResultType resultType) throws SQLException {

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		while (rs.next()) {
			String alleleKey = rs.getString("allele_key");

			SolrInputDocument doc = new SolrInputDocument();

			// allele level fields
			doc.addField(IndexConstants.ALL_ID, rs.getString("allele_id"));
			doc.addField(IndexConstants.ALL_KEY, alleleKey);

			if (this.alleleSortsMap.containsKey(alleleKey)) {
				AlleleSorts alleleSorts = this.alleleSortsMap.get(alleleKey);

				doc.addField(IndexConstants.ALL_IMSR_COUNT, alleleSorts.strainCount);
				doc.addField(CreFields.ALL_REFERENCE_COUNT_SORT, alleleSorts.referenceCount);
				doc.addField(CreFields.ALL_TYPE_SORT, alleleSorts.byAlleleType);
				doc.addField(CreFields.ALL_SYMBOL_SORT, alleleSorts.bySymbol);
				doc.addField(CreFields.DRIVER_SORT, alleleSorts.byDriver);
			}
			doc.addField(CreFields.DRIVER, rs.getString("driver"));
                        for(String searchString : clusterKey2searchStrings.get(rs.getString("cluster_key"))) {
                            doc.addField(CreFields.DRIVER_SEARCH, searchString);
                        }
                        String facetString = "";
                        for(String fString : clusterKey2facetStrings.get(rs.getString("cluster_key"))) {
                            if (facetString.length() > 0) {
                                facetString += ",";
                            }
                            facetString += fString;
                        }
                        doc.addField(CreFields.DRIVER_FACET, facetString);

			doc.addField(CreFields.INDUCER, rs.getString("inducible_note"));

			if (resultType == ResultType.ALLELE) {

				int resultKey = this.maxResultKey + rs.getInt("allele_key");
				// unique key for a document, so is mandatory
				// base it off allele key for the alleles with no assay results
				doc.addField(CreFields.ASSAY_RESULT_KEY, resultKey);

				// other mandatory fields we can null out
				doc.addField(CreFields.DETECTED, false);

			} // end ALLELE fields
			// ASSAY_RESULT only fields
			else if (resultType == ResultType.ASSAY_RESULT) {


				String resultKey = rs.getString("result_key");
				String structure = rs.getString("structure");
				String structureKey = rs.getString("structure_key");	// EMAPS key
				String structureId = rs.getString("structure_id");		// EMAPA ID

				boolean detected = this.isDetected(rs.getString("level"));


				// system level fields
				if (this.systemMap.containsKey(resultKey)) {

					List<CreAlleleSystem> creSystems = this.systemMap.get(resultKey);
					for (CreAlleleSystem creSystem : creSystems) {
						doc.addField(CreFields.ALL_SYSTEM_KEY, creSystem.alleleSystemKey);
						doc.addField(CreFields.SYSTEM, creSystem.system);
					}

					// pick one system for the group key
					String systemGroup = getSystemHighlightGroup(alleleKey, creSystems.get(0).system, detected);
					doc.addField(CreFields.SYSTEM_HL_GROUP, systemGroup);
				}

				// *all* systems for the allele associated with this assay result
				if(this.alleleSystemsMap.containsKey(alleleKey)) {

					AlleleSystems alleleSystems = this.alleleSystemsMap.get(alleleKey);
					for (String system : alleleSystems.detectedSystems) {
						doc.addField(CreFields.ALL_SYSTEM_DETECTED, system);
					}
					for (String system : alleleSystems.notDetectedSystems) {
						doc.addField(CreFields.ALL_SYSTEM_NOT_DETECTED, system);
					}
				}

				// result level fields
				doc.addField(CreFields.DETECTED, detected);

				doc.addField(CreFields.ASSAY_RESULT_KEY, resultKey);
				doc.addField(CreFields.ANNOTATED_STRUCTURE, structure);
				doc.addField(CreFields.ANNOTATED_STRUCTURE_KEY, structureKey);


				this.addFieldNoDup(doc,CreFields.STRUCTURE_SEARCH,structure);
				this.addFieldNoDup(doc,CreFields.STRUCTURE_ID, structureId);

				if(this.structureAncestorIdMap.containsKey(structureKey))
				{
					// get ancestors
					List<String> structure_ancestor_ids = this.structureAncestorIdMap.get(structureKey);
					for (String structure_ancestor_id : structure_ancestor_ids)
					{
						// get synonyms for each ancestor/term
						if(this.structureSynonymMap.containsKey(structure_ancestor_id))
						{
							//also add structure MGI ID
							this.addFieldNoDup(doc,GxdResultFields.STRUCTURE_ID, structure_ancestor_id);
							List<String> structureSynonyms = this.structureSynonymMap.get(structure_ancestor_id);
							for (String structureSynonym : structureSynonyms)
							{
								this.addFieldNoDup(doc,CreFields.STRUCTURE_SEARCH,structureSynonym);
							}
						}
					}
					this.resetDupTracking();
				}              

				if (allStructures.containsKey(alleleKey)) {
					doc.addField(CreFields.ALL_ALL_STRUCTURES, allStructures.get(alleleKey));
				}

				if (exclusiveStructures.containsKey(alleleKey)) {
					doc.addField(CreFields.ALL_EXCLUSIVE_STRUCTURES, exclusiveStructures.get(alleleKey));
				}

				// Add in the result sorting columns

				doc.addField(CreFields.BY_STRUCTURE, rs.getString("by_structure"));
				doc.addField(CreFields.BY_AGE, rs.getString("by_age"));
				doc.addField(CreFields.BY_LEVEL, rs.getString("by_level"));
				doc.addField(CreFields.BY_PATTERN, rs.getString("by_pattern"));
				doc.addField(CreFields.BY_JNUM_ID, rs.getString("by_jnum_id"));
				doc.addField(CreFields.BY_ASSAY_TYPE, rs.getString("by_assay_type"));
				doc.addField(CreFields.BY_REPORTER_GENE, rs.getString("by_reporter_gene"));
				doc.addField(CreFields.BY_DETECTION_METHOD, rs.getString("by_detection_method"));
				doc.addField(CreFields.BY_ASSAY_NOTE, rs.getString("by_assay_note"));
				doc.addField(CreFields.BY_ALLELIC_COMPOSITION, rs.getString("by_allelic_composition"));
				doc.addField(CreFields.BY_SEX, rs.getString("by_sex"));
				doc.addField(CreFields.BY_SPECIMEN_NOTE, rs.getString("by_specimen_note"));
				doc.addField(CreFields.BY_RESULT_NOTE, rs.getString("by_result_note"));
			} // end ASSAY_RESULT fields

			// add allele-level fields computed to sort by Detected and Not Detected systems (where
			// available) and falling back on smart-alpha symbol sort otherwise
			
			if (this.byDetected.containsKey(alleleKey)) {
				doc.addField(CreFields.BY_DETECTED, this.byDetected.get(alleleKey));
			}
			if (this.byNotDetected.containsKey(alleleKey)) {
				doc.addField(CreFields.BY_NOT_DETECTED, this.byNotDetected.get(alleleKey));
			}

			docs.add(doc);

			if (docs.size() > 10000) {
				this.writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}
		this.writeDocs(docs);

	}



	/**
	 * Helper queries
	 */

	/*
	 * Query the sorts needed for allele level fields
	 * 	where allele is cre but has no data
	 */
	private Map<String, AlleleSorts> queryNoDataAlleleSortsMap() throws SQLException {

		String query = "select a.allele_key, "
				+ "asn.by_allele_type, "
				+ "asn.by_symbol, "
				+ "asn.by_driver, "
				+ "ac.reference_count, "
				+ "aic.strain_count "  
				+ "from allele a "
				+ "join allele_sequence_num as asn "
				+	"on asn.allele_key = a.allele_key  "
				+ "join allele_counts as ac "
				+	"on a.allele_key = ac.allele_key "
				+ "join allele_imsr_counts as aic "
				+	"on a.allele_key = aic.allele_key  "
				+ "where a.driver is not null "
				;

		return queryAlleleSortsMap(query);
	}

	/*
	 * Query the sorts needed for allele level fields
	 * 	where result_key between startResultKey to endResultKey
	 */
	private Map<String, AlleleSorts> queryAlleleSortsMap(int startResultKey, int endResultKey) throws SQLException {

		String query = "select a.allele_key, "
				+ "asn.by_allele_type, "
				+ "asn.by_symbol, "
				+ "asn.by_driver, "
				+ "ac.reference_count, "
				+ "aic.strain_count "  
				+ "from allele as a "
				+ "join allele_sequence_num as asn "
				+	"on a.allele_key = asn.allele_key  "
				+ "join allele_counts as ac "
				+	"on a.allele_key = ac.allele_key "
				+ "join allele_imsr_counts as aic "
				+	"on a.allele_key = aic.allele_key  "

	        	// Filter by this result_key batch
	        	+ "where exists (select 1 from "
	        	+ 	"recombinase_allele_system ras join "
	        	+ 	"recombinase_assay_result rar on "
	        	+ 		"rar.allele_system_key = ras.allele_system_key "
	        	+ 	"where ras.allele_key = a.allele_key and "
	        	+   "rar.result_key > " + startResultKey + " and rar.result_key <= " + endResultKey
	        	+ ")";

		return queryAlleleSortsMap(query);
	}

	/*
	 * Takes a query for allele counts and packages it into AlleleSorts objects
	 * 	mapped by allele_key
	 */
	private Map<String, AlleleSorts> queryAlleleSortsMap(String query) throws SQLException {
		Map<String, AlleleSorts> alleleSortsMap = new HashMap<String, AlleleSorts>();
		logger.info("building map of allele keys to allele sorts");
		ResultSet rs = ex.executeProto(query);
		while(rs.next()){

			AlleleSorts alleleSorts = new AlleleSorts();
			alleleSorts.strainCount = rs.getInt("strain_count");
			alleleSorts.referenceCount = rs.getInt("reference_count");
			alleleSorts.byAlleleType = rs.getInt("by_allele_type");
			alleleSorts.bySymbol = rs.getInt("by_symbol");
			alleleSorts.byDriver = rs.getInt("by_driver");

			alleleSortsMap.put(rs.getString("allele_key"), alleleSorts);
		}
		logger.info("done building map of allele keys to allele sorts");

		return alleleSortsMap;
	}

	/*
	 * Query all the detected and not detected systems for each allele
	 * 	with result_key between startResultKey and endResultKey
	 *  Maps them by allele_key
	 */
	private Map<String, AlleleSystems> queryAlleleSystemsMap(int startResultKey, int endResultKey) throws SQLException {

		String query = "with batch_alleles as ( "
				+ 	"select distinct allele_key from "
				+ 	"recombinase_allele_system ras "
				+ 	"join recombinase_assay_result rar on "
				+ 		"rar.allele_system_key = ras.allele_system_key "
				// filter out only alleles that apply to this batch of assay results
				+ 	"where rar.result_key > " + startResultKey + " and rar.result_key <= " + endResultKey + " "
				+ ")"
				+ "select aus.allele_key, "
				+ "aus.system, "
				+ "false as detected "
				+ "from recombinase_unaffected_system aus "
				+ "join batch_alleles ba on "
				+ 	"ba.allele_key = aus.allele_key "
				+ "UNION "
				+ "select aas.allele_key, "
				+ "aas.system, "
				+ "true as detected "
				+ "from recombinase_affected_system aas "
				+ "join batch_alleles ba on "
				+ 	"ba.allele_key = aas.allele_key "
				;

		Map<String, AlleleSystems> alleleSystemsMap = new HashMap<String, AlleleSystems>();

		ResultSet rs = ex.executeProto(query);

		while(rs.next()) {

			String alleleKey = rs.getString("allele_key");
			String system = rs.getString("system");
			boolean detected = rs.getBoolean("detected");

			if(!alleleSystemsMap.containsKey(alleleKey)){
				alleleSystemsMap.put(alleleKey, new AlleleSystems());
			}

			if(detected) {
				alleleSystemsMap.get(alleleKey).detectedSystems.add(system);
			}
			else {
				alleleSystemsMap.get(alleleKey).notDetectedSystems.add(system);
			}

		}

		return alleleSystemsMap;
	}

	/*
	 * Query the mapping between result_key and system(s)
	 */
	private Map<String, List<CreAlleleSystem>> querySystemMap(int startResultKey, int endResultKey) throws SQLException {

		Map<String, List<CreAlleleSystem>> systemMap = new HashMap<String, List<CreAlleleSystem>>();
		logger.info("building map of result keys to affected system");

		String query = "select rar.result_key, "
				+ "ras.allele_system_key, "
				+ "ras.system "
				+ "from recombinase_allele_system ras "
				+ "join recombinase_assay_result rar on "
				+ 	"rar.allele_system_key = ras.allele_system_key "
				+ "where rar.result_key > " + startResultKey + " and rar.result_key <= " + endResultKey;

		ResultSet rs = ex.executeProto(query);

		while(rs.next()) {

			String resultKey = rs.getString("result_key");
			if (!systemMap.containsKey(resultKey)) {
				systemMap.put(resultKey, new ArrayList<CreAlleleSystem>());
			}

			CreAlleleSystem creSystem = new CreAlleleSystem();
			creSystem.alleleSystemKey = rs.getInt("allele_system_key");
			creSystem.system = rs.getString("system");

			systemMap.get(resultKey).add(creSystem);
		}
		logger.info("done building map of result keys to affected system");


		return systemMap;
	}


	/*
	 * Query the ancestor IDs for all structures
	 */
	private Map<String,List<String>> queryStructureAncestorIdMap() throws SQLException {
		Map<String,List<String>> structureAncestorIdMap = new HashMap<String,List<String>>();
		logger.info("building map of structure term_key to ancestor IDs");

		String structureAncestorQuery = SharedQueries.GXD_EMAP_ANCESTOR_QUERY;
		ResultSet rs = ex.executeProto(structureAncestorQuery);

		while (rs.next())
		{
			String skey = rs.getString("structure_term_key");
			String ancestorId = rs.getString("ancestor_id");
			String structureId = rs.getString("structure_id");
			if(!structureAncestorIdMap.containsKey(skey))
			{
				structureAncestorIdMap.put(skey, new ArrayList<String>());
				// Include original term
				structureAncestorIdMap.get(skey).add(structureId);
			}
			structureAncestorIdMap.get(skey).add(ancestorId);
		}
		logger.info("done building map of structure term_key to ancestor IDs");

		return structureAncestorIdMap;
	}

	/*
	 * Query the synonyms for all structures
	 */
	private Map<String,List<String>> queryStructureSynonymMap() throws SQLException {
		Map<String,List<String>> structureSynonymMap = new HashMap<String,List<String>>();
		logger.info("building map of structure synonyms");
		String structureSynonymQuery = SharedQueries.GXD_EMAP_SYNONYMS_QUERY;
		ResultSet rs = ex.executeProto(structureSynonymQuery);
		while (rs.next())
		{
			String sId = rs.getString("structure_id");
			String synonym = rs.getString("synonym");
			String structure = rs.getString("structure");
			if(!structureSynonymMap.containsKey(sId))
			{
				structureSynonymMap.put(sId, new ArrayList<String>());
				// Include original term
				structureSynonymMap.get(sId).add(structure);
			}
			structureSynonymMap.get(sId).add(synonym);
		}
		logger.info("done gathering structure synonyms");

		return structureSynonymMap;
	}


	/**
	 * Helper functions
	 */

	/*
	 * Returns if the detection level represents a detected result
	 */
	boolean isDetected(String level) {

		boolean detected = true;

		if ("absent".equalsIgnoreCase(level)) {
			detected = false;
		} else if ("ambiguous".equalsIgnoreCase(level)) {
			detected = false;
		}
		return detected;

	}

	/*
	 * Get the key for grouping by allele/system/detection
	 */
	String getSystemHighlightGroup(String alleleKey, String system, boolean detected) {

		String detection = "yes";
		if ( !detected) {
			detection = "no";
		}

		List<String> keys = Arrays.asList(alleleKey, system, detection);
		return StringUtils.join(keys, "-");
	}

	/* Populate the maps for sorting by Detected and Not Detected systems.  Each map is from
	 * a (String) allele key to a sort value (Integer).
	 */
	public void fillSystemSorts() throws SQLException {
		// first, we need to collect systems for all the recombinase alleles
		
		// ordered to group rows by allele, then system, then detected before not detected
		String cmd = "select distinct a.allele_key, s.system, n.by_symbol, "
			+ " case when r.level in ('Ambiguous', 'Absent', 'Not Specified') then 0 "
			+ "   else 1 end detected "
			+ "from allele a "
			+ "inner join allele_sequence_num n on (a.allele_key = n.allele_key) "
			+ "left outer join recombinase_allele_system s on (s.allele_key = a.allele_key) "
			+ "left outer join recombinase_assay_result r on (r.allele_system_key = s.allele_system_key) "
			+ "where driver_key is not null "
			+ "order by 3, 2, 4 desc";

		Map<String,Integer> bySymbol = new HashMap<String,Integer>();				// allele key : sort value
		Map<String,StringBuffer> detecteds = new HashMap<String,StringBuffer>();	// allele key : detected systems
		Map<String,StringBuffer> notDetecteds = new HashMap<String,StringBuffer>();	// allele key : not detected systems
		
		String lastAlleleKey = "default";
		String lastSystem = "default";

		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			String alleleKey = rs.getString("allele_key");
			String system = rs.getString("system");
			Integer detected = rs.getInt("detected");
			Map<String,StringBuffer> map = null;

			if (!bySymbol.containsKey(alleleKey)) {
				bySymbol.put(alleleKey, rs.getInt("by_symbol"));
			}
			
			// if there's no system, no other data to collect
			if ((system != null) && (system.trim().length() > 0)) {

				// if the allele & system match the previous, then this 'not detected' annotation is
				// overridden by the previous 'detected' annotation, so skip it
				if (!lastAlleleKey.equals(alleleKey) || !lastSystem.equals(system)) {

					// this is a new annotation, so pick the correct map & run with it
					if (detected == 0) {
						map = notDetecteds;
					} else {
						map = detecteds;
					}
			
					if (map.containsKey(alleleKey)) {
						map.get(alleleKey).append(",");
						map.get(alleleKey).append(rs.getString("system"));
					} else {
						map.put(alleleKey, new StringBuffer(rs.getString("system")));
					}
				}
			}
			lastAlleleKey = alleleKey;
			lastSystem = system;
		}
		rs.close();
		logger.info("Collected systems for " + bySymbol.size() + " alleles");
		
		this.byDetected = buildSortMap(bySymbol, detecteds);
		this.byNotDetected = buildSortMap(bySymbol, notDetecteds);
		logger.info("Sorted " + bySymbol.size() + " alleles by systems");
	}
	
	// helper method for fillSystemSorts()
	public Map<String,Integer> buildSortMap(Map<String,Integer> bySymbol, Map<String,StringBuffer> systems) {
		// compile a list of allele data that we can sort
		List<SortableAllele> sortableAlleles = new ArrayList<SortableAllele>();
		for (String alleleKey : bySymbol.keySet()) {
			SortableAllele allele = new SortableAllele();
			allele.alleleKey = alleleKey;
			allele.bySymbol = bySymbol.get(alleleKey);
			if (systems.containsKey(alleleKey)) {
				allele.systems = systems.get(alleleKey).toString();
			}
			sortableAlleles.add(allele);
		}

		// sort them
		if (sortableAlleles.size() > 0) {
			Collections.sort(sortableAlleles, sortableAlleles.get(0).getComparator());
		}
		
		// assign a sequence number to each
		Map<String,Integer> detectedSorts = new HashMap<String,Integer>();
		int i = 0;
		for (SortableAllele allele : sortableAlleles) {
			detectedSorts.put(allele.alleleKey, i++);
		}
		return detectedSorts;
	}

	/**
	 * Helper classes
	 */

	private class SortableAllele {
		public String alleleKey;
		public int bySymbol = -1;
		public String systems;
		
		public SortableAlleleComparator getComparator() {
			return new SortableAlleleComparator();
		}
		
		private class SortableAlleleComparator implements Comparator<SortableAllele> {
			@Override
			public int compare(SortableAllele a, SortableAllele b) {
				boolean aEmpty = (a.systems == null) || (a.systems.trim().length() == 0);
				boolean bEmpty = (b.systems == null) || (b.systems.trim().length() == 0);

				// if both have systems, compare them
				if (!aEmpty && !bEmpty) {
					int bySystems = a.systems.compareTo(b.systems);
					if (bySystems != 0) {
						return bySystems;
					} 
				} else if (aEmpty && !bEmpty) {
					return 1;	
				} else if (!aEmpty && bEmpty) {
					return -1;	
				} 

				// if both are missing systems or if the systems match, fall back on symbol comparison
				return Integer.compare(a.bySymbol, b.bySymbol);
			}
		}
	}
	
	private class AlleleSorts {
		public  int strainCount;
		public int referenceCount;
		public int byAlleleType;
		public int bySymbol;
		public int byDriver;
	}

	/*
	 * holds all the detected & not detected systems for an allele
	 */
	private class AlleleSystems {
		Set<String> detectedSystems = new HashSet<String>();
		Set<String> notDetectedSystems = new HashSet<String>();
	}

	private class CreAlleleSystem {

		public int alleleSystemKey;
		public String system;

	}

	/*
	 * What type of result
	 * 	Allele or assay result
	 */
	private enum ResultType {
		ALLELE, ASSAY_RESULT
	}
}
