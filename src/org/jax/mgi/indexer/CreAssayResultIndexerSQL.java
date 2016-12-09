package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
	// map of *all* systems for the allele
	Map<String, AlleleSystems> alleleSystemsMap;
	// map of systems for a specific assay result
	Map<String, List<CreAlleleSystem>> systemMap;

	public CreAssayResultIndexerSQL () {
		super("index.url.creAssayResult");
	}


	/*
	 * Indexes both assay result documents 
	 * 	and the documents for alleles with no assay results (i.e. stubs)
	 */
	public void index() throws Exception
	{   

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
					+ "struct.term_key as structure_key, "
					+ "struct.primary_id as structure_id, "
					+ "ras.allele_id, "
					+ "ras.allele_key, "
					+ "a.driver, "
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
					+ "join term struct on "
					+ 	"struct.vocab_name='EMAPA' "
					+ 	"and struct.term=rar.structure "
					+ "where rar.result_key > " + startResultKey + " and rar.result_key <= " + endResultKey
					+ " order by rar.result_key ";

			logger.info(assayResultQuery);
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
				+ "a.inducible_note "
				+ "from allele a "
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
				String structureKey = rs.getString("structure_key");
				String structureId = rs.getString("structure_id");

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

	/**
	 * Helper classes
	 */

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
