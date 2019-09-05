package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.MarkerMPCache;
import org.jax.mgi.shr.MarkerTypeCache;
import org.jax.mgi.shr.MarkerDOCache;
import org.jax.mgi.shr.MarkerGOCache;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;
import org.jax.mgi.shr.fe.query.SolrLocationTranslator;

/**
 * GXDResultIndexerSQL
 *
 * @author kstone This index is has the primary responsibility of populating the
 *         GXD Result solr index. Each document in this index represents an
 *         assay result. This index can/will have fields to group by assayKey
 *         and markerKey
 *
 *         Note: refactored during 5.x development
 */

public class GXDResultIndexerSQL extends Indexer {
	// detected values that should be mapped to "yes"
	public static List<String> detectedYesLevels = Arrays.asList("Present", "Trace", "Weak", "Moderate", "Strong", "Very strong");

	// how many Solr documents are kept in memory before being sent to Solr?
	public int solrCacheSize = 1200;
	
	// number of classical expression results considered for sorting
	public int countOfClassicalResults = 0;

	// caches of genotype data (key is genotype key)
	public Map<String, String> allelePairs = null;
	public Map<String, String> bgStrains = null;

	// caches of structure data (key is annotated structure key)
	public Map<String, String> structureID = null;
	public Map<String, String> emapaID = null;
	public Map<String, String> printname = null;

	// caches of marker data (key is marker key)
	public Map<String, String> markerSymbol = null;
	public Map<String, String> markerID = null;
	public Map<String, String> markerName = null;
	public Map<String, String> markerSubtype = null;
	public Map<String, String> markerBySymbol = null;
	public Map<String, String> markerByLocation = null;
	public Map<String, String> startCoord = null;
	public Map<String, String> endCoord = null;
	public Map<String, String> cytoband = null;
	public Map<String, String> strand = null;
	public Map<String, String> chromosome = null;
	public MarkerMPCache markerMpCache = null;
	public MarkerGOCache markerGoCache = null;
	public MarkerDOCache markerDoCache = null;
	public MarkerTypeCache markerTypeCache = null;
	
	// caches of reference data (key is reference key)
	public Map<String, String> pubmedID = null;
	public Map<String, String> citation = null;
	
	// caches of assay data (key is assay key)
	public Map<String, String> assayHasImage = null;
	public Map<String, String> assayProbeKey = null;
	public Map<String, String> assayAntibodyKey = null;
	public Map<String, String> assayID = null;
	
	public GXDResultIndexerSQL() {
		super("gxdResult");
	}

	// cache data for assays for expression results > startKey and <= endKey
	public void cacheAssays (int startKey, int endKey, boolean forRnaSeq) throws SQLException {
		assayHasImage = new HashMap<String, String>();
		assayProbeKey = new HashMap<String, String>();
		assayAntibodyKey = new HashMap<String, String>();
		assayID = new HashMap<String, String>();

		String assayQuery = "select distinct a.assay_key, a.has_image, a.probe_key, a.antibody_key, "
			+ "  e.assay_id "
			+ "from expression_result_summary e, expression_assay a "
			+ "where e.result_key > " + startKey
			+ " and e.result_key <= " + endKey
			+ " and e.assay_key = a.assay_key ";
		
		// adjust the query if we need to work with RNA-Seq data
		if (forRnaSeq) {
			assayQuery = "select distinct cs.experiment_key as assay_key, 0 as has_image, null as probe_key, "
				+ " null as antibody_key, e.primary_id as assay_id "
				+ "from expression_ht_consolidated_sample cs, "
				+ "  expression_ht_experiment e "
				+ "where cs.experiment_key = e.experiment_key ";
		}
		
		ResultSet rs = ex.executeProto(assayQuery);
		while (rs.next()) {
			String assayKey = rs.getString("assay_key");
			assayHasImage.put(assayKey, rs.getString("has_image"));
			assayProbeKey.put(assayKey, rs.getString("probe_key"));
			assayAntibodyKey.put(assayKey, rs.getString("antibody_key"));
			assayID.put(assayKey, rs.getString("assay_id"));
		}
		rs.close();
		logger.info("Cached data for " + assayID.size() + " assays");
	}
	
	// cache data for structures for expression results > startKey and <= endKey
	public void cacheTerms (int startKey, int endKey, boolean forRnaSeq) throws SQLException {
		structureID = new HashMap<String, String>();
		emapaID = new HashMap<String, String>();
		printname = new HashMap<String, String>();

		String structureQuery = "select distinct e.structure_key, e.structure_printname, "
			+ "  structure.primary_id, emapa.primary_id as emapa_id "
			+ "from expression_result_summary e, term structure, "
			+ "  term_emap mapping, term emapa "
			+ "where e.result_key > " + startKey
			+ " and e.result_key <= " + endKey
			+ " and e.structure_key = structure.term_key "
			+ " and e.structure_key = mapping.term_key "
			+ " and mapping.emapa_term_key = emapa.term_key";
		
		if (forRnaSeq) {
			// adjust the query to deal with RNA-Seq data rather than classical expression data
			structureQuery = "select distinct emaps.term_key as structure_key, "
				+ "  emapa.term as structure_printname, "
				+ "  emaps.primary_id, emapa.primary_id as emapa_id "
				+ "from  expression_ht_consolidated_sample cs, "
				+ "  term emapa, term_emap mapping, term emaps "
				+ "where cs.emapa_key = emapa.term_key "
				+ "  and emapa.term_key = mapping.emapa_term_key "
				+ "  and cs.theiler_stage::integer = mapping.stage "
				+ "  and mapping.term_key = emaps.term_key";
		}
		
		ResultSet rs = ex.executeProto(structureQuery);
		while (rs.next()) {
			String structureKey = rs.getString("structure_key");
			structureID.put(structureKey, rs.getString("primary_id"));
			emapaID.put(structureKey, rs.getString("emapa_id"));
			printname.put(structureKey, rs.getString("structure_printname"));
		}
		rs.close();
		logger.info("Cached data for " + emapaID.size() + " structures");
	}
	
	// cache data for genotypes for expression results > startKey and <= endKey
	public void cacheGenotypes (int startKey, int endKey, boolean forRnaSeq) throws SQLException {
		allelePairs = new HashMap<String, String>();
		bgStrains = new HashMap<String, String>();

		String genotypeQuery = "select distinct g.genotype_key, g.combination_1 "
			+ "from expression_result_summary e, genotype g "
			+ "where e.result_key > " + startKey
			+ " and e.result_key <= " + endKey
			+ " and e.genotype_key = g.genotype_key";
		
		if (forRnaSeq) {
			// adjust the query if we need to be working with RNA-Seq data rather than classical
			genotypeQuery = "select distinct g.genotype_key, g.combination_1, g.background_strain "
				+ "from expression_ht_consolidated_sample cs, "
				+ " genotype g "
				+ "where cs.genotype_key = g.genotype_key";
		}
		
		ResultSet rs = ex.executeProto(genotypeQuery);
		while (rs.next()) {
			allelePairs.put(rs.getString("genotype_key"), rs.getString("combination_1"));
			bgStrains.put(rs.getString("genotype_key"), rs.getString("background_strain"));
		}
		rs.close();
		logger.info("Cached data for " + allelePairs.size() + " genotypes");
	}
	
	// cache data for markers for expression results > startKey and <= endKey
	public void cacheMarkers (int startKey, int endKey, boolean forRnaSeq) throws SQLException {
		markerSymbol = new HashMap<String, String>();
		markerID = new HashMap<String, String>();
		markerName = new HashMap<String, String>();
		markerSubtype = new HashMap<String, String>();
		markerBySymbol = new HashMap<String, String>();
		markerByLocation = new HashMap<String, String>();
		startCoord = new HashMap<String, String>();
		endCoord = new HashMap<String, String>();
		cytoband = new HashMap<String, String>();
		strand = new HashMap<String, String>();
		chromosome = new HashMap<String, String>();
		
		String markerQuery = "select distinct m.marker_key, m.symbol, m.primary_id, m.name, m.marker_subtype, "
			+ " s.by_location, s.by_symbol, loc.chromosome, loc.cytogenetic_offset, loc.start_coordinate, "
			+ " loc.end_coordinate, loc.strand "
			+ "from expression_result_summary e, marker m, marker_sequence_num s, marker_location loc "
			+ "where e.result_key > " + startKey
			+ " and e.result_key <= " + endKey
			+ " and e.marker_key = s.marker_key"
			+ " and e.marker_key = loc.marker_key"
			+ " and loc.sequence_num = 1"
			+ " and e.marker_key = m.marker_key";

		if (forRnaSeq) {
			// adjust the query if we need to be working with RNA-Seq data rather than classical
			markerQuery = "select distinct m.marker_key, m.symbol, m.primary_id, m.name, m.marker_subtype, "
				+ " s.by_location, s.by_symbol, loc.chromosome, loc.cytogenetic_offset, loc.start_coordinate, "
				+ " loc.end_coordinate, loc.strand "
				+ "from marker m, marker_sequence_num s, "
				+ " marker_location loc "
				+ "where m.organism = 'mouse' "
				+ " and m.marker_key = loc.marker_key "
				+ " and loc.sequence_num = 1 "
				+ " and s.marker_key = m.marker_key";
		}
		
		ResultSet rs = ex.executeProto(markerQuery);
		while (rs.next()) {
			String markerKey = rs.getString("marker_key");
			markerSymbol.put(markerKey, rs.getString("symbol"));
			markerID.put(markerKey, rs.getString("primary_id"));
			markerName.put(markerKey, rs.getString("name"));
			markerSubtype.put(markerKey, rs.getString("marker_subtype"));
			markerByLocation.put(markerKey, rs.getString("by_location"));
			markerBySymbol.put(markerKey, rs.getString("by_symbol"));
			startCoord.put(markerKey, rs.getString("start_coordinate"));
			endCoord.put(markerKey, rs.getString("end_coordinate"));
			cytoband.put(markerKey, rs.getString("cytogenetic_offset"));
			strand.put(markerKey, rs.getString("strand"));
			chromosome.put(markerKey, rs.getString("chromosome"));
		}
		rs.close();
		logger.info("Cached data for " + markerID.size() + " markers");
	}
	
	// cache data for references for expression results > startKey and <= endKey
	public void cacheReferences (int startKey, int endKey) throws SQLException {
		pubmedID = new HashMap<String, String>();
		citation = new HashMap<String, String>();

		String referenceQuery = "select distinct r.reference_key, r.pubmed_id, r.mini_citation "
			+ "from expression_result_summary e, reference r "
			+ "where e.result_key > " + startKey
			+ " and e.result_key <= " + endKey
			+ " and e.reference_key = r.reference_key";
		
		ResultSet rs = ex.executeProto(referenceQuery);
		while (rs.next()) {
			String referenceKey = rs.getString("reference_key");
			pubmedID.put(referenceKey, rs.getString("pubmed_id"));
			citation.put(referenceKey, rs.getString("mini_citation"));
		}
		rs.close();
		logger.info("Cached data for " + pubmedID.size() + " references");
	}
	
	// join non-null strings s1, s2, and s3 together, separated by underscores
	public String joiner (String s1, String s2, String s3) {
		return joiner(s1, s2, s3, null);
	}
	
	// join non-null strings s1, s2, s3, and (nullable) s4 together, separated by underscores.
	public String joiner (String s1, String s2, String s3, String s4) {
		StringBuffer sb = new StringBuffer();
		sb.append(s1);
		sb.append("_");
		sb.append(s2);
		sb.append("_");
		sb.append(s3);
		if (s4 != null) {
			sb.append("_");
			sb.append(s4);
		}
		return sb.toString();
	}
	
	/* build a temp table of all anatomical system IDs (high level EMAPA terms)
	 * and index it for performance
	 */
	private void identifySystemIDs() throws SQLException {
		String cmd1 = "select distinct emapa_id "
			+ "into temp table tmp_anatomical_systems "
			+ "from expression_result_anatomical_systems";
		String cmd2 = "create index tasIndex1 on tmp_anatomical_systems(emapa_id)";
		String cmd3 = "select count(1) as term_count from tmp_anatomical_systems";

		ex.executeVoid(cmd1);
		ex.executeVoid(cmd2);
		ResultSet rs = ex.executeProto(cmd3);
		if (rs.next()) {
			logger.info("Build temp table of " + rs.getInt("term_count") + " anatomical systems");
		}
		rs.close();
	}

	/* get a mapping from result keys (as Strings) to a List of Strings,
	 * each of which is a high-level EMAPA term (a high-level ancestor of
	 * the structure noted in the result.  Returns for result keys > startKey and <= endKey.
	 */
	private Map<String, Set<String>> getAnatomicalSystemMap(int startKey, int endKey, boolean forRnaSeq) throws Exception {
		logger.info ("building map of high-level EMAPA terms for results " + startKey + ".." + endKey);
		
		Map<String, Set<String>> systemMap = new HashMap<String, Set<String>>();

		String systemQuery = "select result_key, anatomical_system, emapa_id from expression_result_anatomical_systems"
			+ " where result_key > " + startKey + " and result_key <= " + endKey;

		if (forRnaSeq) {
			// adjust the query when dealing with RNA-Seq data
			systemQuery = "select distinct sm.consolidated_measurement_key as result_key, "
				+ "  ta.ancestor_term as anatomical_system, "
				+ "  ta.ancestor_primary_id as emapa_id "
				+ "from expression_ht_consolidated_sample_measurement sm, "
				+ " expression_ht_consolidated_sample cs, "
				+ " term_ancestor ta, tmp_anatomical_systems tas "
				+ "where sm.consolidated_measurement_key >= " + startKey
				+ " and sm.consolidated_measurement_key <= " + endKey
				+ " and sm.consolidated_sample_key = cs.consolidated_sample_key "
				+ " and cs.emapa_key = ta.term_key "
				+ " and ta.ancestor_primary_id = tas.emapa_id";
		}
		
		ResultSet rs = ex.executeProto(systemQuery);

		while (rs.next()) {
			String resultKey = "rnaseq" + rs.getString("result_key");
			String system = rs.getString("anatomical_system") + "_" + rs.getString("emapa_id");

			if (!systemMap.containsKey(resultKey)) {
				systemMap.put(resultKey, new HashSet<String>());
			}
			systemMap.get(resultKey).add(system);
		}
		logger.info(" - gathered EMAPA terms for " + systemMap.size() + " results, RAM used: " + memoryUsed());
		rs.close();
		return systemMap;
	}

	/*
	 * get a mapping from marker keys (as Strings) to a List of Strings, each of
	 * which is a synonym for the marker -- where those markers also have
	 * expression results
	 */
	private Map<String, List<String>> getMarkerNomenMap() throws Exception {
		Map<String, List<String>> markerNomenMap = new HashMap<String, List<String>>();

		logger.info("building map of marker searchable nomenclature");
		String nomenQuery = "select distinct marker_key, term "
				+ "from marker_searchable_nomenclature msn "
				+ "where term_type in ('synonym','related synonym') "
				+ "and (exists (select 1 from expression_result_summary ers "
				+ "  where msn.marker_key = ers.marker_key) "
				+ "or exists (select 1 from expression_ht_consolidated_sample_measurement sm "
				+ "  where msn.marker_key = sm.marker_key) )";
		ResultSet rs = ex.executeProto(nomenQuery);

		String mkey; // marker key
		String term; // synonym

		while (rs.next()) {
			mkey = rs.getString("marker_key");
			term = rs.getString("term");

			if (!markerNomenMap.containsKey(mkey)) {
				markerNomenMap.put(mkey, new ArrayList<String>());
			}
			markerNomenMap.get(mkey).add(term);
		}
		logger.info(" - gathered synonyms for " + markerNomenMap.size() + " markers, RAM used: " + memoryUsed());
		rs.close();

		return markerNomenMap;
	}

	/*
	 * get a mapping from marker keys (as Strings) to their corresponding cM
	 * offsets, also as Strings -- where those markers also have expression
	 * results.
	 */
	private Map<String, String> getCentimorganMap() throws Exception {
		Map<String, String> centimorganMap = new HashMap<String, String>();

		logger.info("building map of marker centimorgans");
		String centimorganQuery = "select distinct marker_key, cm_offset "
				+ "from marker_location ml "
				+ "where location_type='centimorgans' "
				+ "and (exists (select 1 from expression_result_summary ers "
				+ "  where ml.marker_key = ers.marker_key)"
				+ "or exists (select 1 from expression_ht_consolidated_sample_measurement sm "
				+ "  where ml.marker_key = sm.marker_key) )";
		ResultSet rs = ex.executeProto(centimorganQuery);

		String mkey; // marker key
		String cm_offset; // centimorgan offset for the marker

		while (rs.next()) {
			mkey = rs.getString("marker_key");
			cm_offset = rs.getString("cm_offset");

			centimorganMap.put(mkey, cm_offset);
		}
		rs.close();
		logger.info(" - gathered cM for " + centimorganMap.size() + " markers, RAM used: " + memoryUsed());
		return centimorganMap;
	}

	/*
	 * get a mapping from genotype keys (as Strings) to data about markers
	 * mutated in those genotypes. Mapping returned is like: { genotype key : {
	 * marker key : { "symbol" : symbol, "name" : name } } } The mapping only
	 * includes genotypes tied to expression results.
	 */
	private Map<String, Map<String, Map<String, String>>> getMutatedInMap() throws Exception {

		// maps from genotype key (as a String) to a map of marker data like:
		// { marker key : { "symbol" : symbol,
		// "name" : name } }
		Map<String, Map<String, Map<String, String>>> mutatedInMap = new HashMap<String, Map<String, Map<String, String>>>();

		logger.info("building map of specimen mutated in genes");
		String mutatedInQuery = "select m.marker_key, " + "  m.symbol, "
				+ "  m.name, " + "  ag.genotype_key " + "from marker m, "
				+ "  marker_to_allele ma, " + "  allele_to_genotype ag "
				+ "where ag.allele_key = ma.allele_key "
				+ "  and ma.marker_key = m.marker_key"
				+ "  and (exists (select 1 from expression_result_summary ers "
				+ "    where ag.genotype_key = ers.genotype_key)"
				+ "  or exists (select 1 from expression_ht_consolidated_sample_measurement sm "
				+ "    where m.marker_key = sm.marker_key) )";
		ResultSet rs = ex.executeProto(mutatedInQuery);

		String gkey; // genotype key
		String mkey; // marker key
		String symbol; // marker symbol
		String name; // marker name

		// maps from marker key to map { "symbol" : symbol, "name" : name }
		Map<String, Map<String, String>> genotype;

		while (rs.next()) {
			gkey = rs.getString("genotype_key");
			mkey = rs.getString("marker_key");
			symbol = rs.getString("symbol");
			name = rs.getString("name");

			// if we haven't seen this genotype before, we need to add it
			// with its corresponding marker mapping

			if (!mutatedInMap.containsKey(gkey)) {
				genotype = new HashMap<String, Map<String, String>>();
				genotype.put(mkey, new HashMap<String, String>());
				mutatedInMap.put(gkey, genotype);
			}

			// if we've seen the genotype, but haven't seen the marker, then
			// add the marker to the genotype

			if (!mutatedInMap.get(gkey).containsKey(mkey)) {
				mutatedInMap.get(gkey).put(mkey, new HashMap<String, String>());
			}

			// store the symbol and name for the marker

			mutatedInMap.get(gkey).get(mkey).put("symbol", symbol);
			mutatedInMap.get(gkey).get(mkey).put("name", name);
		}
		rs.close();
		logger.info(" - gathered markers for " + mutatedInMap.size() + " genotypes, RAM used: " + memoryUsed());

		return mutatedInMap;
	}

	/*
	 * get a mapping from genotype keys (as Strings) to a List of IDs for
	 * alleles in that genotype. Only includes genotypes with allele data.
	 */
	private Map<String, List<String>> getMutatedInAlleleMap() throws Exception {

		Map<String, List<String>> mutatedInAlleleMap = new HashMap<String, List<String>>();

		logger.info("building map of specimen mutated in allele IDs");

		String mutatedInAlleleQuery = "select distinct a.primary_id acc_id, "
				+ "  ag.genotype_key " + "from allele a, "
				+ "  allele_to_genotype ag "
				+ "where ag.allele_key = a.allele_key "
				+ "  and (exists (select 1 from expression_result_summary ers "
				+ "    where ag.genotype_key = ers.genotype_key)"
				+ "  or exists (select 1 from expression_ht_consolidated_sample cs "
				+ "    where ag.genotype_key = cs.genotype_key) )";

		ResultSet rs = ex.executeProto(mutatedInAlleleQuery);

		String gkey; // genotype key
		String alleleId; // allele ID

		while (rs.next()) {
			gkey = rs.getString("genotype_key");
			alleleId = rs.getString("acc_id");

			// if we've not seen this genotype before, add it with an empty
			// list of allele IDs

			if (!mutatedInAlleleMap.containsKey(gkey)) {
				mutatedInAlleleMap.put(gkey, new ArrayList<String>());
			}

			// add the allele ID to the list for this genotype
			mutatedInAlleleMap.get(gkey).add(alleleId);
		}
		logger.info(" - gathered alleles for " + mutatedInAlleleMap.size()
				+ " genotypes, RAM used: " + memoryUsed());
		rs.close();

		return mutatedInAlleleMap;
	}

	/*
	 * get a mapping from marker keys (as Strings) to IDs of other (non-anatomy)
	 * vocabulary terms annotated to those markers. Each marker key refers to a
	 * List of IDs.
	 */
	private Map<String, List<String>> getMarkerVocabMap() throws Exception {
		
		HashMap<String, String> allTermIdBuffer = new HashMap<String, String>();
		
		Map<String, List<String>> markerVocabMap = new HashMap<String, List<String>>();

		logger.info("building map of vocabulary annotations");
		String vocabQuery = SharedQueries.GXD_VOCAB_EXPRESSION_QUERY;
		ResultSet rs = ex.executeProto(vocabQuery);

		String mkey; // marker key
		String termId; // term ID

		while (rs.next()) {
			mkey = rs.getString("marker_key");
			termId = rs.getString("term_id");

			if (!markerVocabMap.containsKey(mkey)) {
				markerVocabMap.put(mkey, new ArrayList<String>());
			}
			if(!allTermIdBuffer.containsKey(termId)) {
				allTermIdBuffer.put(termId, termId);
			}
			markerVocabMap.get(mkey).add(allTermIdBuffer.get(termId));
		}
		rs.close();
		logger.info(" - gathered annotated terms for " + markerVocabMap.size() + " markers, RAM used: " + memoryUsed());

		// add extra data for DO terms associated to human markers
		// which are associated with mouse markers via homology

		ResultSet rs2 = ex.executeProto(SharedQueries.GXD_DO_HOMOLOGY_QUERY);
		int i = 0;

		while (rs2.next()) {
			mkey = rs2.getString("marker_key");
			termId = rs2.getString("term_id");

			if (!markerVocabMap.containsKey(mkey)) {
				markerVocabMap.put(mkey, new ArrayList<String>());
			}
			if(!allTermIdBuffer.containsKey(termId)) {
				allTermIdBuffer.put(termId, termId);
			}
			if (!markerVocabMap.get(mkey).contains(termId)) {
				markerVocabMap.get(mkey).add(allTermIdBuffer.get(termId));
				i++;
			}
		}

		logger.info(" - added " + i + " annotations to DO via homology, RAM used: " + memoryUsed());
		allTermIdBuffer.clear();
		rs2.close();
		return markerVocabMap;
	}

	/*
	 * get a mapping from each term ID to a Set of IDs for its ancestor terms,
	 * for terms in non-anatomy vocabularies which are annotated to markers.
	 */
	private Map<String, Set<String>> getVocabAncestorMap() throws Exception {
		Map<String, Set<String>> vocabAncestorMap = new HashMap<String, Set<String>>();

		logger.info("building map of vocabulary term ancestors");

		String vocabAncestorQuery = SharedQueries.GXD_VOCAB_ANCESTOR_QUERY;
		String termId; // term's ID
		ResultSet rs = ex.executeProto(vocabAncestorQuery);

		while (rs.next()) {
			termId = rs.getString("primary_id");

			if (!vocabAncestorMap.containsKey(termId)) {
				vocabAncestorMap.put(termId, new HashSet<String>());
			}
			vocabAncestorMap.get(termId).add(rs.getString("ancestor_primary_id"));
		}
		logger.info(" - gathered ancestor IDs for " + vocabAncestorMap.size() + " terms, RAM used: " + memoryUsed());
		rs.close();
		return vocabAncestorMap;
	}

	/*
	 * get a mapping from expression result key (as a String) to a List of
	 * figure labels for that result.  Returns results for result keys > startKey and <= endKey.
	 */
	private Map<String, Set<String>> getImageMap(int startKey, int endKey) throws Exception {
		Map<String, Set<String>> imageMap = new HashMap<String, Set<String>>();

		logger.info("building map of expression images for results " + startKey + ".." + endKey);

		// label could be either specimen label or if null use the figure label
		String imageQuery = "select eri.result_key, "
				+ "  case when ei.pane_label is null then i.figure_label "
				+ "    else (i.figure_label || ei.pane_label) end as label "
				+ "from expression_result_summary ers, "
				+ "  expression_result_to_imagepane eri, "
				+ "  expression_imagepane ei, " + "  image i "
				+ "where eri.imagepane_key = ei.imagepane_key "
				+ "  and eri.result_key > " + startKey
				+ "  and eri.result_key <= " + endKey
				+ "  and ei.image_key = i.image_key "
				+ "  and eri.result_key = ers.result_key "
				+ "  and ers.specimen_key is null " + "UNION "
				+ "select ers.result_key, sp.specimen_label as label "
				+ "from expression_result_summary ers, "
				+ "  assay_specimen sp "
				+ "where ers.specimen_key = sp.specimen_key "
				+ "  and ers.result_key > " + startKey
				+ "  and ers.result_key <= " + endKey;

		ResultSet rs = ex.executeProto(imageQuery);

		String rkey; // result key
		String label; // specimen label

		while (rs.next()) {
			rkey = rs.getString("result_key");
			label = rs.getString("label");

			// skip empty labels
			if (label != null && !label.equals("")) {
				if (!imageMap.containsKey(rkey)) {
					imageMap.put(rkey, new HashSet<String>());
				}
				imageMap.get(rkey).add(label);
			}
		}
		logger.info(" - gathered figure labels for " + imageMap.size() + " results, RAM used: " + memoryUsed());
		rs.close();
		return imageMap;
	}

	/*
	 * build a mapping from a string (field specified by 'key') to a List of
	 * String values (field specified by 'value1'). If 'value2' is specified
	 * then we also include the value of the field with that name the first time
	 * we find each 'key'. 'msg' specifies the type of items we are gathering,
	 * only used for debugging output.
	 */
	private Map<String, List<String>> getMap(String query, String key, String value1, String value2, String msg) throws Exception {

		HashMap<String, String> allValuesBuffer = new HashMap<String, String>();
		
		Map<String, List<String>> structureAncestorMap = new HashMap<String, List<String>>();

		logger.info("building map of " + msg + " for structures");

		ResultSet rs = ex.executeProto(query);

		String sKey; // value of structure key
		String sValue1; // primary value to collect
		String sValue2; // primary value to collect

		while (rs.next()) {
			sKey = rs.getString(key);
			sValue1 = rs.getString(value1);

			if(!allValuesBuffer.containsKey(sValue1)) {
				allValuesBuffer.put(sValue1, sValue1);
			}
			
			if (!structureAncestorMap.containsKey(sKey)) {
				structureAncestorMap.put(sKey, new ArrayList<String>());

				// add value2 the first time this key is found, if defined
				if (value2 != null) {
					sValue2 = rs.getString(value2);
					if(!allValuesBuffer.containsKey(sValue2)) {
						allValuesBuffer.put(sValue2, sValue2);
					}
					structureAncestorMap.get(sKey).add(allValuesBuffer.get(sValue2));
				}
			}

			if ((sValue1 != null) && (!sValue1.equals(""))) {
				structureAncestorMap.get(sKey).add(allValuesBuffer.get(sValue1));
			}
		}
		logger.info(" - gathered " + msg + " for " + structureAncestorMap.size() + " terms, RAM used: " + memoryUsed());
		allValuesBuffer.clear();
		rs.close();
		return structureAncestorMap;
	}

	/*
	 * -------------------- main indexing method --------------------
	 */
	public void index() throws Exception {
		// get the maximum sequence number assigned for classical
		// expression results, so we know that RNA-Seq results come
		// after classical results

		String cmd = "select max(by_assay_type) as max_seqnum "
			+ "from expression_result_sequence_num";

		ResultSet rs = ex.executeProto(cmd);
		if (rs.next()) {
			countOfClassicalResults = rs.getInt("max_seqnum");
		} else {
			countOfClassicalResults = 0;
		}
		rs.close();
		logger.info(countOfClassicalResults + " sequence numbers assigned for classical data");

		// pull a bunch of mappings into memory, to make later
		// processing easier

		try {
			markerMpCache = new MarkerMPCache();
		} catch (Exception e) {
			logger.error("Marker/MP Cache failed; no MP filtering terms will be indexed.");
		}

		try {
			markerGoCache = new MarkerGOCache();
		} catch (Exception e) {
			logger.error("Marker/GO Cache failed; no GO filtering terms will be indexed.");
		}

		try {
			markerDoCache = new MarkerDOCache();
		} catch (Exception e) {
			logger.error("Marker/DO Cache failed; no DO filtering terms will be indexed.");
		}

		try {
			markerTypeCache = new MarkerTypeCache();
		} catch (Exception e) {
			logger.error("Marker/Type Cache failed; no Feature Type filtering terms will be indexed.");
		}

		// mapping from marker key to List of synonyms for each marker
		Map<String, List<String>> markerNomenMap = getMarkerNomenMap();

		// mapping from marker key to its cM location, if available
		Map<String, String> centimorganMap = getCentimorganMap();
		
		// get markers mutated in each genotype
		Map<String, Map<String, Map<String, String>>> mutatedInMap = getMutatedInMap();

		// get IDs of alleles in each genotype
		Map<String, List<String>> mutatedInAlleleMap = getMutatedInAlleleMap();

		// get IDs of non-anatomy terms annotated to markers
		Map<String, List<String>> markerVocabMap = getMarkerVocabMap();

		// get List of ancestor term IDs for each non-anatomy term
		Map<String, Set<String>> vocabAncestorMap = getVocabAncestorMap();

		// get List of ancestor IDs for each structure
		Map<String, List<String>> structureAncestorIdMap = getMap(
				SharedQueries.GXD_EMAP_ANCESTOR_QUERY, "structure_term_key", "ancestor_id", "structure_id", "IDs");

		// get List of ancestor keys for each structure
		Map<String, List<String>> structureAncestorKeyMap = getMap(
				SharedQueries.GXD_EMAP_ANCESTOR_QUERY, "structure_term_key", "default_parent_key", null, "keys");

		// get List of synonyms for each structure
		Map<String, List<String>> structureSynonymMap = getMap(
				SharedQueries.GXD_EMAP_SYNONYMS_QUERY, "structure_id", "synonym", "structure", "synonyms");

		// -------------------------------------------------------------------
		// Finally finished gathering mappings, time for the main body of work
		// -------------------------------------------------------------------

		identifySystemIDs();
		indexRnaSeqData(markerNomenMap, centimorganMap, mutatedInMap, mutatedInAlleleMap,
			markerVocabMap, vocabAncestorMap, structureAncestorIdMap, structureAncestorKeyMap,
			structureSynonymMap);
		indexClassicalData(markerNomenMap, centimorganMap, mutatedInMap, mutatedInAlleleMap,
			markerVocabMap, vocabAncestorMap, structureAncestorIdMap, structureAncestorKeyMap,
			structureSynonymMap);
		this.setSkipOptimizer(true);
	}
		
	// index classical expression data (not RNA-Seq data)
	public void	indexClassicalData(
			Map<String, List<String>> markerNomenMap,
			Map<String, String> centimorganMap,
			Map<String, Map<String, Map<String, String>>> mutatedInMap,
			Map<String, List<String>> mutatedInAlleleMap,
			Map<String, List<String>> markerVocabMap,
			Map<String, Set<String>> vocabAncestorMap, 
			Map<String, List<String>> structureAncestorIdMap,
			Map<String, List<String>> structureAncestorKeyMap,
			Map<String, List<String>> structureSynonymMap) throws Exception {

		// find the maximum result key, so we have an upper bound when
		// stepping through chunks of results

		ResultSet rs_tmp = ex.executeProto("select max(result_key) as max_result_key from expression_result_summary");
		rs_tmp.next();

		Integer start = 0;
		Integer end = rs_tmp.getInt("max_result_key");
		rs_tmp.close();
		int chunkSize = 50000;

		// While it appears that modValue could be one iteration too low (due
		// to rounding down), this is accounted for by using <= in the loop.

		int modValue = end.intValue() / chunkSize;

		// Perform the chunking

		logger.info("Getting all assay results and related search criteria");
		logger.info("Max result_key: " + end + ", chunks: " + (modValue + 1));

		// can set the size to our known max (slight efficiency gain)
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(1001);
		
		for (int i = 0; i <= modValue; i++) {

			start = i * chunkSize;
			end = start + chunkSize;

			cacheGenotypes(start, end, false);		// cache allele combinations for genotypes for this chunk
			cacheMarkers(start, end, false);		// cache marker symbols, names, IDs, and subtypes for this chunk
			cacheReferences(start, end);			// cache pubmed IDs and citations for references for this chunk
			cacheAssays(start, end, false);			// cache data for assays in this chunk
			cacheTerms(start, end, false);			// cache data for structures in this chunk
			
			// mapping from result key to List of high-level EMAPA structures for each result
			Map<String, Set<String>> systemMap = getAnatomicalSystemMap(start, end, false);

			// get List of figure labels for each expression result key
			Map<String, Set<String>> imageMap = getImageMap(start, end);

			logger.info("Processing result key > " + start + " and <= " + end + ", RAM used: " + memoryUsed());
			String query = "select ers.result_key, "
					+ "  ers.marker_key, ers.assay_key, ers.assay_type, "
					+ "  ers.structure_key, ers.theiler_stage, ers.is_expressed, ers.has_image, " 
					+ "  ers.age_abbreviation,  ers.jnum_id, ers.detection_level, "
					+ "  ers.age_min, ers.age_max, ers.pattern, emaps.primary_id as emaps_id, "
					+ "  ers.is_wild_type, ers.genotype_key, ers.reference_key, " 
					+ "  ersn.by_assay_type r_by_assay_type, "
					+ "  ersn.by_gene_symbol r_by_gene_symbol, "
					+ "  ersn.by_age r_by_age, "
					+ "  ersn.by_expressed r_by_expressed, "
					+ "  ersn.by_structure r_by_structure, "
					+ "  ersn.by_mutant_alleles r_by_mutant_alleles, "
					+ "  ersn.by_reference r_by_reference "
					+ "from expression_result_summary ers, "
					+ "  marker_counts mc, term emaps, "
					+ "  expression_result_sequence_num ersn "
					+ "where ers.marker_key=mc.marker_key "
					+ "  and ersn.result_key=ers.result_key "
					+ "  and ers.structure_key = emaps.term_key "
					+ "  and ers.assay_type != 'Recombinase reporter'"
					+ "  and ers.assay_type != 'In situ reporter (transgenic)'"
					+ "  and mc.gxd_literature_count>0 "
					+ "  and ers.result_key > " + start
					+ "  and ers.result_key <= " + end + " ";

			ResultSet rs = ex.executeProto(query);

			while (rs.next()) {
				String markerKey = rs.getString("marker_key");
				String result_key = rs.getString("result_key");
				String assay_key = rs.getString("assay_key");
				String assay_type = rs.getString("assay_type");

				// result fields
				String theilerStage = rs.getString("theiler_stage");
				String isExpressed = rs.getString("is_expressed");
				String structureTermKey = rs.getString("structure_key");
				String has_image = rs.getString("has_image");

				String chr = chromosome.get(markerKey);
				String cm_offset = "";
				if (centimorganMap.containsKey(markerKey)) {
					cm_offset = centimorganMap.get(markerKey);
				}
				String start_coord = startCoord.get(markerKey);
				String end_coord = endCoord.get(markerKey);
				String spatialString = new String("");
				if ((start_coord != null) && (end_coord != null)) {
					spatialString = SolrLocationTranslator.getIndexValue(
							chr,Long.parseLong(start_coord),Long.parseLong(end_coord),true);
				}

				String unique_key = assay_type + "-" + result_key;
				if (unique_key == null || unique_key.equals("-")) {
					continue;
				}

				SolrInputDocument doc = new SolrInputDocument();

				// Add the single value fields
				doc.addField(GxdResultFields.KEY, unique_key);
				doc.addField(GxdResultFields.MARKER_KEY, markerKey);
				doc.addField(IndexConstants.MRK_BY_SYMBOL, rs.getString("r_by_gene_symbol"));
				doc.addField(GxdResultFields.M_BY_LOCATION, markerByLocation.get(markerKey));
				doc.addField(GxdResultFields.ASSAY_KEY, assay_key);
				doc.addField(GxdResultFields.RESULT_KEY, result_key);
				doc.addField(GxdResultFields.RESULT_TYPE, assay_type);
				doc.addField(GxdResultFields.ASSAY_TYPE, assay_type);
				doc.addField(GxdResultFields.THEILER_STAGE, theilerStage);
				doc.addField(GxdResultFields.EMAPS_ID, rs.getString("emaps_id"));
				doc.addField(GxdResultFields.IS_EXPRESSED, isExpressed);
				doc.addField(GxdResultFields.AGE_MIN, roundAge(rs.getString("age_min")));
				doc.addField(GxdResultFields.AGE_MAX, roundAge(rs.getString("age_max")));

				boolean isWildType = rs.getString("is_wild_type").equals("1") || rs.getString("genotype_key").equals("-1");

				String wildType = "mutant";
				if (isWildType) {
					wildType = "wild type";
				}

				doc.addField(GxdResultFields.IS_WILD_TYPE, wildType);

				// marker summary
				doc.addField(GxdResultFields.MARKER_MGIID, markerID.get(markerKey));
				doc.addField(GxdResultFields.MARKER_SYMBOL, markerSymbol.get(markerKey));
				doc.addField(GxdResultFields.MARKER_NAME, markerName.get(markerKey));

				// also add symbol and current name to searchable nomenclature
				doc.addField(GxdResultFields.NOMENCLATURE, markerSymbol.get(markerKey));
				doc.addField(GxdResultFields.NOMENCLATURE, markerName.get(markerKey));
				doc.addField(GxdResultFields.MARKER_TYPE, markerSubtype.get(markerKey));

				// location stuff
				doc.addField(GxdResultFields.CHROMOSOME, chr);
				doc.addField(GxdResultFields.START_COORD, start_coord);
				doc.addField(GxdResultFields.END_COORD, end_coord);
				doc.addField(GxdResultFields.CYTOBAND, cytoband.get(markerKey));
				doc.addField(GxdResultFields.STRAND, strand.get(markerKey));
				if (!spatialString.equals("")) {
					doc.addField(GxdResultFields.MOUSE_COORDINATE, spatialString);
				}

				if (cm_offset == null || cm_offset.equals("-1"))
					cm_offset = "";
				doc.addField(GxdResultFields.CENTIMORGAN, cm_offset);

				// assay summary
				doc.addField(GxdResultFields.ASSAY_HAS_IMAGE, "1".equals(assayHasImage.get(assay_key)));
				doc.addField(GxdResultFields.PROBE_KEY, assayProbeKey.get(assay_key));
				doc.addField(GxdResultFields.ANTIBODY_KEY, assayAntibodyKey.get(assay_key));

				// assay sorts
				doc.addField(GxdResultFields.A_BY_SYMBOL, rs.getString("r_by_assay_type"));
				doc.addField(GxdResultFields.A_BY_ASSAY_TYPE, rs.getString("r_by_assay_type"));

				// result summary
				doc.addField(GxdResultFields.DETECTION_LEVEL, mapDetectionLevel(rs.getString("detection_level")) );
				doc.addField(GxdResultFields.STRUCTURE_PRINTNAME, printname.get(structureTermKey));
				doc.addField(GxdResultFields.AGE, rs.getString("age_abbreviation"));
				doc.addField(GxdResultFields.ASSAY_MGIID, assayID.get(assay_key));
				doc.addField(GxdResultFields.JNUM, rs.getString("jnum_id"));
				doc.addField(GxdResultFields.PUBMED_ID, pubmedID.get(rs.getString("reference_key")));
				doc.addField(GxdResultFields.SHORT_CITATION, citation.get(rs.getString("reference_key")));
				doc.addField(GxdResultFields.GENOTYPE, allelePairs.get(rs.getString("genotype_key")));
				doc.addField(GxdResultFields.PATTERN, rs.getString("pattern"));

				// add fields for filtering by marker-associated vocabularies
				for (String mpTerm : markerMpCache.getTerms(markerKey)) {
					doc.addField(GxdResultFields.MP_HEADERS, mpTerm);
				}

				for (String goTerm : markerGoCache.getTerms(markerKey)) {
					doc.addField(GxdResultFields.GO_HEADERS, goTerm);
				}

				for (String doTerm : markerDoCache.getTerms(markerKey)) {
					doc.addField(GxdResultFields.DO_HEADERS, doTerm);
				}

				for (String featureType : markerTypeCache.getTerms(markerKey)) {
					doc.addField(GxdResultFields.FEATURE_TYPES, featureType);
				}

				// multi values

				if (systemMap.containsKey(result_key)) {
					for (String system : systemMap.get(result_key)) {
						doc.addField(GxdResultFields.ANATOMICAL_SYSTEM, system);
					}
					systemMap.remove(result_key);
				}

				if (markerNomenMap.containsKey(markerKey)) {
					for (String nomen : markerNomenMap.get(markerKey)) {
						doc.addField(GxdResultFields.NOMENCLATURE, nomen);
					}
				}

				String genotype_key = rs.getString("genotype_key");
				if (mutatedInMap.containsKey(genotype_key)) {
					Map<String, Map<String, String>> gMap = mutatedInMap.get(genotype_key);
					for (String genotype_marker_key : gMap.keySet()) {
						doc.addField(GxdResultFields.MUTATED_IN, gMap.get(genotype_marker_key).get("symbol"));
						doc.addField(GxdResultFields.MUTATED_IN, gMap.get(genotype_marker_key).get("name"));

						// get any synonyms
						if (markerNomenMap.containsKey(genotype_marker_key)) {
							for (String synonym : markerNomenMap.get(genotype_marker_key)) {
								doc.addField(GxdResultFields.MUTATED_IN, synonym);
							}
						}
					}
				}

				if (mutatedInAlleleMap.containsKey(genotype_key)) {
					List<String> alleleIds = mutatedInAlleleMap.get(genotype_key);

					for (String alleleId : alleleIds) {
						doc.addField(GxdResultFields.ALLELE_ID, alleleId);
					}

				}

				if (markerVocabMap.containsKey(markerKey)) {
					Set<String> uniqueAnnotationIDs = new HashSet<String>();

					for (String termId : markerVocabMap.get(markerKey)) {
						uniqueAnnotationIDs.add(termId);
						if (vocabAncestorMap.containsKey(termId)) {
							for (String ancestorId : vocabAncestorMap.get(termId)) {
								uniqueAnnotationIDs.add(ancestorId);
							}
						}
					}

					for (String annotationID : uniqueAnnotationIDs) {
						doc.addField(GxdResultFields.ANNOTATION, annotationID);
					}
				}

				if (imageMap.containsKey(result_key)) {
					if (has_image.equals("1")) {
						for (String figure : imageMap.get(result_key)) {
							doc.addField(GxdResultFields.FIGURE, figure);
							doc.addField(GxdResultFields.FIGURE_PLAIN, figure);
						}
					}
					imageMap.remove(result_key);
				}

				String myEmapaID = emapaID.get(structureTermKey);

				Set<String> ancestorIDs = new HashSet<String>();
				ancestorIDs.add(structureID.get(structureTermKey));
				Set<String> ancestorStructures = new HashSet<String>();
				ancestorStructures.add(printname.get(structureTermKey));

				if (structureAncestorIdMap.containsKey(structureTermKey)) {
					// get ancestors
					List<String> structure_ancestor_ids = structureAncestorIdMap.get(structureTermKey);

					for (String structure_ancestor_id : structure_ancestor_ids) {
						// get synonyms for each ancestor/term

						if (structureSynonymMap.containsKey(structure_ancestor_id)) {

							// also add structure MGI ID
							ancestorIDs.add(structure_ancestor_id);
							for (String structureSynonym : structureSynonymMap.get(structure_ancestor_id)) {
								ancestorStructures.add(structureSynonym);
							}
						}
					}

					// only add unique structures (for best solr indexing
					// performance)
					for (String ancestorId : ancestorIDs) {
						doc.addField(GxdResultFields.STRUCTURE_ID, ancestorId);
					}
					for (String ancestorStructure : ancestorStructures) {
						doc.addField(GxdResultFields.STRUCTURE_ANCESTORS, ancestorStructure);
					}
				}
				
				// add the id for this exact structure
				doc.addField(GxdResultFields.STRUCTURE_EXACT, myEmapaID);

				Set<String> structureKeys = new HashSet<String>();
				structureKeys.add(structureTermKey);
				doc.addField(GxdResultFields.ANNOTATED_STRUCTURE_KEY, structureTermKey);

				if (structureAncestorKeyMap.containsKey(structureTermKey)) {
					// get ancestors by key as well (for links from AD browser)
					for (String structureAncestorKey : structureAncestorKeyMap.get(structureTermKey)) {
						structureKeys.add(structureAncestorKey);
					}
				}

				for (String structKey : structureKeys) {
					doc.addField(GxdResultFields.STRUCTURE_KEY, structKey);
				}

				// result sorts
				doc.addField(GxdResultFields.R_BY_ASSAY_TYPE, rs.getString("r_by_assay_type"));
				doc.addField(GxdResultFields.R_BY_MRK_SYMBOL, rs.getString("r_by_gene_symbol"));
				doc.addField(GxdResultFields.R_BY_AGE, rs.getString("r_by_age"));
				doc.addField(GxdResultFields.R_BY_STRUCTURE, rs.getString("r_by_structure"));
				doc.addField(GxdResultFields.R_BY_EXPRESSED, rs.getString("r_by_expressed"));
				doc.addField(GxdResultFields.R_BY_MUTANT_ALLELES, rs.getString("r_by_mutant_alleles"));
				doc.addField(GxdResultFields.R_BY_REFERENCE, rs.getString("r_by_reference"));

				// add matrix grouping fields
				String stageMatrixGroup = joiner(myEmapaID, isExpressed, theilerStage);
				doc.addField(GxdResultFields.STAGE_MATRIX_GROUP, stageMatrixGroup);

				String geneMatrixGroup = joiner(myEmapaID, isExpressed, markerKey, theilerStage);
				doc.addField(GxdResultFields.GENE_MATRIX_GROUP, geneMatrixGroup);

				docs.add(doc);
				if (docs.size() >= solrCacheSize) {
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>(solrCacheSize);		// max known size
				}
			} // while loop (stepping through rows for this chunk)

			rs.close();
			String ramUsed = memoryUsed();
			systemMap = null;
			imageMap = null;
			logger.info("Finished chunk; RAM used: " + ramUsed + " -> " + memoryUsed());

			if(memoryPercent() > .80) { printMemory(); commit(); }
			
		} // for loop (stepping through chunks)
		
		writeDocs(docs);
		commit();
	}

	// maps detection level to currently approved display text.
	public String mapDetectionLevel(String level) {
		if (level.equals("Absent"))
			return "No";
		else if (detectedYesLevels.contains(level))
			return "Yes";

		return level;
	}

	public Double roundAge(String ageStr) {
		if (ageStr != null) {
			Double age = Double.parseDouble(ageStr);
			Double ageInt = Math.floor(age);
			Double ageDecimal = age - ageInt;
			// try the rounding to nearest 0.5
			if (ageDecimal < 0.25)
				ageDecimal = 0.0;
			else if (ageDecimal < 0.75)
				ageDecimal = 0.5;
			else
				ageDecimal = 1.0;
			return ageInt + ageDecimal;
		}
		// not sure what to do here... age should never be null.
		return -1.0;
	}

	// index RNA-Sequence expression data (not classical data)
	public void	indexRnaSeqData(
			Map<String, List<String>> markerNomenMap,
			Map<String, String> centimorganMap,
			Map<String, Map<String, Map<String, String>>> mutatedInMap,
			Map<String, List<String>> mutatedInAlleleMap,
			Map<String, List<String>> markerVocabMap,
			Map<String, Set<String>> vocabAncestorMap, 
			Map<String, List<String>> structureAncestorIdMap,
			Map<String, List<String>> structureAncestorKeyMap,
			Map<String, List<String>> structureSynonymMap) throws Exception {

		// find the maximum result key, so we have an upper bound when
		// stepping through chunks of results

		ResultSet rs_tmp = ex.executeProto("select max(consolidated_measurement_key) as max_cm_key from expression_ht_consolidated_sample_measurement");
		rs_tmp.next();

		Integer start = 0;
		Integer end = rs_tmp.getInt("max_cm_key");
		rs_tmp.close();
		int chunkSize = 75000;

		// While it appears that modValue could be one iteration too low (due
		// to rounding down), this is accounted for by using <= in the loop.

		int modValue = end.intValue() / chunkSize;

		// pre-cache all the needed genotypes, markers, assays, terms
		cacheGenotypes(0, end.intValue(), true);
		cacheMarkers(0, end.intValue(), true);
		cacheAssays(0, end.intValue(), true);
		cacheTerms(0, end.intValue(), true);

		// Perform the chunking

		logger.info("Getting all RNA-Seq results and related search criteria");
		logger.info("Max consolidated_measurement_key: " + end + ", chunks: " + (modValue + 1));

		// can set the size to our known max (slight efficiency gain)
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(1001);
		
		for (int i = 0; i <= modValue; i++) {

			start = i * chunkSize;
			end = start + chunkSize;

			// mapping from result key to List of high-level EMAPA structures for each result
			Map<String, Set<String>> systemMap = getAnatomicalSystemMap(start, end, true);

			// Note: There are no figure labels for RNA-Seq data.

			logger.info("Processing measurement key > " + start + " and <= " + end + ", RAM used: " + memoryUsed());

			String query = "select sm.consolidated_measurement_key, "
				+ "  sm.marker_key, cs.experiment_key, "
				+ "  emaps.term_key as structure_key, cs.theiler_stage, "
				+ "  cs.age as age_abbreviation, "
				+ "  sm.level as detection_level, cs.age_min, cs.age_max, "
				+ "  null as pattern, et.primary_id as emaps_id, "
				+ "  null as is_wild_type, cs.genotype_key, "
				+ "  exp.primary_id as ref_id, exp.name as ref_title, "
				+ "  sn.by_gene_symbol as r_by_assay_type, "
				+ "  sn.by_gene_symbol as r_by_gene_symbol, "
				+ "  sn.by_age as r_by_age, "
				+ "  sn.by_expressed as r_by_expressed, "
				+ "  sn.by_structure as r_by_structure, "
				+ "  sn.by_experiment as r_by_reference, "
				+ "  sm.average_qn_tpm, "
				+ "  sm.biological_replicate_count, "
				+ "  cs.sex, cs.note "
				+ "from expression_ht_consolidated_sample_measurement sm, "
				+ "  expression_ht_consolidated_sample_measurement_sequence_num sn, "
				+ "  expression_ht_consolidated_sample cs, "
				+ "  expression_ht_experiment exp, "
				+ "  term_emap emaps, term et "
				+ "where sm.consolidated_measurement_key > " + start
				+ "  and sm.consolidated_measurement_key <= " + end
				+ "  and sm.consolidated_measurement_key = sn.consolidated_measurement_key "
				+ "  and cs.experiment_key = exp.experiment_key "
				+ "  and sm.consolidated_sample_key = cs.consolidated_sample_key "
				+ "  and cs.theiler_stage::integer = emaps.stage "
				+ "  and cs.emapa_key = emaps.emapa_term_key "
				+ "  and emaps.term_key = et.term_key";
			
			ResultSet rs = ex.executeProto(query);
			String assay_type = "RNA-Seq";
			String isExpressed = "0";

			while (rs.next()) {
				String markerKey = rs.getString("marker_key");
				String result_key = "rnaseq" + rs.getString("consolidated_measurement_key");
				String assay_key = rs.getString("experiment_key");

				// result fields
				String theilerStage = rs.getString("theiler_stage");
				if ("Below Cutoff".equals(rs.getString("detection_level"))) {
					isExpressed = "0";
				} else {
					isExpressed = "1";
				}
				String structureTermKey = rs.getString("structure_key");

				String chr = chromosome.get(markerKey);
				String cm_offset = "";
				if (centimorganMap.containsKey(markerKey)) {
					cm_offset = centimorganMap.get(markerKey);
				}
				String start_coord = startCoord.get(markerKey);
				String end_coord = endCoord.get(markerKey);
				String spatialString = new String("");
				if ((start_coord != null) && (end_coord != null)) {
					spatialString = SolrLocationTranslator.getIndexValue(
							chr,Long.parseLong(start_coord),Long.parseLong(end_coord),true);
				}

				String unique_key = assay_type + "-" + result_key;
				if (unique_key == null || unique_key.equals("-")) {
					continue;
				}

				SolrInputDocument doc = new SolrInputDocument();

				// Add the single value fields
				doc.addField(GxdResultFields.KEY, unique_key);
				doc.addField(GxdResultFields.MARKER_KEY, markerKey);
				doc.addField(IndexConstants.MRK_BY_SYMBOL, Integer.toString(rs.getInt("r_by_gene_symbol") + countOfClassicalResults));
				doc.addField(GxdResultFields.M_BY_LOCATION, markerByLocation.get(markerKey));
				doc.addField(GxdResultFields.ASSAY_KEY, assay_key);
				doc.addField(GxdResultFields.RESULT_KEY, result_key);
				doc.addField(GxdResultFields.RESULT_TYPE, assay_type);
				doc.addField(GxdResultFields.ASSAY_TYPE, assay_type);
				doc.addField(GxdResultFields.THEILER_STAGE, theilerStage);
				doc.addField(GxdResultFields.EMAPS_ID, rs.getString("emaps_id"));
				doc.addField(GxdResultFields.IS_EXPRESSED, isExpressed);
				doc.addField(GxdResultFields.AGE_MIN, roundAge(rs.getString("age_min")));
				doc.addField(GxdResultFields.AGE_MAX, roundAge(rs.getString("age_max")));
				doc.addField(GxdResultFields.TPM_LEVEL, rs.getString("average_qn_tpm"));
				doc.addField(GxdResultFields.BIOLOGICAL_REPLICATES, rs.getString("biological_replicate_count"));
				doc.addField(GxdResultFields.SEX, rs.getString("sex"));
				doc.addField(GxdResultFields.NOTES, rs.getString("note"));

				boolean isWildType = "1".equals(rs.getString("is_wild_type")) || "-1".equals(rs.getString("genotype_key"));

				String wildType = "mutant";
				if (isWildType) {
					wildType = "wild type";
				}

				doc.addField(GxdResultFields.IS_WILD_TYPE, wildType);

				// marker summary
				doc.addField(GxdResultFields.MARKER_MGIID, markerID.get(markerKey));
				doc.addField(GxdResultFields.MARKER_SYMBOL, markerSymbol.get(markerKey));
				doc.addField(GxdResultFields.MARKER_NAME, markerName.get(markerKey));

				// also add symbol and current name to searchable nomenclature
				doc.addField(GxdResultFields.NOMENCLATURE, markerSymbol.get(markerKey));
				doc.addField(GxdResultFields.NOMENCLATURE, markerName.get(markerKey));
				doc.addField(GxdResultFields.MARKER_TYPE, markerSubtype.get(markerKey));

				// add fields for filtering by marker-associated vocabularies
				for (String mpTerm : markerMpCache.getTerms(markerKey)) {
					doc.addField(GxdResultFields.MP_HEADERS, mpTerm);
				}
				
				for (String goTerm : markerGoCache.getTerms(markerKey)) {
					doc.addField(GxdResultFields.GO_HEADERS, goTerm);
				}

				for (String doTerm : markerDoCache.getTerms(markerKey)) {
					doc.addField(GxdResultFields.DO_HEADERS, doTerm);
				}

				for (String featureType : markerTypeCache.getTerms(markerKey)) {
					doc.addField(GxdResultFields.FEATURE_TYPES, featureType);
				}

				// location stuff
				doc.addField(GxdResultFields.CHROMOSOME, chr);
				doc.addField(GxdResultFields.START_COORD, start_coord);
				doc.addField(GxdResultFields.END_COORD, end_coord);
				doc.addField(GxdResultFields.CYTOBAND, cytoband.get(markerKey));
				doc.addField(GxdResultFields.STRAND, strand.get(markerKey));
				if (!spatialString.equals("")) {
					doc.addField(GxdResultFields.MOUSE_COORDINATE, spatialString);
				}

				if (cm_offset == null || cm_offset.equals("-1"))
					cm_offset = "";
				doc.addField(GxdResultFields.CENTIMORGAN, cm_offset);

				// assay summary
				doc.addField(GxdResultFields.ASSAY_HAS_IMAGE, "1".equals(assayHasImage.get(assay_key)));
				doc.addField(GxdResultFields.PROBE_KEY, assayProbeKey.get(assay_key));
				doc.addField(GxdResultFields.ANTIBODY_KEY, assayAntibodyKey.get(assay_key));

				// assay sorts
				doc.addField(GxdResultFields.A_BY_SYMBOL, Integer.toString(rs.getInt("r_by_gene_symbol") + countOfClassicalResults));
				doc.addField(GxdResultFields.A_BY_ASSAY_TYPE, Integer.toString(rs.getInt("r_by_gene_symbol") + countOfClassicalResults));

				// result summary
				doc.addField(GxdResultFields.DETECTION_LEVEL, mapDetectionLevel(rs.getString("detection_level")) );
				doc.addField(GxdResultFields.STRUCTURE_PRINTNAME, printname.get(structureTermKey));
				doc.addField(GxdResultFields.AGE, rs.getString("age_abbreviation"));
				doc.addField(GxdResultFields.ASSAY_MGIID, assayID.get(assay_key));
				doc.addField(GxdResultFields.JNUM, rs.getString("ref_id"));
				doc.addField(GxdResultFields.PUBMED_ID, rs.getString("ref_id"));
				doc.addField(GxdResultFields.SHORT_CITATION, rs.getString("ref_title"));
				doc.addField(GxdResultFields.GENOTYPE, allelePairs.get(rs.getString("genotype_key")));
				doc.addField(GxdResultFields.STRAIN, bgStrains.get(rs.getString("genotype_key")));
				doc.addField(GxdResultFields.PATTERN, rs.getString("pattern"));

				// multi values

				if (systemMap.containsKey(result_key)) {
					for (String system : systemMap.get(result_key)) {
						doc.addField(GxdResultFields.ANATOMICAL_SYSTEM, system);
					}
					systemMap.remove(result_key);
				}

				if (markerNomenMap.containsKey(markerKey)) {
					for (String nomen : markerNomenMap.get(markerKey)) {
						doc.addField(GxdResultFields.NOMENCLATURE, nomen);
					}
				}

				String genotype_key = rs.getString("genotype_key");
				if (mutatedInMap.containsKey(genotype_key)) {
					Map<String, Map<String, String>> gMap = mutatedInMap.get(genotype_key);
					for (String genotype_marker_key : gMap.keySet()) {
						doc.addField(GxdResultFields.MUTATED_IN, gMap.get(genotype_marker_key).get("symbol"));
						doc.addField(GxdResultFields.MUTATED_IN, gMap.get(genotype_marker_key).get("name"));

						// get any synonyms
						if (markerNomenMap.containsKey(genotype_marker_key)) {
							for (String synonym : markerNomenMap.get(genotype_marker_key)) {
								doc.addField(GxdResultFields.MUTATED_IN, synonym);
							}
						}
					}
				}

				if (mutatedInAlleleMap.containsKey(genotype_key)) {
					List<String> alleleIds = mutatedInAlleleMap.get(genotype_key);

					for (String alleleId : alleleIds) {
						doc.addField(GxdResultFields.ALLELE_ID, alleleId);
					}

				}

				if (markerVocabMap.containsKey(markerKey)) {
					Set<String> uniqueAnnotationIDs = new HashSet<String>();

					for (String termId : markerVocabMap.get(markerKey)) {
						uniqueAnnotationIDs.add(termId);
						if (vocabAncestorMap.containsKey(termId)) {
							for (String ancestorId : vocabAncestorMap.get(termId)) {
								uniqueAnnotationIDs.add(ancestorId);
							}
						}
					}

					for (String annotationID : uniqueAnnotationIDs) {
						doc.addField(GxdResultFields.ANNOTATION, annotationID);
					}
				}

				String myEmapaID = emapaID.get(structureTermKey);

				Set<String> ancestorIDs = new HashSet<String>();
				ancestorIDs.add(structureID.get(structureTermKey));
				Set<String> ancestorStructures = new HashSet<String>();
				ancestorStructures.add(printname.get(structureTermKey));

				if (structureAncestorIdMap.containsKey(structureTermKey)) {
					// get ancestors
					List<String> structure_ancestor_ids = structureAncestorIdMap.get(structureTermKey);

					for (String structure_ancestor_id : structure_ancestor_ids) {
						// get synonyms for each ancestor/term

						if (structureSynonymMap.containsKey(structure_ancestor_id)) {

							// also add structure MGI ID
							ancestorIDs.add(structure_ancestor_id);
							for (String structureSynonym : structureSynonymMap.get(structure_ancestor_id)) {
								ancestorStructures.add(structureSynonym);
							}
						}
					}

					// only add unique structures (for best solr indexing
					// performance)
					for (String ancestorId : ancestorIDs) {
						doc.addField(GxdResultFields.STRUCTURE_ID, ancestorId);
					}
					for (String ancestorStructure : ancestorStructures) {
						doc.addField(GxdResultFields.STRUCTURE_ANCESTORS, ancestorStructure);
					}
				}
				
				// add the id for this exact structure
				doc.addField(GxdResultFields.STRUCTURE_EXACT, myEmapaID);

				Set<String> structureKeys = new HashSet<String>();
				structureKeys.add(structureTermKey);
				doc.addField(GxdResultFields.ANNOTATED_STRUCTURE_KEY, structureTermKey);

				if (structureAncestorKeyMap.containsKey(structureTermKey)) {
					// get ancestors by key as well (for links from AD browser)
					for (String structureAncestorKey : structureAncestorKeyMap.get(structureTermKey)) {
						structureKeys.add(structureAncestorKey);
					}
				}

				for (String structKey : structureKeys) {
					doc.addField(GxdResultFields.STRUCTURE_KEY, structKey);
				}

				// result sorts
				doc.addField(GxdResultFields.R_BY_ASSAY_TYPE, Integer.toString(rs.getInt("r_by_assay_type") + countOfClassicalResults));
				doc.addField(GxdResultFields.R_BY_MRK_SYMBOL, Integer.toString(rs.getInt("r_by_gene_symbol") + countOfClassicalResults));
				doc.addField(GxdResultFields.R_BY_AGE, Integer.toString(rs.getInt("r_by_age") + countOfClassicalResults));
				doc.addField(GxdResultFields.R_BY_STRUCTURE, Integer.toString(rs.getInt("r_by_structure") + countOfClassicalResults));
				doc.addField(GxdResultFields.R_BY_EXPRESSED, Integer.toString(rs.getInt("r_by_expressed") + countOfClassicalResults));
				doc.addField(GxdResultFields.R_BY_MUTANT_ALLELES, "0");
				doc.addField(GxdResultFields.R_BY_REFERENCE, Integer.toString(rs.getInt("r_by_reference") + countOfClassicalResults));

				// add matrix grouping fields
				String stageMatrixGroup = joiner(myEmapaID, isExpressed, theilerStage);
				doc.addField(GxdResultFields.STAGE_MATRIX_GROUP, stageMatrixGroup);

				String geneMatrixGroup = joiner(myEmapaID, isExpressed, markerKey, theilerStage);
				doc.addField(GxdResultFields.GENE_MATRIX_GROUP, geneMatrixGroup);

				docs.add(doc);
				if (docs.size() >= solrCacheSize) {
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>(solrCacheSize);		// max known size
				}
			} // while loop (stepping through rows for this chunk)

			rs.close();
			String ramUsed = memoryUsed();
			systemMap = null;
			logger.info("Finished chunk; RAM used: " + ramUsed + " -> " + memoryUsed());

			if(memoryPercent() > .80) { printMemory(); commit(); }
			else if (i % 4 == 0) {
				// To avoid out of memory errors on the server, we need to commit frequently regardless of the memoryPercent()
				// of the Indexer.  We use a mod factor to try to spread out the commits and save a little performance.
				commit();
			}
			
		} // for loop (stepping through chunks)
		
		writeDocs(docs);
		commit();
	}
}
