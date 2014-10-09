package org.jax.mgi.indexer;

import java.sql.ResultSet;
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
	public GXDResultIndexerSQL() {
		super("index.url.gxdResult");
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
				+ "and exists (select 1 from expression_result_summary ers "
				+ "  where msn.marker_key = ers.marker_key) ";
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
		logger.info(" - gathered synonyms for " + markerNomenMap.size()
				+ " markers");

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
				+ "and exists (select 1 from expression_result_summary ers "
				+ "  where ml.marker_key = ers.marker_key)";
		ResultSet rs = ex.executeProto(centimorganQuery);

		String mkey; // marker key
		String cm_offset; // centimorgan offset for the marker

		while (rs.next()) {
			mkey = rs.getString("marker_key");
			cm_offset = rs.getString("cm_offset");

			centimorganMap.put(mkey, cm_offset);
		}
		logger.info(" - gathered cM for " + centimorganMap.size() + " markers");
		return centimorganMap;
	}

	/*
	 * get a mapping from genotype keys (as Strings) to data about markers
	 * mutated in those genotypes. Mapping returned is like: { genotype key : {
	 * marker key : { "symbol" : symbol, "name" : name } } } The mapping only
	 * includes genotypes tied to expression results.
	 */
	private Map<String, Map<String, Map<String, String>>> getMutatedInMap()
			throws Exception {

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
				+ "  and exists (select 1 from expression_result_summary ers "
				+ "    where ag.genotype_key = ers.genotype_key)";
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
		logger.info(" - gathered markers for " + mutatedInMap.size()
				+ " genotypes");

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
				+ "  and exists (select 1 from expression_result_summary ers "
				+ "    where ag.genotype_key = ers.genotype_key)";

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
				+ " genotypes");

		return mutatedInAlleleMap;
	}

	/*
	 * get a mapping from marker keys (as Strings) to IDs of other (non-anatomy)
	 * vocabulary terms annotated to those markers. Each marker key refers to a
	 * List of IDs.
	 */
	private Map<String, List<String>> getMarkerVocabMap() throws Exception {
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
			markerVocabMap.get(mkey).add(termId);
		}
		logger.info(" - gathered annotated terms for " + markerVocabMap.size()
				+ " markers");

		// add extra data for OMIM terms associated to human markers
		// which are associated with mouse markers via homology

		ResultSet rs2 = ex.executeProto(
			SharedQueries.GXD_OMIM_HOMOLOGY_QUERY);
		int i = 0;

		while (rs2.next()) {
			mkey = rs2.getString("marker_key");
			termId = rs2.getString("term_id");

			if (!markerVocabMap.containsKey(mkey)) {
				markerVocabMap.put(mkey,
					new ArrayList<String>());
			}
			if (!markerVocabMap.get(mkey).contains(termId)) {
				markerVocabMap.get(mkey).add(termId);
				i++;
			}
		}

		logger.info(" - added " + i
			+ " annotations to OMIM via homology");
		
		return markerVocabMap;
	}

	/*
	 * get a mapping from each term ID to a List of IDs for its ancestor terms,
	 * for terms in non-anatomy vocabularies which are annotated to markers.
	 */
	private Map<String, List<String>> getVocabAncestorMap() throws Exception {
		Map<String, List<String>> vocabAncestorMap = new HashMap<String, List<String>>();

		logger.info("building map of vocabulary term ancestors");

		String vocabAncestorQuery = SharedQueries.GXD_VOCAB_ANCESTOR_QUERY;
		ResultSet rs = ex.executeProto(vocabAncestorQuery);

		String termId; // term's ID
		String ancestorId; // ancestor's term ID

		while (rs.next()) {
			termId = rs.getString("primary_id");
			ancestorId = rs.getString("ancestor_primary_id");

			if (!vocabAncestorMap.containsKey(termId)) {
				vocabAncestorMap.put(termId, new ArrayList<String>());
			}
			vocabAncestorMap.get(termId).add(ancestorId);
		}
		logger.info(" - gathered ancestor IDs for " + vocabAncestorMap.size()
				+ " terms");

		return vocabAncestorMap;
	}

	/*
	 * get a mapping from expression result key (as a String) to a List of
	 * figure labels for that result.
	 */
	private Map<String, List<String>> getImageMap() throws Exception {
		Map<String, List<String>> imageMap = new HashMap<String, List<String>>();

		logger.info("building map of expression images");

		// label could be either specimen label or if null use the figure label
		String imageQuery = "select eri.result_key, "
				+ "  case when ei.pane_label is null then i.figure_label "
				+ "    else (i.figure_label || ei.pane_label) end as label "
				+ "from expression_result_summary ers, "
				+ "  expression_result_to_imagepane eri, "
				+ "  expression_imagepane ei, " + "  image i "
				+ "where eri.imagepane_key = ei.imagepane_key "
				+ "  and ei.image_key = i.image_key "
				+ "  and eri.result_key = ers.result_key "
				+ "  and ers.specimen_key is null " + "UNION "
				+ "select ers.result_key, sp.specimen_label as label "
				+ "from expression_result_summary ers, "
				+ "  assay_specimen sp "
				+ "where ers.specimen_key = sp.specimen_key ";

		ResultSet rs = ex.executeProto(imageQuery);

		String rkey; // result key
		String label; // specimen label

		while (rs.next()) {
			rkey = rs.getString("result_key");
			label = rs.getString("label");

			// skip empty labels
			if (label != null && !label.equals("")) {
				if (!imageMap.containsKey(rkey)) {
					imageMap.put(rkey, new ArrayList<String>());
				}
				imageMap.get(rkey).add(label);
			}
		}
		logger.info(" - gathered figure labels for " + imageMap.size()
				+ " results");

		return imageMap;
	}

	/*
	 * build a mapping from a string (field specified by 'key') to a List of
	 * String values (field specified by 'value1'). If 'value2' is specified
	 * then we also include the value of the field with that name the first time
	 * we find each 'key'. 'msg' specifies the type of items we are gathering,
	 * only used for debugging output.
	 */
	private Map<String, List<String>> getMap(String query, String key,
			String value1, String value2, String msg) throws Exception {

		Map<String, List<String>> structureAncestorMap = new HashMap<String, List<String>>();

		logger.info("building map of " + msg + " for structures");

		ResultSet rs = ex.executeProto(query);

		String sKey; // value of structure key
		String sValue1; // primary value to collect

		while (rs.next()) {
			sKey = rs.getString(key);
			sValue1 = rs.getString(value1);

			if (!structureAncestorMap.containsKey(sKey)) {
				structureAncestorMap.put(sKey, new ArrayList<String>());

				// add value2 the first time this key is found, if defined
				if (value2 != null) {
					structureAncestorMap.get(sKey).add(rs.getString(value2));
				}
			}

			if ((sValue1 != null) && (!sValue1.equals(""))) {
				structureAncestorMap.get(sKey).add(sValue1);
			}
		}
		logger.info(" - gathered " + msg + " for "
				+ structureAncestorMap.size() + " terms");

		return structureAncestorMap;
	}

	/*
	 * -------------------- main indexing method --------------------
	 */
	public void index() throws Exception {
		// first get a pull a bunch of mappings into memory, to make later
		// processing easier

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
		Map<String, List<String>> vocabAncestorMap = getVocabAncestorMap();

		// get List of figure labels for each expression result key
		Map<String, List<String>> imageMap = getImageMap();

		// get List of ancestor IDs for each structure
		Map<String, List<String>> structureAncestorIdMap = getMap(
				SharedQueries.GXD_EMAP_ANCESTOR_QUERY, "structure_term_key",
				"ancestor_id", "structure_id", "IDs");

		// get List of ancestor keys for each structure
		Map<String, List<String>> structureAncestorKeyMap = getMap(
				SharedQueries.GXD_EMAP_ANCESTOR_QUERY, "structure_term_key",
				"default_parent_key", null, "keys");

		// get List of synonyms for each structure
		Map<String, List<String>> structureSynonymMap = getMap(
				SharedQueries.GXD_EMAP_SYNONYMS_QUERY, "structure_id",
				"synonym", "structure", "synonyms");

		// -------------------------------------------------------------------
		// Finally finished gathering mappings, time for the main body of work
		// -------------------------------------------------------------------

		// find the maximum result key, so we have an upper bound when
		// stepping through chunks of results

		ResultSet rs_tmp = ex
				.executeProto("select max(result_key) as max_result_key "
						+ "from expression_result_summary");
		rs_tmp.next();

		Integer start = 0;
		Integer end = rs_tmp.getInt("max_result_key");
		int chunkSize = 150000;

		// While it appears that modValue could be one iteration too low (due
		// to rounding down), this is accounted for by using <= in the loop.

		int modValue = end.intValue() / chunkSize;

		// Perform the chunking

		logger.info("Getting all assay results and related search criteria");
		logger.info("Max result_key: " + end + ", chunks: " + (modValue + 1));

		for (int i = 0; i <= modValue; i++) {

			start = i * chunkSize;
			end = start + chunkSize;

			logger.info("Processing result key > " + start + " and <= " + end);
			String query = "select distinct ers.result_key, "
					+ "  ers.marker_key, " + "  ers.assay_key, "
					+ "  ers.assay_type, "
					+ "  tae.term_key as mgd_structure_key, "
					+ "  ers.structure_key, " + "  ers.theiler_stage, "
					+ "  ers.is_expressed, " + "  ers.has_image, "
					+ "  ers.structure_printname, "
					+ "  ers.age_abbreviation, " + "  ers.anatomical_system, "
					+ "  ers.jnum_id, " + "  ers.detection_level, "
					+ "  ers.marker_symbol, " + "  ers.assay_id, "
					+ "  ers.age_min, " + "  ers.age_max, " + "  ers.pattern, "
					+ "  ers.is_wild_type, " + "  ers.genotype_key, "
					+ "  m.marker_subtype, " + "  m.name marker_name, "
					+ "  m.primary_id marker_id, " + "  ml.chromosome, "
					+ "  ml.cytogenetic_offset, " + "  ml.start_coordinate, "
					+ "  ml.end_coordinate,ml.strand, "
					+ "  r.mini_citation, r.pubmed_id, "
					+ "  g.combination_1 genotype, "
					+ "  structure.primary_id structure_id, "
					+ "  msqn.by_symbol, " + "  msqn.by_location, "
					+ "  ersn.by_assay_type r_by_assay_type, "
					+ "  ersn.by_gene_symbol r_by_gene_symbol, "
					+ "  ersn.by_anatomical_system r_by_anatomical_system, "
					+ "  ersn.by_age r_by_age, "
					+ "  ersn.by_expressed r_by_expressed, "
					+ "  ersn.by_structure r_by_structure, "
					+ "  ersn.by_mutant_alleles r_by_mutant_alleles, "
					+ "  ersn.by_reference r_by_reference, "
					+ "  easn.by_symbol a_by_symbol, "
					+ "  easn.by_assay_type a_by_assay_type, "
					+ "  exa.has_image a_has_image, "
					+ "  exa.probe_key a_probe_key, "
					+ "  exa.antibody_key a_antibody_key, "
					+ "  emapa.primary_id emapa_id "
					+ "from expression_result_summary ers, "
					+ "  marker_sequence_num msqn, " + "  marker_counts mc, "
					+ "  marker m, " + "  marker_location ml, "
					+ "  reference r, " + "  genotype g, "
					+ "  term structure,  " + "  term_emap tae, "
					+ "  term emapa, " + "  expression_assay exa, "
					+ "  expression_assay_sequence_num easn, "
					+ "  expression_result_sequence_num ersn "
					+ "where ers.marker_key=msqn.marker_key "
					+ "  and ers.marker_key = ml.marker_key "
					+ "  and ml.sequence_num=1 "
					+ "  and ers.reference_key=r.reference_key "
					+ "  and ers.genotype_key=g.genotype_key "
					+ "  and ers.marker_key=mc.marker_key "
					+ "  and ers.marker_key=m.marker_key "
					+ "  and ersn.result_key=ers.result_key "
					+ "  and exa.assay_key=ers.assay_key "
					+ "  and easn.assay_key=ers.assay_key "
					+ "  and ers.assay_type != 'Recombinase reporter'"
					+ "  and ers.assay_type != 'In situ reporter (transgenic)'"
					+ "  and mc.gxd_literature_count>0 "
					+ "  and ers.structure_key=structure.term_key "
					+ "  and ers.structure_key=tae.term_key "
					+ "  and tae.emapa_term_key=emapa.term_key "
					+ "  and ers.result_key > " + start
					+ "  and ers.result_key <= " + end + " ";
			ResultSet rs = ex.executeProto(query);

			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			logger.info("Parsing them");
			while (rs.next()) {
				String markerKey = rs.getString("marker_key");
				// the marker symbol sort
				String by_symbol = rs.getString("by_symbol");
				String by_location = rs.getString("by_location");
				String result_key = rs.getString("result_key");
				String assay_key = rs.getString("assay_key");

				String assay_type = rs.getString("assay_type");

				// result fields
				String theilerStage = rs.getString("theiler_stage");
				String isExpressed = rs.getString("is_expressed");
				String detection_level = rs.getString("detection_level");
				String printname = rs.getString("structure_printname");
				String structureTermKey = rs.getString("structure_key");
				String mgd_structure_key = rs.getString("mgd_structure_key");
				String age = rs.getString("age_abbreviation");
				String assay_id = rs.getString("assay_id");
				String anatomical_system = rs.getString("anatomical_system");
				String jnum = rs.getString("jnum_id");
				String mini_citation = rs.getString("mini_citation");
				String genotype = rs.getString("genotype");
				String has_image = rs.getString("has_image");

				String markerSymbol = rs.getString("marker_symbol");
				String markerName = rs.getString("marker_name");
				String markerType = rs.getString("marker_subtype");
				String markerMgiid = rs.getString("marker_id");

				String chr = rs.getString("chromosome");
				String cm_offset = "";
				if (centimorganMap.containsKey(markerKey)) {
					cm_offset = centimorganMap.get(markerKey);
				}
				String cytogenetic_offset = rs.getString("cytogenetic_offset");
				String start_coord = rs.getString("start_coordinate");
				String end_coord = rs.getString("end_coordinate");
				String strand = rs.getString("strand");
				String spatialString = new String("");
				if ((start_coord != null) && (end_coord != null)) {
					spatialString = SolrLocationTranslator.getIndexValue(
					chr,Long.parseLong(start_coord),Long.parseLong(end_coord),true);
				}
				// result sorts
				String r_by_assay_type = rs.getString("r_by_assay_type");
				String r_by_gene_symbol = rs.getString("r_by_gene_symbol");
				String r_by_anatomical_system = rs
						.getString("r_by_anatomical_system");
				String r_by_age = rs.getString("r_by_age");
				String r_by_structure = rs.getString("r_by_structure");
				String r_by_expressed = rs.getString("r_by_expressed");
				String r_by_mutant_alleles = rs
						.getString("r_by_mutant_alleles");
				String r_by_reference = rs.getString("r_by_reference");

				// assay summary
				String a_has_image = rs.getString("a_has_image");
				String a_probe_key = rs.getString("a_probe_key");
				String a_antibody_key = rs.getString("a_antibody_key");

				// assay sorts
				String a_by_symbol = rs.getString("a_by_symbol");
				String a_by_assay_type = rs.getString("a_by_assay_type");

				String unique_key = assay_type + "-" + result_key;
				if (unique_key == null || unique_key.equals("-")) {
					continue;
				}

				SolrInputDocument doc = new SolrInputDocument();

				// if(docs.containsKey(unique_key))
				// {
				// doc = docs.get(unique_key);
				// }
				// else
				// {
				//
				// Add the single value fields
				doc.addField(GxdResultFields.KEY, unique_key);
				doc.addField(GxdResultFields.MARKER_KEY, markerKey);
				doc.addField(IndexConstants.MRK_BY_SYMBOL, by_symbol);
				doc.addField(GxdResultFields.M_BY_LOCATION, by_location);
				doc.addField(GxdResultFields.ASSAY_KEY, assay_key);
				doc.addField(GxdResultFields.RESULT_KEY, result_key);
				doc.addField(GxdResultFields.RESULT_TYPE, assay_type);
				doc.addField(GxdResultFields.ASSAY_TYPE, assay_type);
				doc.addField(GxdResultFields.THEILER_STAGE, theilerStage);
				doc.addField(GxdResultFields.IS_EXPRESSED, isExpressed);
				doc.addField(GxdResultFields.AGE_MIN,
						roundAge(rs.getString("age_min")));
				doc.addField(GxdResultFields.AGE_MAX,
						roundAge(rs.getString("age_max")));

				boolean isWildType = rs.getString("is_wild_type").equals("1")
						|| rs.getString("genotype_key").equals("-1");

				String wildType = "mutant";
				if (isWildType) {
					wildType = "wild type";
				}

				doc.addField(GxdResultFields.IS_WILD_TYPE, wildType);

				// marker summary
				doc.addField(GxdResultFields.MARKER_MGIID, markerMgiid);
				doc.addField(GxdResultFields.MARKER_SYMBOL, markerSymbol);
				doc.addField(GxdResultFields.MARKER_NAME, markerName);

				// also add symbol and current name to searchable nomenclature
				doc.addField(GxdResultFields.NOMENCLATURE, markerSymbol);
				doc.addField(GxdResultFields.NOMENCLATURE, markerName);
				doc.addField(GxdResultFields.MARKER_TYPE, markerType);

				// location stuff
				doc.addField(GxdResultFields.CHROMOSOME, chr);
				doc.addField(GxdResultFields.START_COORD, start_coord);
				doc.addField(GxdResultFields.END_COORD, end_coord);
				doc.addField(GxdResultFields.CYTOBAND, cytogenetic_offset);
				doc.addField(GxdResultFields.STRAND, strand);
				if (!spatialString.equals("")) {
					doc.addField(GxdResultFields.MOUSE_COORDINATE, spatialString);
				}

				if (cm_offset == null || cm_offset.equals("-1"))
					cm_offset = "";
				doc.addField(GxdResultFields.CENTIMORGAN, cm_offset);

				// assay summary
				doc.addField(GxdResultFields.ASSAY_HAS_IMAGE,
						a_has_image.equals("1"));
				doc.addField(GxdResultFields.PROBE_KEY, a_probe_key);
				doc.addField(GxdResultFields.ANTIBODY_KEY, a_antibody_key);

				// assay sorts
				doc.addField(GxdResultFields.A_BY_SYMBOL, a_by_symbol);
				doc.addField(GxdResultFields.A_BY_ASSAY_TYPE, a_by_assay_type);

				// result summary
				doc.addField(GxdResultFields.DETECTION_LEVEL,
						mapDetectionLevel(detection_level));
				doc.addField(GxdResultFields.STRUCTURE_PRINTNAME, printname);
				doc.addField(GxdResultFields.AGE, age);
				doc.addField(GxdResultFields.ASSAY_MGIID, assay_id);
				doc.addField(GxdResultFields.JNUM, jnum);
				doc.addField(GxdResultFields.PUBMED_ID,
						rs.getString("pubmed_id"));
				doc.addField(GxdResultFields.ANATOMICAL_SYSTEM,
						anatomical_system);
				doc.addField(GxdResultFields.SHORT_CITATION, mini_citation);
				doc.addField(GxdResultFields.GENOTYPE, genotype);
				doc.addField(GxdResultFields.PATTERN, rs.getString("pattern"));

				// multi values
				if (markerNomenMap.containsKey(markerKey)) {
					for (String nomen : markerNomenMap.get(markerKey)) {
						doc.addField(GxdResultFields.NOMENCLATURE, nomen);
					}
				}

				String genotype_key = rs.getString("genotype_key");
				if (mutatedInMap.containsKey(genotype_key)) {
					Map<String, Map<String, String>> gMap = mutatedInMap
							.get(genotype_key);
					for (String genotype_marker_key : gMap.keySet()) {
						doc.addField(GxdResultFields.MUTATED_IN,
								gMap.get(genotype_marker_key).get("symbol"));
						doc.addField(GxdResultFields.MUTATED_IN,
								gMap.get(genotype_marker_key).get("name"));

						// get any synonyms
						if (markerNomenMap.containsKey(genotype_marker_key)) {
							for (String synonym : markerNomenMap
									.get(genotype_marker_key)) {

								doc.addField(GxdResultFields.MUTATED_IN,
										synonym);
							}
						}
					}
				}

				if (mutatedInAlleleMap.containsKey(genotype_key)) {
					List<String> alleleIds = mutatedInAlleleMap
							.get(genotype_key);

					for (String alleleId : alleleIds) {
						doc.addField(GxdResultFields.ALLELE_ID, alleleId);
					}

				}

				if (markerVocabMap.containsKey(markerKey)) {
					Set<String> uniqueAnnotationIDs = new HashSet<String>();

					for (String termId : markerVocabMap.get(markerKey)) {
						uniqueAnnotationIDs.add(termId);
						if (vocabAncestorMap.containsKey(termId)) {
							for (String ancestorId : vocabAncestorMap
									.get(termId)) {

								uniqueAnnotationIDs.add(ancestorId);
							}
						}
					}

					for (String annotationID : uniqueAnnotationIDs) {
						doc.addField(GxdResultFields.ANNOTATION, annotationID);
					}
				}

				if (imageMap.containsKey(result_key)) {
					List<String> figures = imageMap.get(result_key);
					for (String figure : figures) {
						if (has_image.equals("1")) {
							doc.addField(GxdResultFields.FIGURE, figure);
							doc.addField(GxdResultFields.FIGURE_PLAIN, figure);
						}
					}
				}

				String structureID = rs.getString("structure_id");
				String emapaID = rs.getString("emapa_id");

				Set<String> ancestorIDs = new HashSet<String>();
				ancestorIDs.add(structureID);
				Set<String> ancestorStructures = new HashSet<String>();
				ancestorStructures.add(printname);

				if (structureAncestorIdMap.containsKey(structureTermKey)) {
					// get ancestors
					List<String> structure_ancestor_ids = structureAncestorIdMap
							.get(structureTermKey);

					for (String structure_ancestor_id : structure_ancestor_ids) {
						// get synonyms for each ancestor/term

						if (structureSynonymMap
								.containsKey(structure_ancestor_id)) {

							// also add structure MGI ID
							ancestorIDs.add(structure_ancestor_id);
							for (String structureSynonym : structureSynonymMap
									.get(structure_ancestor_id)) {

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
						doc.addField(GxdResultFields.STRUCTURE_ANCESTORS,
								ancestorStructure);
					}
				}

				// add the id for this exact structure
				doc.addField(GxdResultFields.STRUCTURE_EXACT, emapaID);

				Set<String> structureKeys = new HashSet<String>();
				structureKeys.add(mgd_structure_key);
				doc.addField(GxdResultFields.ANNOTATED_STRUCTURE_KEY,
						mgd_structure_key);

				if (structureAncestorKeyMap.containsKey(structureTermKey)) {
					// get ancestors by key as well (for links from AD browser)
					for (String structureAncestorKey : structureAncestorKeyMap
							.get(structureTermKey)) {

						structureKeys.add(structureAncestorKey);
					}
				}

				for (String structKey : structureKeys) {
					doc.addField(GxdResultFields.STRUCTURE_KEY, structKey);
				}

				// result sorts
				doc.addField(GxdResultFields.R_BY_ASSAY_TYPE, r_by_assay_type);
				doc.addField(GxdResultFields.R_BY_MRK_SYMBOL, r_by_gene_symbol);
				doc.addField(GxdResultFields.R_BY_ANATOMICAL_SYSTEM,
						r_by_anatomical_system);
				doc.addField(GxdResultFields.R_BY_AGE, r_by_age);
				doc.addField(GxdResultFields.R_BY_STRUCTURE, r_by_structure);
				doc.addField(GxdResultFields.R_BY_EXPRESSED, r_by_expressed);
				doc.addField(GxdResultFields.R_BY_MUTANT_ALLELES,
						r_by_mutant_alleles);
				doc.addField(GxdResultFields.R_BY_REFERENCE, r_by_reference);

				// add matrix grouping fields
				String stageMatrixGroup = StringUtils.join(
						Arrays.asList(emapaID, isExpressed, theilerStage), "_");
				doc.addField(GxdResultFields.STAGE_MATRIX_GROUP,
						stageMatrixGroup);

				String geneMatrixGroup = StringUtils.join(Arrays.asList(
						emapaID, isExpressed, markerKey, theilerStage), "_");
				doc.addField(GxdResultFields.GENE_MATRIX_GROUP, geneMatrixGroup);

				docs.add(doc);
				if (docs.size() > 1000) {
					startTime();
					writeDocs(docs);

					long endTime = stopTime();
					if (endTime > 500) {
						logger.info("time to call writeDocs() " + stopTime());
					}

					docs = new ArrayList<SolrInputDocument>();
				}
			} // while loop (stepping through rows for this chunk)

			// add and commit
			if (!docs.isEmpty()) {
				server.add(docs);
			}
			server.commit();

		} // for loop (stepping through chunks)
	}

	// maps detection level to currently approved display text.
	public String mapDetectionLevel(String level) {
		List<String> detectedYesLevels = Arrays.asList("Present", "Trace",
				"Weak", "Moderate", "Strong", "Very strong");
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

	/*
	 * For debugging purposes only
	 */
	private long startTime = 0;

	public void startTime() {
		startTime = System.nanoTime();
	}

	public long stopTime() {
		long endTime = System.nanoTime();
		return (endTime - startTime) / 1000000;

	}
}
