package org.jax.mgi.shr;

import java.sql.ResultSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QSExpressionFacetToolkit {
	//--- static variables ---//
	
	// temp table name for collected EMAPA header terms
	private static String EMAPA_HEADERS = "emapa_headers";
	
	// temp table name for EMAPS terms, with a flag for each to indicate whether it has any annotations
	private static String EMAPS_TERMS = "emaps_terms";
	
	// temp table name for EMAPS terms, with a term key, primary ID, and header term for any EMAPS terms
	// that have positive annotations
	private static String EMAPS_TERMS_TO_HEADERS = "emaps_terms_to_headers";
	
	// temp table name for EMAPA terms, with a term key, primary ID, and header term for any EMAPA terms
	// that have positive annotations (computed in a stage-dependent manner).
	private static String EMAPA_TERMS_TO_HEADERS = "emapa_terms_to_headers";
	
	// temp table name for (feature key, header term key, expressed flag) triples.  Used to track which
	// anatomical systems have exhibited expression in which genes.
	private static String MARKER_HEADER_MAP = "marker_header_map";
	
	//--- instance variables ---//
	
	// logger for this class
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	// set of temp tables already built by this object
	private Set<String> existingTables = new HashSet<String>();
	
	// count of indexes created so far
	private int indexCount = 0;
	
	//--- public methods ---//
	
	// Get a String SQL command that returns marker keys and their corresponding EMAPA header terms,
	// for those terms that have positive expression data.  Those data may be from either classical
	// assays or high-throughput experiments.
	public String getAnatomyHeadersForMarkers(SQLExecutor ex) throws SQLException {
		String markerToHeaders = getMarkerHeaderMapTable(ex);
		
		// markers and their EMAPA headers (computed in a stage-dependent manner), for terms with positive
		// expression data.
		String cmd = "select marker_key as feature_key, header as term "
			+ "from " + markerToHeaders + " "
			+ "where expressed = 1 "
			+ "order by 1, 2";
		
		return cmd;
	}

	// Get a String SQL command that returns EMAPS IDs and their corresponding EMAPA header terms,
	// for those EMAPS terms that have positive expression data.  Those data may be from either
	// classical assays or high-throughput experiments.
	public String getHeadersForExpressedEmapsTerms(SQLExecutor ex) throws SQLException {
		String emapsTermsToHeaders = getEmapsTermsToHeadersTable(ex);
		
		// EMAPS terms and their headers (computed in a stage-dependent manner), for terms with positive
		// expression data.
		String cmd = "select primary_id, header "
			+ "from " + emapsTermsToHeaders + " "
			+ "order by 1, 2";
		
		return cmd;
	}

	// Get a String SQL command that returns EMAPA IDs and their respective header terms, but only
	// for those EMAPA terms that have positive expression data.  Those expression data may be from either
	// classical assays or high-throughput experiments.  Traverses the DAG in a stage-aware manner
	// using the EMAPS terms associated with the EMAPA ones.  
	public String getHeadersForExpressedEmapaTerms(SQLExecutor ex) throws SQLException {
		String emapaTermsToHeaders = getEmapaTermsToHeadersTable(ex);
		
		// EMAPA terms and their headers (computed in a stage-dependent manner), for terms with positive
		// expression data.  Top half is looking for at the terms' descendants, bottom half is looking at
		// the terms themselves.
		String cmd = "select primary_id, header "
			+ "from " + emapaTermsToHeaders + " "
			+ "order by 1, 2";
		
		return cmd;
	}
	
	//--- private methods (general purpose) ---//
	
	// Build a new index on the given table for the given field(s), flagged as unique if specified.
	// Fields are comma-separated, if more than one.  Returns name of new index.
	private String createIndex(SQLExecutor ex, String onTable, String onFields, boolean isUnique) {
		String name = "qvft" + indexCount++;
		
		String cmd = "create <isUnique> index <name> on <table> (<fields>)"
			.replaceFirst("<table>", onTable)
			.replaceFirst("<fields>", onFields)
			.replaceFirst("<name>", name);

		if (isUnique) {
			cmd = cmd.replaceFirst("<isUnique>", "unique");
		} else {
			cmd = cmd.replaceFirst("<isUnique>", "");
		}
		ex.executeUpdate(cmd);
		logger.debug("Executed: " + cmd);
		
		return name;
	}
	
	// Create and return the name of a table containing one row for each EMAPA header term, containing
	// the term and its ID.
	private String getEmapaHeaderTable (SQLExecutor ex) throws SQLException {
		if (existingTables.contains(EMAPA_HEADERS)) { return EMAPA_HEADERS; }

		String cmd = "create temp table " + EMAPA_HEADERS + " as ( "
			+ "select distinct anatomical_system, emapa_id "
			+ "from expression_result_anatomical_systems)";
		
		ex.executeUpdate(cmd);
		logger.info("Created: " + EMAPA_HEADERS);
		
		createIndex(ex, EMAPA_HEADERS, "anatomical_system", true);
		createIndex(ex, EMAPA_HEADERS, "emapa_id", true);
		
		existingTables.add(EMAPA_HEADERS);
		return EMAPA_HEADERS;
	}
	
	//--- private methods (for Vocab bucket) ---//
	
	// Create and return the name of a table containing one row for each (EMAPA term key, EMAPA ID, and
	// header) for terms with positive annotations below them (computed in a stage-dependent manner).
	private String getEmapaTermsToHeadersTable (SQLExecutor ex) throws SQLException {
		if (existingTables.contains(EMAPA_TERMS_TO_HEADERS)) { return EMAPA_TERMS_TO_HEADERS; }
		
		// To ensure stage-dependent computations, we'll first compute the similar EMAPS table and then
		// digest its data to make this one.
		String emapsTermsToHeaders = this.getEmapsTermsToHeadersTable(ex);

		String cmd = "create temp table " + EMAPA_TERMS_TO_HEADERS + " as ("
			+ "select distinct emapa.term_key, emapa.primary_id, emaps.header "
			+ "from " + emapsTermsToHeaders + " emaps, term_emap stoa, term emapa "
			+ "where emaps.term_key = stoa.term_key "
			+ " and stoa.emapa_term_key = emapa.term_key "
			+ ");";
		
		ex.executeUpdate(cmd);
		logger.info("Created: " + EMAPA_TERMS_TO_HEADERS);
		
		createIndex(ex, EMAPA_TERMS_TO_HEADERS, "term_key", false);
		createIndex(ex, EMAPA_TERMS_TO_HEADERS, "primary_id", false);
		createIndex(ex, EMAPA_TERMS_TO_HEADERS, "header", false);

		existingTables.add(EMAPA_TERMS_TO_HEADERS);
		return EMAPA_TERMS_TO_HEADERS;
	}

	// Create and return the name of a table containing one row for each (EMAPS term key, EMAPS ID, and
	// header) for terms with positive annotations below them.
	private String getEmapsTermsToHeadersTable (SQLExecutor ex) throws SQLException {
		if (existingTables.contains(EMAPS_TERMS_TO_HEADERS)) { return EMAPS_TERMS_TO_HEADERS; }
		
		String emapsTerms = this.getEmapsTermTable(ex);		// EMAPS terms: term key, ID, and expressed flag
		String validHeaders = this.getEmapaHeaderTable(ex);	// valid header terms:  header and emapa_id
		
		// One row per EMAPS term/header pair, but only for those terms with positive annotation results.
		String cmd = "create temp table " + EMAPS_TERMS_TO_HEADERS + " as ("
			+ "select distinct et.term_key, et.primary_id, headers.anatomical_system as header "
			+ "from " + emapsTerms + " et, term_ancestor anc, term_emap mapping, term emapa, " + validHeaders + " headers "
			+ "where et.expressed = 1 "
			+ " and et.term_key = anc.term_key "
			+ " and anc.ancestor_term_key = mapping.term_key "
			+ " and mapping.emapa_term_key = emapa.term_key "
			+ " and emapa.primary_id = headers.emapa_id "
			+ "union "
			+ "select distinct et.term_key, et.primary_id, headers.anatomical_system as header "
			+ "from " + emapsTerms + " et, term_emap mapping, term emapa, " + validHeaders + " headers "
			+ "where et.expressed = 1 "
			+ " and et.term_key = mapping.term_key "
			+ " and mapping.emapa_term_key = emapa.term_key "
			+ " and emapa.primary_id = headers.emapa_id)";

		ex.executeUpdate(cmd);
		logger.info("Created: " + EMAPS_TERMS_TO_HEADERS);
		
		createIndex(ex, EMAPS_TERMS_TO_HEADERS, "term_key", false);
		createIndex(ex, EMAPS_TERMS_TO_HEADERS, "primary_id", false);
		createIndex(ex, EMAPS_TERMS_TO_HEADERS, "header", false);

		existingTables.add(EMAPS_TERMS_TO_HEADERS);
		return EMAPS_TERMS_TO_HEADERS;
	}
	
	// Create and return the name of a table containing one row for each EMAPS term, containing the term,
	// its ID, and a flag to indicate whether it or its descendants have any positive annotations below them.
	private String getEmapsTermTable (SQLExecutor ex) throws SQLException {
		if (existingTables.contains(EMAPS_TERMS)) { return EMAPS_TERMS; }
		
		// Fill the table with a row for each term, assuming none have annotations.
		String cmd = "create temp table " + EMAPS_TERMS + " as ("
			+ "select t.term_key, t.primary_id, 0 as expressed "
			+ "from term t "
			+ "where t.vocab_name = 'EMAPS')";

		ex.executeUpdate(cmd);
		logger.info("Created: " + EMAPS_TERMS);
		
		createIndex(ex, EMAPS_TERMS, "term_key", true);
		createIndex(ex, EMAPS_TERMS, "primary_id", true);
		
		flagClassicalAnnotations(ex, EMAPS_TERMS);
		flagHighThroughputAnnotations(ex, EMAPS_TERMS);
		createIndex(ex, EMAPS_TERMS, "expressed", false);
		
		existingTables.add(EMAPS_TERMS);
		return EMAPS_TERMS;
	}
	
	// Update the given table to flag with a 1 any structures that have positive classical expression
	// annotations, either in that structure or its descendants.
	private void flagClassicalAnnotations(SQLExecutor ex, String table) throws SQLException {
		String cmd = "update " + table + " ha "
			+ "set expressed = 1 "
			+ "where exists (select 1 from expression_result_summary ers "
			+ "  where ha.term_key = ers.structure_key "
			+ "  and ers.is_wild_type = 1 "
			+ "  and ers.is_expressed = 'Yes') "
			+ "or exists (select 1 from term_ancestor ta, expression_result_summary ers "
			+ "  where ha.term_key = ta.ancestor_term_key "
			+ "  and ta.term_key = ers.structure_key "
			+ "  and ers.is_wild_type = 1 "
			+ "  and ers.is_expressed = 'Yes')";

		ex.executeUpdate(cmd);
		logger.info("Flagged terms with classical annotations in: " + table);
	}

	// Update the given table to flag with a 1 any structures that have positive HT expression annotations,
	// either in that structure or its descendants.
	private void flagHighThroughputAnnotations(SQLExecutor ex, String table) throws SQLException {
		String cmd = "update " + table + " ha "
			+ "set expressed = 1 "
			+ "where exists (select 1 from expression_ht_consolidated_sample cs, genotype g, "
			+ "    expression_ht_consolidated_sample_measurement sm, term_emap te "
			+ "  where sm.level in ('High', 'Low', 'Medium') "
			+ "  and cs.consolidated_sample_key = sm.consolidated_sample_key "
			+ "  and cs.genotype_key = g.genotype_key "
			+ "  and (g.combination_1 = '' or g.combination_1 is null) "
			+ "  and ha.term_key = te.term_key "
			+ "  and te.emapa_term_key = cs.emapa_key "
			+ "  and te.stage = cs.theiler_stage::integer "
			+ ") "
			+ "or exists (select 1 from expression_ht_consolidated_sample cs, genotype g, "
			+ "    expression_ht_consolidated_sample_measurement sm, term_emap te, term_ancestor ta "
			+ "  where sm.level in ('High', 'Low', 'Medium') "
			+ "  and cs.consolidated_sample_key = sm.consolidated_sample_key "
			+ "  and cs.genotype_key = g.genotype_key "
			+ "  and (g.combination_1 = '' or g.combination_1 is null) "
			+ "  and ha.term_key = ta.ancestor_term_key "
			+ "  and ta.term_key = te.term_key "
			+ "  and te.emapa_term_key = cs.emapa_key "
			+ "  and te.stage = cs.theiler_stage::integer "
			+ ")";

		ex.executeUpdate(cmd);
		logger.info("Flagged terms with high-throughput annotations in: " + table);
	}

	//--- private methods (for Feature bucket) ---//
	
	// create a temp table containing a row (marker key, header ID, expression flag) for each possible 
	// (marker) x (anatomical system) combination.  Start all expression flags at 0.
	private String getMarkerHeaderMapTable(SQLExecutor ex) throws SQLException {
		if (existingTables.contains(MARKER_HEADER_MAP)) { return MARKER_HEADER_MAP; }
		
		String validHeaders = this.getEmapaHeaderTable(ex);	// valid header terms:  header and emapa_id
		String cmd = "create temp table " + MARKER_HEADER_MAP + " as ( "
			+ "select m.marker_key, h.emapa_id, h.anatomical_system as header, 0 as expressed "
			+ "from marker m, " + validHeaders + " h "
			+ "where exists (select 1 from expression_result_summary ers "
			+ "  where m.marker_key = ers.marker_key "
			+ "  and ers.is_wild_type = 1) "
			+ "or  "
			+ "exists (select 1 from expression_ht_consolidated_sample_measurement csm,  "
			+ "  expression_ht_consolidated_sample cs, genotype g "
			+ "  where csm.level in ('High', 'Low', 'Medium') "
			+ "  and csm.marker_key = m.marker_key "
			+ "  and cs.consolidated_sample_key = csm.consolidated_sample_key "
			+ "  and cs.genotype_key = g.genotype_key "
			+ "  and (g.combination_1 = '' or g.combination_1 is null) "
			+ "  ) "
			+ ")";
		
		ex.executeUpdate(cmd);
		logger.info("Created: " + MARKER_HEADER_MAP);
		
		createIndex(ex, MARKER_HEADER_MAP, "marker_key", false);
		createIndex(ex, MARKER_HEADER_MAP, "emapa_id", false);

		flagClassicalForMarkers(ex, MARKER_HEADER_MAP);
		flagHighThroughputForMarkers(ex, MARKER_HEADER_MAP);

		existingTables.add(MARKER_HEADER_MAP);
		return MARKER_HEADER_MAP;
	}
	
	// In the given table, flag the rows for each (marker, header) pair with positive classical
	// annotations with a 1.
	private void flagClassicalForMarkers(SQLExecutor ex, String table) throws SQLException {
		String headerTable = getEmapsTermsToHeadersTable(ex);
		String cmd = "update " + table + " eh "
			+ "set expressed = 1 "
			+ "where exists (select 1  "
			+ "  from expression_result_summary ers, " + headerTable + " emaps "
			+ "  where ers.marker_key = eh.marker_key "
			+ "  and ers.structure_key = emaps.term_key "
			+ "  and ers.is_wild_type = 1 "
			+ "  and emaps.header = eh.header)";

		ex.executeUpdate(cmd);
		logger.info("Flagged terms with classical annotations in: " + table);
	}
	
	// In the given table, flag the rows for each (marker, header) pair with positive RNA-Seq
	// annotations with a 1.
	private void flagHighThroughputForMarkers(SQLExecutor ex, String table) throws SQLException {
		String headerTable = getEmapsTermsToHeadersTable(ex);
		String cmd = "update " + table + " eh "
			+ "set expressed = 1 "
			+ "where exists (select 1 "
			+ "  from expression_ht_consolidated_sample_measurement csm, genotype g, "
			+ "    expression_ht_consolidated_sample cs, term_emap mapping, " + headerTable + " emaps "
			+ "  where eh.marker_key = csm.marker_key "
			+ "    and csm.level in ('High', 'Low', 'Medium') "
			+ "    and csm.consolidated_sample_key = cs.consolidated_sample_key "
			+ "    and cs.genotype_key = g.genotype_key "
			+ "    and (g.combination_1 = '' or g.combination_1 is null) "
			+ "    and cs.emapa_key = mapping.emapa_term_key "
			+ "    and cs.theiler_stage::int = mapping.stage "
			+ "    and mapping.term_key = emaps.term_key "
			+ "    and emaps.header = eh.header "
			+ ")";

		ex.executeUpdate(cmd);
		logger.info("Flagged terms with high-throughput annotations in: " + table);
	}
}
