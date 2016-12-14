package org.jax.mgi.indexer;


/**
 * A class of shared queries, for cases when logic needs to be consistent across multiple indexes
 * @author kstone
 *
 */
public class SharedQueries {
	
	// This list is for querying only, not the autocomplete. It defines which term IDs can be used in queries.
	static String GXD_VOCABULARIES = "('GO', 'Mammalian Phenotype', 'InterPro Domains', 'PIR Superfamily', 'DO', 'MouseCyc')";
	
	// Gets vocab annotation IDs(including children, excluding NOTs) by marker key
	// Also excludes the 3 GO high level terms
	static String GXD_VOCAB_QUERY = "select a.term_id,mta.marker_key "+
    		"from annotation a, marker_to_annotation mta "+
    		"where a.vocab_name in "+GXD_VOCABULARIES+" "+
    		"and a.annotation_key=mta.annotation_key "+
    		"and a.term_id != 'GO:0008150' and a.term_id != 'GO:0003674' and a.term_id != 'GO:0005575' "+
    		"and (a.qualifier is null or a.qualifier not like 'NOT%') ";
	
	// excludes the above query to only markers with expression data
	static String GXD_VOCAB_EXPRESSION_QUERY = GXD_VOCAB_QUERY+
			"and exists(select 1 from expression_result_summary ers where mta.marker_key=ers.marker_key) ";
    
	// get relationships to DO terms for mouse markers which are
	// associated with human markers (via homology) where those human
	// markers have DO annotations.
	static String GXD_DO_HOMOLOGY_QUERY =
		"select a.term_id, m.marker_key "
		+ "from annotation a, "
		+ "  marker_to_annotation mta, "
		+ "  homology_cluster_organism_to_marker hm, "
		+ "  homology_cluster_organism hco, "
		+ "  homology_cluster hc, "
		+ "  homology_cluster_organism mco, "
		+ "  homology_cluster_organism_to_marker mm, "
		+ "  marker m "
		+ "where a.annotation_key = mta.annotation_key "
		+ "  and a.annotation_type = 'DO/Human Marker' "
		+ "  and mta.marker_key = hm.marker_key "
		+ "  and hm.cluster_organism_key = hco.cluster_organism_key "
		+ "  and hco.cluster_key = hc.cluster_key "
		+ "  and hc.cluster_key = mco.cluster_key "
		+ "  and (hc.source like '%HGNC%' and hc.source like '%HomoloGene%') "
		+ "  and mco.cluster_organism_key = mm.cluster_organism_key "
		+ "  and mm.marker_key = m.marker_key "
		+ "  and m.organism = 'mouse'";

	// Gets all the ancestor IDs of each term. (To be combined with the above query)
	static String GXD_VOCAB_ANCESTOR_QUERY = "select t.primary_id,ta.ancestor_primary_id "+
    		"from term t,term_ancestor_simple ta "+
    		"where t.vocab_name in "+GXD_VOCABULARIES+" "+
    		"and t.term_key = ta.term_key ";
	
	// Gets All anatomy term ancestors
	static String GXD_ANATOMY_ANCESTOR_QUERY = "select ta.ancestor_primary_id ancestor_id, "+
			"t.primary_id structure_id, "+
			"t.term_key structure_term_key, "+
			"tae.mgd_structure_key ancestor_mgd_structure_key "+
			"from term t, term_ancestor_simple ta, term_anatomy_extras tae, term ancestor_join "+
			"where t.term_key=ta.term_key and t.vocab_name='Anatomical Dictionary' "+
			"and ta.ancestor_primary_id=ancestor_join.primary_id "+
			"and tae.term_key=ancestor_join.term_key ";
	
	// Gets all the anatomy synonyms by term_id
	static String GXD_ANATOMY_SYNONYMS_QUERY ="select ts.synonym, t.definition structure,t.primary_id structure_id "+
    		"from term t left outer join term_synonym ts on t.term_key=ts.term_key "+
    		"where t.vocab_name='Anatomical Dictionary' ";

	// Gets All anatomy term ancestors (for EMAPA and EMAPS terms).  For
	// EMAPS terms, also gets their corresponding EMAPA terms and the
	// ancestors of those terms.
	static String GXD_EMAP_ANCESTOR_QUERY =

		// Get the ancestors of each EMAPA and EMAPS term, staying
		// within their own vocabularies.

		"select ta.ancestor_primary_id ancestor_id, "+
		    "t.primary_id structure_id, "+
		    "t.term_key structure_term_key, "+
		    "tae.default_parent_key "+
		"from term t, " +
		    "term_ancestor_simple ta, " +
		    "term_emap tae, " +
		    "term ancestor_join "+
		"where t.term_key = ta.term_key " +
		    "and t.vocab_name in ('EMAPA', 'EMAPS') " +
		    "and ta.ancestor_primary_id = ancestor_join.primary_id "+
		    "and tae.term_key = ancestor_join.term_key " +

		// for each EMAPS term, include the EMAPA terms that correspond
		// to its EMAPS ancestors.  (We trace ancestry by EMAPS to
		// ensure that we only follow valid stage-aware paths, then
		// make the jump over to EMAPA.)

		"union " +
		"select emapa.primary_id, " +
		    "emaps.primary_id, " +
		    "emaps.term_key, " +
		    "emapa.term_key " +
		"from term emaps, " +
		    "term_ancestor anc, " +
		    "term_emap te, " +
		    "term emapa, " +
		    "term_emap ae " +
		"where emaps.vocab_name = 'EMAPS' " +
		    "and emaps.term_key = anc.term_key " +
		    "and anc.ancestor_term_key = te.term_key " +
		    "and te.emapa_term_key = emapa.term_key " +
		    "and te.emapa_term_key = ae.term_key " +
		    "and te.stage >= ae.start_stage " +
		    "and te.stage <= ae.end_stage " +

		// include EMAPA term corresponding to each EMAPS term

		"union " +
		"select emapa.primary_id, " +
		    "emaps.primary_id, " +
		    "emaps.term_key, " +
		    "emapa.term_key " +
		"from term emaps, " +
		    "term_emap te, " +
		    "term emapa " +
		"where emaps.vocab_name = 'EMAPS' " +
		    "and emaps.term_key = te.term_key " +
		    "and te.emapa_term_key = emapa.term_key ";

	// Gets all the anatomy synonyms by term_id (for EMAPA and EMAPS terms)
	static String GXD_EMAP_SYNONYMS_QUERY ="select ts.synonym, t.term structure,t.primary_id structure_id "+
    		"from term t left outer join term_synonym ts on t.term_key=ts.term_key "+
    		"where t.vocab_name in ('EMAPA', 'EMAPS') ";
}
