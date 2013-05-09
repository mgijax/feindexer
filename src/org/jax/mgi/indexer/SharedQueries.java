package org.jax.mgi.indexer;

import java.util.Arrays;
import java.util.List;

/**
 * A class of shared queries, for cases when logic needs to be consistent across multiple indexes
 * @author kstone
 *
 */
public class SharedQueries {
	
	// This list is for querying only, not the autocomplete. It defines which term IDs can be used in queries.
	static String GXD_VOCABULARIES = "('GO', 'Mammalian Phenotype', 'InterPro Domains', 'PIR Superfamily', 'OMIM', 'MouseCyc')";
	
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
}
