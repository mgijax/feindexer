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
}