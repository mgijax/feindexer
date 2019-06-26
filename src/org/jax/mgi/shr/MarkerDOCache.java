package org.jax.mgi.shr;

/* Is: a TermCache that maps from each mouse marker key to a list of associated DO header terms
 */
public class MarkerDOCache extends TermCache {
	// initialize this cache upon instantiation of the object, propagating any Exception
	// raised in the initialization process
	public MarkerDOCache() throws Exception {
		String cmd = "with headers as ( "
			+ "  select t.term_key, h.term_key as header_key, h.term as header "
			+ "  from term t, term_to_header tth, term h "
			+ "  where t.vocab_name = 'Disease Ontology' "
			+ "    and t.term_key = tth.term_Key "
			+ "    and tth.header_term_key = h.term_key "
			+ ") "
			+ "select distinct m.marker_key as object_key, h.header as term "
			+ "from marker m, marker_to_annotation mta, annotation a, headers h "
			+ "where m.organism = 'mouse' and m.status = 'official' "
			+ " and m.marker_key = mta.marker_key "
			+ " and mta.annotation_key = a.annotation_key "
			+ " and mta.annotation_type = 'DO/Marker' "
			+ " and a.term_key = h.term_key "
			+ "order by 1, 2";
		this.populate(cmd);
	}
}
