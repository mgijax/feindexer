package org.jax.mgi.shr;

/* Is: a TermCache that maps from each mouse marker key to a list of associated MP header terms
 */
public class MarkerTypeCache extends TermCache {
	// initialize this cache upon instantiation of the object, propagating any Exception
	// raised in the initialization process
	public MarkerTypeCache() throws Exception {
		String cmd = "with mcv_map as ( "
			+ "  select p.ancestor_term_key as parent_key, p.ancestor_term as parent, "
			+ "    c.term_key as child_key, c.term as child "
			+ "  from term c, term_ancestor p "
			+ "  where c.vocab_name = 'Marker Category' "
			+ "    and c.term_key = p.term_key "
			+ "    and c.is_obsolete = 0 "
			+ "), filter_terms as ( "
			+ "  select child_key as child_key, child as term "
			+ "  from mcv_map "
			+ "  where child_key not in (6238159, 6238160, 6238185) "
			+ "  union "
			+ "  select child_key, parent "
			+ "  from mcv_map "
			+ "  where parent_key not in (6238159, 6238160, 6238185) "
			+ ") "
			+ "select m.marker_key as object_key, f.term  "
			+ "from marker m, term t, filter_terms f "
			+ "where m.organism = 'mouse' "
			+ "  and t.vocab_name = 'Marker Category' "
			+ "  and m.marker_subtype = t.term "
			+ "  and t.term is not null "
			+ "  and t.term_key = f.child_key "
			+ "order by 1, 2";
		this.populate(cmd);
	}
}
