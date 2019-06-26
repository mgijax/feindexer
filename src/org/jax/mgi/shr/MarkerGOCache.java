package org.jax.mgi.shr;

/* Is: a TermCache that maps from each mouse marker key to a list of associated GO header terms
 */
public class MarkerGOCache extends TermCache {
	// initialize this cache upon instantiation of the object, propagating any Exception
	// raised in the initialization process
	public MarkerGOCache() throws Exception {
		String cmd = "select m.marker_key as object_key, h.heading_abbreviation as term "
			+ "from marker m, marker_grid_cell c, marker_grid_heading h "
			+ "where m.organism = 'mouse' and m.status = 'official' "
			+ " and m.marker_key = c.marker_key "
			+ " and c.heading_key = h.heading_key "
			+ " and h.grid_name in ('Molecular Function', 'Cellular Component', 'Biological Process') "
			+ " and c.value > 0 "
			+ "order by 1, 2";
		this.populate(cmd);
	}
}
