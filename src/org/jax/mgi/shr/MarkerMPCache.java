package org.jax.mgi.shr;

/* Is: a TermCache that maps from each mouse marker key to a list of associated MP header terms
 */
public class MarkerMPCache extends TermCache {
	// initialize this cache upon instantiation of the object, propagating any Exception
	// raised in the initialization process
	public MarkerMPCache() throws Exception {
		String cmd = "select m.marker_key as object_key, h.heading_abbreviation as term "
			+ "from marker m, marker_grid_cell c, marker_grid_heading h "
			+ "where m.organism = 'mouse' and m.status = 'official' "
			+ " and m.marker_key = c.marker_key "
			+ " and c.heading_key = h.heading_key "
			+ " and h.grid_name_abbreviation = 'MP' "
			+ " and c.value > 0 "
			+ "order by 1, 2";
		this.populate(cmd);
	}
}
