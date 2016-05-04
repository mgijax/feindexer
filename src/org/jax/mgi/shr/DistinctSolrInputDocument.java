package org.jax.mgi.shr;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

/* Is: a SolrInputDocument that keeps only distinct values for each field (no duplicates
 * 	are allowed for multi-valued fields)
 */
public class DistinctSolrInputDocument extends SolrInputDocument {
	/* maps from field name to set of its values */
	Map<String,Set<Object>> cache = new HashMap<String,Set<Object>>();

	/* constructors simply use the superclass */
	public DistinctSolrInputDocument() { super(); }
	public DistinctSolrInputDocument(Map<String, SolrInputField> fields) { super(fields); }

	/* add the given value to 'solrField', if it is not already present for it.
	 * (do not add duplicate values to the same field)
	 */
	public void addDistinctField(String solrField, Object value) {
		if ((solrField == null) || (value == null)) { return; }
		
		boolean toAdd = true;
		
		if (cache.containsKey(solrField)) {
			if (cache.get(solrField).contains(value)) { toAdd = false; }
		} else {
			cache.put(solrField, new HashSet<Object>());
		}
		
		if (toAdd) {
			cache.get(solrField).add(value);
			this.addField(solrField, value);
		}
	}
	
	/* add the given set of 'values' to 'solrField', ensuring that we only keep a single
	 * copy of each value added to the field.
	 */
	public void addAllDistinct(String solrField, Collection<Object> values) {
		if ((solrField == null) || (values == null)) { return; }
		for (Object value : values) {
			this.addDistinctField(solrField, value);
		}
	}
	
	/* similar method to above, but for sets of strings
	 */
	public void addAllDistinct(String solrField, Set<String> values) {
		if ((solrField == null) || (values == null)) { return; }
		for (String value : values) {
			this.addDistinctField(solrField, value);
		}
	}

	/* similar method to above, but for lists of strings
	 */
	public void addAllDistinct(String solrField, List<String> values) {
		if ((solrField == null) || (values == null)) { return; }
		for (String value : values) {
			this.addDistinctField(solrField, value);
		}
	}

	/* add 'value' for 'solrField', if both are non-null.  if either is null, is a no-op.
	 */
	public void addField(String solrField, Object value) {
		if ((solrField == null) || (value == null)) { return; }
		super.addField(solrField, value);
	}
}
