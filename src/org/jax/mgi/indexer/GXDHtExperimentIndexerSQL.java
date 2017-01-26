package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.DistinctSolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.GxdHtFields;

/** Is: the indexer for high-throughput expression experiments
 *  Does: populates the gxdHtExperiment solr index
 *  Notes: added during the GXD HT project (TR12370)
 */
public class GXDHtExperimentIndexerSQL extends Indexer {
	public GXDHtExperimentIndexerSQL() {
		super("gxdHtExperiment");
	}

	// return the first String appearing in 'items', or null if empty
	private String getFirst(Set<String> items) {
		if ((items == null) || (items.size() == 0)) {
			return null;
		}
		Iterator<String> it = items.iterator();
		return it.next();
	}
	
	public void index() throws Exception {
		logger.info("Beginning index() method");
		
		// look up sample count for each experiment
		String cmd1 = "select e.experiment_key, count(distinct s.sample_key) as sample_count "
			+ "from expression_ht_experiment e "
			+ "left outer join expression_ht_sample s on (e.experiment_key = s.experiment_key) "
			+ "group by 1";
		HashMap<String, HashSet<String>> sampleCounts = makeHash(cmd1, "experiment_key", "sample_count");
		logger.info("Retrieved " + sampleCounts.size() + " sample counts");
		
		// look up GEO ID for each experiment
		String cmd2 = "select experiment_key, acc_id "
			+ "from expression_ht_experiment_id "
			+ "where logical_db = 'GEO Series'";
		HashMap<String, HashSet<String>> geoIDs = makeHash(cmd2, "experiment_key", "acc_id");
		logger.info("Retrieved GEO IDs for " + geoIDs.size() + " experiments");
		
		// look up experimental variables for each experiment
		String cmd3 = "select experiment_key, variable from expression_ht_experiment_variable";
		HashMap<String, HashSet<String>> variables = makeHash(cmd3, "experiment_key", "variable");
		logger.info("Retrieved experimental variables for " + variables.size() + " experiments");
		
		// look up note(s) for each experiment
		String cmd4 = "select experiment_key, note from expression_ht_experiment_note";
		HashMap<String, HashSet<String>> notes = makeHash(cmd4, "experiment_key", "note");
		logger.info("Retrieved experiment notes for " + notes.size() + " experiments");
		
		// main query including pre-computed sequence num
		String cmd5 = "select e.experiment_key, e.primary_id as arrayexpress_id, e.name as title, "
			+ " e.description, e.method, e.study_type, o.by_primary_id as sequence_num "
			+ "from expression_ht_experiment e, expression_ht_experiment_sequence_num o "
			+ "where e.experiment_key = o.experiment_key";
		
		logger.info("Processing experiments...");
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		ResultSet rs = ex.executeProto(cmd5, 1000);
		rs.next();
		
		while (!rs.isAfterLast()) {
			String exptKey = rs.getString("experiment_key");
			
			DistinctSolrInputDocument doc = new DistinctSolrInputDocument();
			doc.addField(GxdHtFields.EXPERIMENT_KEY, exptKey);
			doc.addField(GxdHtFields.ARRAYEXPRESS_ID, rs.getString("arrayexpress_id"));
			doc.addField(GxdHtFields.TITLE, rs.getString("title"));
			doc.addField(GxdHtFields.DESCRIPTION, rs.getString("description"));
			doc.addField(GxdHtFields.STUDY_TYPE, rs.getString("study_type"));
			doc.addField(GxdHtFields.METHOD, rs.getString("method"));
			doc.addField(GxdHtFields.BY_DEFAULT, rs.getString("sequence_num"));
			
			if (sampleCounts.containsKey(exptKey)) {
				doc.addAllDistinct(GxdHtFields.SAMPLE_COUNT, sampleCounts.get(exptKey));
			} else {
				doc.addField(GxdHtFields.SAMPLE_COUNT, "0");
			}
			
			if (geoIDs.containsKey(exptKey)) {
				doc.addField(GxdHtFields.GEO_ID, getFirst(geoIDs.get(exptKey)));
			}
			
			if (variables.containsKey(exptKey)) {
				doc.addAllDistinct(GxdHtFields.EXPERIMENTAL_VARIABLES, variables.get(exptKey));
			}
			
			if (notes.containsKey(exptKey)) {
				doc.addAllDistinct(GxdHtFields.NOTE, notes.get(exptKey));
			}
			rs.next();
			
			docs.add(doc);
			if (docs.size() > 10000) {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
				logger.info("Added stack of docs to Solr");
			}
		}
		rs.close();
		logger.info("Writing final batch of docs to Solr");
		writeDocs(docs);
		commit();
		logger.info("Done");
	}
}
