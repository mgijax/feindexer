package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
	
	private Map<String, List<String>> getPubMedIDs() throws Exception {
		logger.info("getting PubMed IDs for experiments");
		String cmd0 = "select experiment_key, value, sequence_num "
			+ "from expression_ht_experiment_property "
			+ "where name = 'PubMed ID' "
			+ "order by experiment_key, sequence_num";
		Map<String, List<String>> pmIDs = new HashMap<String, List<String>>();

		ResultSet rs = ex.executeProto(cmd0, 1000);
		while (rs.next()) {
			String exptKey = rs.getString("experiment_key");
			if (!pmIDs.containsKey(exptKey)) {
				pmIDs.put(exptKey, new ArrayList<String>());
			}
			pmIDs.get(exptKey).add(rs.getString("value"));
		}
		rs.close();
		logger.info(" - finished. got " + pmIDs.size() + " experiments");
		return pmIDs;
	}
	
	// Get the experiment IDs that have been loaded and are available with
	// the fully-coded classical expression data.
	private Set<String> getLoadedIDs() throws Exception {
		logger.info("getting IDs for loaded experiments");
		String cmd1 = "select distinct e.primary_id "
			+ "from expression_ht_experiment e "
			+ "where exists (select 1 from expression_ht_consolidated_sample s "
			+ "  where e.experiment_key = s.experiment_key)";
		Set<String> loadedIDs = new HashSet<String>();

		ResultSet rs = ex.executeProto(cmd1, 1000);
		while (rs.next()) {
			loadedIDs.add(rs.getString("primary_id"));
		}
		rs.close();
		logger.info(" - finished. got " + loadedIDs.size());
		return loadedIDs;
	}

	public void index() throws Exception {
		logger.info("Beginning index() method");
		
		// look up the PubMed IDs associated with each experiment
		Map<String, List<String>> pmIDs = this.getPubMedIDs();
		
		// look up the IDs of the experiments that have been loaded
		Set<String> loadedIDs = this.getLoadedIDs();

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
			+ " e.description, e.method, e.study_type, o.by_primary_id as sequence_num, "
			+ " e.is_in_atlas "
			+ "from expression_ht_experiment e, expression_ht_experiment_sequence_num o "
			+ "where e.experiment_key = o.experiment_key";
		
		logger.info("Processing experiments...");
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		ResultSet rs = ex.executeProto(cmd5, 1000);

		while (rs.next()) {
			String exptKey = rs.getString("experiment_key");
			String primaryID = rs.getString("arrayexpress_id");
			
			DistinctSolrInputDocument doc = new DistinctSolrInputDocument();
			doc.addField(GxdHtFields.EXPERIMENT_KEY, exptKey);
			doc.addField(GxdHtFields.ARRAYEXPRESS_ID, primaryID);
			doc.addField(GxdHtFields.TITLE, rs.getString("title"));
			doc.addField(GxdHtFields.DESCRIPTION, rs.getString("description"));
			doc.addField(GxdHtFields.STUDY_TYPE, rs.getString("study_type"));
			doc.addField(GxdHtFields.METHOD, rs.getString("method"));
			doc.addField(GxdHtFields.BY_DEFAULT, rs.getString("sequence_num"));
			doc.addField(GxdHtFields.IS_IN_ATLAS, rs.getString("is_in_atlas"));
			
			if (pmIDs.containsKey(exptKey)) {
				doc.addAllDistinct(GxdHtFields.PUBMED_IDS, pmIDs.get(exptKey));
			}
			
			if (loadedIDs.contains(primaryID)) {
				doc.addField(GxdHtFields.IS_LOADED, 1);
			} else {
				doc.addField(GxdHtFields.IS_LOADED, 0);
			}

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
