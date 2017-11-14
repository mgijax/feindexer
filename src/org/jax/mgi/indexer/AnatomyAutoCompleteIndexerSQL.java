package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * This indexer populates the anatomyAC index, which drives the search box on
 * the new fewi-based anatomy browser.  This index includes the following data
 * for the EMAPA vocabulary (not EMAPS):
 * 	structureKey
 * 	accID
 * 	structure
 * 	synonym (multi-valued)
 * 	start stage
 * 	end stage
 *  crossRef (cross-referenced IDs, currently from MP)
 * @author jsb
 */

public class AnatomyAutoCompleteIndexerSQL extends Indexer {


	public AnatomyAutoCompleteIndexerSQL () {
		super("anatomyAC");
	}

	public void index() throws Exception {
		// Since we're only dealing with a single vocabulary, we'll try to
		// process it all in one fell swoop, rather than dealing with
		// chunking.

		// Get a mapping from each EMAPA structure key to its synonyms.

		logger.info("Getting EMAPA synonyms");
		String synonymSQL = "select t.term_key, s.synonym "
				+ "from term t, term_synonym s "
				+ "where t.term_key = s.term_key "
				+ " and t.vocab_name = 'EMAPA' "
				+ " and t.is_obsolete = 0";

		HashMap <String, HashSet <String>> structureToSynonyms =
				makeHash(synonymSQL, "term_key", "synonym");

		// get a mapping from each EMAPA structure key to its cross-reference IDs (if any)
		
		logger.info("Getting cross-references");
		String crossRefSQL = "select tt.term_key_2 as term_key, e.primary_id as crossRef " + 
				"from term_to_term tt, term e " + 
				"where tt.relationship_type = 'MP to EMAPA' " + 
				"and tt.term_key_1 = e.term_key";
		
		HashMap <String, HashSet <String>> structureToCrossRefs = makeHash(crossRefSQL, "term_key", "crossRef");
		
		// get the main EMAPA term data

		logger.info("Getting all EMAPA structures");
		ResultSet rs_overall = ex.executeProto(
				"select t.term_key, t.term, t.primary_id, "
						+ " e.start_stage, e.end_stage "
						+ "from term t, term_emap e "
						+ "where t.term_key = e.term_key "
						+ " and t.vocab_name = 'EMAPA' "
						+ " and t.is_obsolete = 0");

		rs_overall.next();

		// collection of Solr documents
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		logger.info("Parsing EMAPA structures");
		String termKey;

		while (!rs_overall.isAfterLast()) {
			termKey = rs_overall.getString("term_key");

			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.STRUCTUREAC_KEY, termKey);
			doc.addField(IndexConstants.ACC_ID, rs_overall.getString("primary_id"));
			doc.addField(IndexConstants.STRUCTUREAC_STRUCTURE, rs_overall.getString("term"));
			doc.addField(IndexConstants.STRUCTUREAC_START_STAGE, rs_overall.getString("start_stage"));
			doc.addField(IndexConstants.STRUCTUREAC_END_STAGE, rs_overall.getString("end_stage"));

			if (structureToSynonyms.containsKey(termKey)) {
				for (String synonym: structureToSynonyms.get(termKey)) {
					doc.addField(IndexConstants.STRUCTUREAC_SYNONYM, synonym);
				}
			}
			
			if (structureToCrossRefs.containsKey(termKey)) {
				for (String crossRef: structureToCrossRefs.get(termKey)) {
					doc.addField(IndexConstants.STRUCTUREAC_CROSSREF, crossRef);
				}
			}

			rs_overall.next();

			docs.add(doc);

			if (docs.size() > 10000) {
				logger.info("Adding a stack of the documents to Solr");
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
				logger.info("Done adding to solr, Moving on");
			}
		}
		writeDocs(docs);
		commit();
	}
}
