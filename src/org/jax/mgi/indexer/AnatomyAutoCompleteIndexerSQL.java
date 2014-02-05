package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.reporting.Timer;

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
 * @author jsb
 */

public class AnatomyAutoCompleteIndexerSQL extends Indexer {

   
    public AnatomyAutoCompleteIndexerSQL () {
        super("index.url.anatomyAC");
    }

    public void index() throws Exception
    {
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

            System.out.println(synonymSQL);
            HashMap <String, HashSet <String>> structureToSynonyms =
		makeHash(synonymSQL, "term_key", "synonym");
            
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
            Collection<SolrInputDocument> docs =
		new ArrayList<SolrInputDocument>();
            
            // TODO Parse the main query results here.
            
            logger.info("Parsing EMAPA structures");
	    String termKey;

            while (!rs_overall.isAfterLast()) {
		termKey = rs_overall.getString("term_key");

                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("structureKey", termKey);
                doc.addField("accID", rs_overall.getString("primary_id"));
                doc.addField("structure", rs_overall.getString("term"));
                doc.addField("startStage", rs_overall.getString("start_stage"));
                doc.addField("endStage", rs_overall.getString("end_stage"));

                if (structureToSynonyms.containsKey(termKey)) {
                    for (String synonym: structureToSynonyms.get(termKey)) {
                        doc.addField("synonym", synonym);
                    }
                }

                rs_overall.next();
                
                docs.add(doc);
                
                if (docs.size() > 10000) {
                    logger.info("Adding a stack of the documents to Solr");
                    server.add(docs);
                    docs = new ArrayList<SolrInputDocument>();
                    logger.info("Done adding to solr, Moving on");
                }
            }
            server.add(docs);
            server.commit();
    }
}
