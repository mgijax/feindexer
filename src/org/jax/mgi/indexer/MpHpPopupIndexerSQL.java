package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * Indexer for MP HP Popup (from HDMC)
 */

public class MpHpPopupIndexerSQL extends Indexer {


	public MpHpPopupIndexerSQL () {
		super("mpHpPopup");
	}

	public void index() throws Exception
	{
		logger.info("Selecting all associations from term_to_term");
		ResultSet rs_overall = ex.executeProto("SELECT " +
                " t2t.relationship_type,  " +
                " t2t.evidence,  " +
                " t2t.cross_reference, " +
                " t1.primary_id as searchID, " +
                " t2.primary_id as matchID, " +
                " t1.term as searchTerm, " +
                " t2.term as matchTerm, " +
                " t2.definition " +
                "FROM term_to_term t2t, term t1, term t2 " +
                "WHERE relationship_type = 'MP HP Popup' " +
                "  AND t2t.term_key_1 = t1.term_key " +
                "  AND t2t.term_key_2 = t2.term_key " +
                "ORDER BY " +
                "  CASE evidence " +
                "      WHEN 'LexicalMatching' THEN 1 " +
                "      WHEN 'LogicalReasoning' THEN 2 " +
                "      WHEN 'ManualMappingCuration' THEN 3 " +
                "      ELSE 4 " +
                "      END, " +
                "   CASE cross_reference " +
                "      WHEN 'relatedMatch' THEN 1 " +
                "      WHEN 'broadMatch' THEN 2 " +
                "      WHEN 'narrowMatch' THEN 3 " +
                "      WHEN 'closeMatch' THEN 4 " +
                "      WHEN 'exactMatch' THEN 5 " +
                "      ELSE 6 " +
                "      END "); 

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		logger.info("Creating Docs");
		while (rs_overall.next()) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("uniqueKey", rs_overall.getString("searchID") + "-" + rs_overall.getString("matchID"));
			doc.addField("searchTermID", rs_overall.getString("searchID"));
			doc.addField("searchTerm", rs_overall.getString("searchTerm"));
			doc.addField("matchTermID", rs_overall.getString("matchID"));
			doc.addField("matchTerm", rs_overall.getString("matchTerm"));
			doc.addField("matchType", rs_overall.getString("cross_reference"));
			doc.addField("matchMethod", rs_overall.getString("evidence"));
			doc.addField("matchTermDefinition", rs_overall.getString("definition"));
			doc.addField("matchTermSynonym", "matchTermSynonym");
			docs.add(doc);
		}
		logger.info("Created Docs: " + docs.size());


		writeDocs(docs);
		commit();

	}
}
