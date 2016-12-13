package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * Indexer for the 'disease' index.
 * @author jsb
 */

public class DiseaseIndexerSQL extends Indexer {


	public DiseaseIndexerSQL () {
		super("disease");
	}

	public void index() {

		try {

			logger.info("Getting all disease IDs and keys");
			ResultSet rs_overall = ex.executeProto("select disease_key, primary_id from disease");

			rs_overall.next();

			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			// Parse the main query results here.

			logger.info("Parsing them");
			while (!rs_overall.isAfterLast()) {
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField(IndexConstants.DISEASE_ID, rs_overall.getString("primary_id"));
				doc.addField(IndexConstants.DISEASE_KEY, rs_overall.getString("disease_key"));

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

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
