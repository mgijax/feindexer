package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * The template indexer
 * @author mhall
 * Copy this code to create a new indexer, and then just change the appropriate sections.
 * 
 * If you need chunking go and take the code from the sequence indexer.
 * 
 * Note: refactored during 5.x development
 */

public class MarkerPanesetIndexerSQL extends Indexer {


	public MarkerPanesetIndexerSQL () {
		super("index.url.markerPanesetImage");
	}

	public void index() throws Exception
	{
		logger.info("Seleceting all imagepanet set for markers");
		String imagesToMrkSQL = "select distinct marker_key, paneset_key " + 
				" from expression_imagepane_set order by paneset_key";
		HashMap <String, HashSet <String>> imagesToMarkers = makeHash(imagesToMrkSQL, "paneset_key", "marker_key");


		// Get the image paneset for markers

		logger.info("Getting marker images");
		ResultSet rs_overall = ex.executeProto("select distinct s.paneset_key, n.by_default " + 
				"from expression_imagepane_set s, image_sequence_num n " + 
				"where s.image_key = n.image_key " +
				"and s.in_pixeldb = 1");

		rs_overall.next();

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		// TODO Parse the main query results here.

		logger.info("Parsing them");
		while (!rs_overall.isAfterLast()) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.PANESET_KEY, rs_overall.getString("paneset_key"));
			doc.addField(IndexConstants.BY_DEFAULT, rs_overall.getString("by_default"));


			if (imagesToMarkers.containsKey(rs_overall.getString("paneset_key"))) {
				for (String markerKey: imagesToMarkers.get(rs_overall.getString("paneset_key"))) {
					doc.addField(IndexConstants.MRK_KEY, markerKey);
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
