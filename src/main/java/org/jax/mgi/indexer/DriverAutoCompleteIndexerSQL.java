package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.CreFields;

/**
 * DriverAutoCompleteIndexerSQL
 * This index is has the primary responsibility of populating the driver autocomplete index.
 */

public class DriverAutoCompleteIndexerSQL extends Indexer {

	/* configure indexer to point at the correct index */
	public DriverAutoCompleteIndexerSQL () {
		super("driverAC");
	}

	public void index() throws Exception {
		logger.info("Getting all distinct drivers");

		ResultSet rs_overall = ex.executeProto(
                    "select cluster_key, driver from driver order by cluster_key, driver");

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

                //
                Map<Integer,Set<String>> ck2uniqLC = new HashMap<Integer,Set<String>>();
                Map<Integer,Set<String>> ck2uniq   = new HashMap<Integer,Set<String>>();

		// distill down to unique lowercase drivers, but keep display values
		logger.info("Parsing drivers to unique map");
		while (rs_overall.next()) {
                        Integer clusterKey = rs_overall.getInt("cluster_key");
			String driver = rs_overall.getString("driver");
			String lowerCaseDriver = driver.toLowerCase();
			if(!ck2uniq.containsKey(clusterKey)){
                            ck2uniq.put(clusterKey, new HashSet<String>());
                            ck2uniqLC.put(clusterKey, new HashSet<String>());
                        }
                        ck2uniq.get(clusterKey).add(driver);
                        ck2uniqLC.get(clusterKey).add(lowerCaseDriver);
		}

		// add documents to solr

                for(Integer clusterKey : ck2uniq.keySet()) {
                    Set<String> uniqDrivers = ck2uniq.get(clusterKey);
                    Set<String> uniqLowerCaseDrivers = ck2uniqLC.get(clusterKey);
                    //
                    String displayString = "";
                    for(String driver : uniqDrivers) {
                        if (displayString.length() > 0) {
                            displayString += ", ";
                        }
                        displayString += driver;
                    }
                    for(String lowerCaseDriver : uniqLowerCaseDrivers) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(CreFields.DRIVER, lowerCaseDriver );
			doc.addField(CreFields.DRIVER_DISPLAY, displayString );
			docs.add(doc);
                    }
                }

		logger.info("Adding the documents to the index.");

		writeDocs(docs);
		commit();
	}
}
