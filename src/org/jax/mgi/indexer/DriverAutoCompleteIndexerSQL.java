package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.CreFields;

/**
 * DriverAutoCompleteIndexerSQL
 * This index is has the primary responsibility of populating the driver autocomplete index.
 */

public class DriverAutoCompleteIndexerSQL extends Indexer {

   /* configure indexer to point at the correct index */
    public DriverAutoCompleteIndexerSQL () {
        super("index.url.driverAC");
    }

    public void index() throws Exception
    {
             logger.info("Getting all distinct drivers");

             ResultSet rs_overall = ex.executeProto("select driver " +
				"from driver order by driver ");

             Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

              // Map<lowered driver><driverDisplay>
              Map<String,String> driverMap = new HashMap<String,String>();

             // distill down to unique lowercase drivers, but keep display values
             logger.info("Parsing drivers to unique map");
             while (rs_overall.next()) {
				 String driver = rs_overall.getString("driver");
				 String lowerCaseDriver = driver.toLowerCase();
                 if(driverMap.containsKey(lowerCaseDriver)){
	                 driverMap.put(lowerCaseDriver , driverMap.get(lowerCaseDriver) + ", " + driver);
				 } else {
	                 driverMap.put(lowerCaseDriver , driver);
				 }
             }

             // add documents to solr
             for(String driver : driverMap.keySet()) {
                 SolrInputDocument doc = new SolrInputDocument();
                 doc.addField(CreFields.DRIVER, driver );
                 doc.addField(CreFields.DRIVER_DISPLAY, driverMap.get(driver) );
                 docs.add(doc);
			 }


             logger.info("Adding the documents to the index.");

             server.add(docs);
             server.commit();
    }
}
