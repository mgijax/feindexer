package org.jax.mgi.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.jax.mgi.shr.SQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexer
 * @author mhall
 * This is the parent class for all of the indexers, and it supplies some useful 
 * functions that all of the indexers might need.
 * 
 * It also sets up the sql connection, as well as the connection to a solr index, 
 * which is passed to it during construction time.
 */

public abstract class Indexer {

    public CommonsHttpSolrServer server = null;
    public SQLExecutor ex = new SQLExecutor();
    public Properties props = new Properties();
    public Logger logger = LoggerFactory.getLogger(this.getClass());
    
    protected Indexer(String httpPropName) {
      
        logger.info("Setting up the properties");
        
        InputStream in = Indexer.class.getClassLoader().getResourceAsStream("config.props");
        try {
            props.load(in);
            logger.debug(props.toString());
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        
        // Setup the solr connection as configured 
        
        logger.info("Starting to setup the connection");
        
        try { server = new CommonsHttpSolrServer( props.getProperty(httpPropName) );}
        catch (Exception e) {e.printStackTrace();}

        logger.info("Working with index: " + props.getProperty(httpPropName) );
        logger.info("Past the initial connection.");
        
        server.setSoTimeout(100000);  // socket read timeout
        server.setConnectionTimeout(100000);
        server.setDefaultMaxConnectionsPerHost(100);
        server.setMaxTotalConnections(100);
        server.setFollowRedirects(false);  // defaults to false
        server.setAllowCompression(true);
        server.setMaxRetries(1);
        
        try {
            logger.info("Deleting current index.");
            server.deleteByQuery("*:*");
            server.commit();

	    // force actual deletion of records, not just flagging of them
	    // (to save disk space and provide more efficient disk access)
	    server.optimize();
        }
        catch (Exception e) {e.printStackTrace();}
        
    }

    // Convert the counts from actual values to something like a bit.
    
    protected Integer convertCount(Integer count) {
        if (count > 0) {
            return 1;
        }
        else {
            return 0; 
        }
    }
    
    // Create a hashmap, of a key -> hashSet mapping.
    // The hashSet is simply a collection for our 1->N cases.
    
    protected HashMap <String, HashSet <String>> makeHash(String sql, String keyString, String valueString) {
    
        HashMap <String, HashSet <String>> tempMap = new HashMap <String, HashSet <String>> ();
        
        try {
            ResultSet rs = ex.executeProto(sql);         

            String key = null;
            String value = null;
            
            while (rs.next()) {
                key = rs.getString(keyString);
                value = rs.getString(valueString);
                if (tempMap.containsKey(key)) {
                    tempMap.get(key).add(value);
                }
                else {
                    HashSet <String> temp = new HashSet <String> ();
                    temp.add(value);
                    tempMap.put(key, temp);
                }
            }
        } catch (Exception e) {e.printStackTrace();}
        return tempMap;
        
    }
}
