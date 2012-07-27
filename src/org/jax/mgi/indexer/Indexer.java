package org.jax.mgi.indexer;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.common.SolrInputDocument;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
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

    public StreamingUpdateSolrServer server = null;
    public SQLExecutor ex = new SQLExecutor();
    public Properties props = new Properties();
    public Logger logger = LoggerFactory.getLogger(this.getClass());
    private String httpPropName = "";
    
    // Variables for handling threads
    private List<Thread> currentThreads =new ArrayList<Thread>();
    // maxThreads is configurable. When maxThreads is reached, program waits until they are finished.
    // This is essentially running them in batches
    private int maxThreads = 40;
    
    protected Indexer(String httpPropName) {
      this.httpPropName = httpPropName;
    }
    
    protected void setupConnection()
    {
        logger.info("Setting up the properties");
        
        InputStream in = Indexer.class.getClassLoader().getResourceAsStream("config.props");
        if (in== null)
        {
        	logger.info("resource config.props not found");
        }
        try {
            props.load(in);
            logger.debug(props.toString());
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        
        // Setup the solr connection as configured 
        
        logger.info("Starting to setup the connection");
//        MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
//
//        HttpClient client = new HttpClient(mgr);

        try { server = new StreamingUpdateSolrServer( props.getProperty(httpPropName),100,40 );}
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
        // set to use javabin format for faster indexing
        server.setRequestWriter(new BinaryRequestWriter());
        
        
        try {
            logger.info("Deleting current index.");
            server.deleteByQuery("*:*");
            server.commit();
        }
        catch (Exception e) {e.printStackTrace();}
    }
    
    /*
     * Code for loading a solr index must be implemented here
     */
    abstract void index() throws IOException;
    
    // Convert the counts from actual values to something like a bit.
    //TODO: WTF is this for anyway?
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
    //TODO: This does not belong in this class. It's a straight up utility function
    
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
    

    public void setMaxThreads(int maxThreads)
    {
    	this.maxThreads = maxThreads;
    }
    
    public void writeDoc(SolrInputDocument doc)
    {
    	try {
			server.add(doc,50000);
		} catch (SolrServerException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
    }
    /*
     * writes documents to solr.
     * Best practice is to write small batches of documents to Solr
     * and to commit less frequently.
     * Here we also spawn a new process for each batch of documents.
     */
    public void writeDocs(Collection<SolrInputDocument> docs)
    {
    	DocWriterThread docWriter = new DocWriterThread(server,docs);
    	Thread newThread = new Thread(docWriter);
    	// kick off the thread
    	newThread.start();
    	// add to list of threads to monitor
    	currentThreads.add(newThread);
    	if(currentThreads.size() >= maxThreads)
    	{
    		// max thread pool size reached. Wait until they all finish, and clear the pool
    		logger.info("Max threads ("+maxThreads+") reached. Waiting for all threads to finish.");
    		for(Thread t : currentThreads)
    		{
    			try {
					t.join();
				} catch (InterruptedException e) {
					// not quite sure what to do here. Let's hope we don't see this error.
					logger.error(e.getMessage());
					e.printStackTrace();
				}
    		}
    		currentThreads = new ArrayList<Thread>();
    	}
    }
    
    /*
     * Add documents to Solr in a thread to improve load time
     * This was investigated to be a bottleneck in large data loads
     */
    class DocWriterThread implements Runnable
    {
    	CommonsHttpSolrServer server;
    	Collection<SolrInputDocument> docs;
    	int times_to_retry = 2;
    	int times_retried = 0;
    	public DocWriterThread(CommonsHttpSolrServer server,Collection<SolrInputDocument> docs)
    	{
    		this.server=server;
    		this.docs=docs;
    	}
    	
    	public void run()
    	{
    		try {
    			// set the commitWithin feature to 50 seconds
    			server.add(docs,50000);
    		} catch (SolrServerException e) {
    			logger.error(e.getMessage());
    			e.printStackTrace();
    			retry();
    		} catch (IOException e) {
    			logger.error(e.getMessage());
    			e.printStackTrace();
    		}
    	}
    	public void retry()
    	{
    		if(times_retried < times_to_retry)
    		{
    			times_retried ++;
    			run();
    		}
    		else
    		{
    			logger.error("tried to re-submit stack of documents "+times_retried+" times. Giving up.");
    		}
    	}
    }
}
