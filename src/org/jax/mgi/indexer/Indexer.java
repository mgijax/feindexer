package org.jax.mgi.indexer;

import java.io.IOException;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.solr.common.SolrException;
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
import org.apache.solr.client.solrj.impl.HttpSolrServer;
//import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.jax.mgi.shr.SQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexer
 * @author kstone
 * This is the parent class for all of the indexers, and it supplies some useful 
 * functions that all of the indexers might need.
 * 
 * It also sets up the sql connection, as well as the connection to a solr index, 
 * which is passed to it during construction time.
 */

public abstract class Indexer {

    public HttpSolrServer server = null;
    public SQLExecutor ex = new SQLExecutor();
    public Properties props = new Properties();
    public Logger logger = LoggerFactory.getLogger(this.getClass());
    private String httpPropName = "";
    private boolean THREAD_LOG=true;
    private int failedThreads=0;
    
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
        logger.info("db connection info: "+this.ex);
        
        // Setup the solr connection as configured 
        
        logger.info("Starting to setup the connection");
        
        // We start up a CommonsHttpSolrServer using a MultiThreaded connection manager so that we can do bulk updates 
        // by using threads.
        // (kstone) NOTE: Supposedly the StreamingUpdateSolrServer does this kind of threading for you, but I could not get it to work
        // 	without either crashing unexpectedly, or running much slower. So good luck to anyone who tries to figure out that approach.
        ThreadSafeClientConnManager mgr = new ThreadSafeClientConnManager();
        DefaultHttpClient client = new DefaultHttpClient(mgr);
        
        try { server = new HttpSolrServer( props.getProperty(httpPropName),client );}
        catch (Exception e) {e.printStackTrace();}

        logger.info("Working with index: " + props.getProperty(httpPropName)+"/update" );
        logger.info("Past the initial connection.");
        
        server.setSoTimeout(100000);  // socket read timeout
        server.setConnectionTimeout(100000);
        //server.setDefaultMaxConnectionsPerHost(100);
        //server.setMaxTotalConnections(100);
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
    abstract void index() throws Exception;
    
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
    
    /*
     * writes documents to solr.
     * Best practice is to write small batches of documents to Solr
     * and to commit less frequently. (TIP: this method will commit documents automatically using commitWithin)
     * Here we also spawn a new process for each batch of documents.
     */
    public void writeDocs(Collection<SolrInputDocument> docs)
    {
    	DocWriterThread docWriter = new DocWriterThread(this,docs);
    	Thread newThread = new Thread(docWriter);
    	// kick off the thread
    	newThread.start();
    	// add to list of threads to monitor
    	currentThreads.add(newThread);
    	if(currentThreads.size() >= maxThreads)
    	{
    		// max thread pool size reached. Wait until they all finish, and clear the pool
    		if(THREAD_LOG)
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
    /* Prevents logging information related to threading */
    public void stopThreadLogging()
    {
    	THREAD_LOG=false;
    }
    
    
    @Override
	public String toString() { return this.getClass().toString(); }

	/*
     * Add documents to Solr in a thread to improve load time
     * This was investigated to be a bottleneck in large data loads
     */
    class DocWriterThread implements Runnable
    {
    	HttpSolrServer server;
    	Collection<SolrInputDocument> docs;
    	private int commitWithin = 50000; // 50 seconds
    	private int times_to_retry = 5;
    	private int times_retried = 0;
    	private Indexer idx;
    	
    	public DocWriterThread(Indexer idx,Collection<SolrInputDocument> docs)
    	{
    		this.server=idx.server;
    		this.docs=docs;
    		this.idx=idx;
    	}
    	
    	public void run()
    	{
    		try {
    			// set the commitWithin feature
    			server.add(docs,commitWithin);
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
    			logger.info("retrying submit of stack of documents that failed");
    			boolean succeeded = true;
    			try {
        			// set the commitWithin feature
        			server.add(docs,commitWithin);
        		} catch (SolrServerException e) {
        			succeeded = false;
        			logger.error("failed");
        			e.printStackTrace();
        			retry();
        		} catch (IOException e) {
        			succeeded = false;
        			logger.error(e.getMessage());
        			e.printStackTrace();
        			retry();
        		}
    			catch (Exception e){
    				succeeded = false;
        			logger.error(e.getMessage());
        			e.printStackTrace();
        			// don't know what this exception is. Not retrying
    			}
    			if(succeeded) logger.info("succeeded!");
    			else reportFailure();
    		}
    		else
    		{
    			logger.error("tried to re-submit stack of documents "+times_retried+" times. Giving up.");
    			reportFailure();
    		}
    	}
    	public void reportFailure()
    	{
    		this.idx.reportThreadFailure();
    	}
    }
    
    // used by threads to alert when a thread failed.
    public void reportThreadFailure()
    {
    	this.failedThreads+=1;
    }
    // returns true if any threads reported a failure
    public boolean hasFailedThreads()
    {
    	return this.failedThreads>0;
    }
}
