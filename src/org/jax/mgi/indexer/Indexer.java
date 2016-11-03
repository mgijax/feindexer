package org.jax.mgi.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
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

	private HttpSolrServer server = null;
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
	private int maxThreads = 10;

	protected Indexer(String httpPropName) {
		this.httpPropName = httpPropName;
	}

	public void setupConnection() throws Exception {
		logger.info("Setting up the properties");

		InputStream in = Indexer.class.getClassLoader().getResourceAsStream("config.props");
		if (in== null) {
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
		PoolingClientConnectionManager mgr = new PoolingClientConnectionManager();
		DefaultHttpClient client = new DefaultHttpClient(mgr);

		String httpUrl = props.getProperty(httpPropName);
		if(httpUrl==null) httpUrl = httpPropName;
		server = new HttpSolrServer( httpUrl,client );

		logger.info("Working with index: " + props.getProperty(httpPropName)+"/update" );
		logger.info("Past the initial connection.");

		server.setSoTimeout(100000);  // socket read timeout
		server.setConnectionTimeout(200000);	// upped to avoid IOExceptions
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
			commit();
		}
		catch (Exception e) {e.printStackTrace();}
	}

	/*
	 * Code for loading a solr index must be implemented here
	 */
	abstract void index() throws Exception;

	
	// closes down the connection and makes sure a last commit is run
	public void closeConnection() {
		
		logger.info("Waiting for Threads to finish: ");
		
		for(Thread t : currentThreads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		commit();
		
		logger.info("Solr Documents are flushed to the server shuting down: " + props.getProperty(httpPropName));
		
	}
	
	public void commit() {
		try {
			logger.info("Waiting for Solr Commit");
			server.commit(true, true);
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
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

	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	/*
	 * writes documents to solr.
	 * Best practice is to write small batches of documents to Solr
	 * and to commit less frequently. (TIP: this method will commit documents automatically using commitWithin)
	 * Here we also spawn a new process for each batch of documents.
	 */
	public void writeDocs(Collection<SolrInputDocument> docs) {
		if(docs == null || docs.size() == 0) return;
		
		DocWriterThread docWriter = new DocWriterThread(this,docs);
		Thread newThread = new Thread(docWriter);
		// kick off the thread
		newThread.start();
		// add to list of threads to monitor
		currentThreads.add(newThread);
		if(currentThreads.size() >= maxThreads) {
			// max thread pool size reached. Wait until they all finish, and clear the pool
			if(THREAD_LOG)
				logger.info("Max threads ("+maxThreads+") reached. Waiting for all threads to finish.");
			for(Thread t : currentThreads) {
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
	public void stopThreadLogging() {
		THREAD_LOG=false;
	}

	@Override
	public String toString() {
		return getClass().toString();
	}

	/*
	 * Add documents to Solr in a thread to improve load time
	 * This was investigated to be a bottleneck in large data loads
	 */
	class DocWriterThread implements Runnable {
		private final HttpSolrServer server;
		Collection<SolrInputDocument> docs;
		private final int commitWithin = 50000; // 50 seconds
		private final int times_to_retry = 5;
		private int times_retried = 0;
		private final Indexer idx;

		public DocWriterThread(Indexer idx,Collection<SolrInputDocument> docs) {
			this.server=idx.server;
			this.docs=docs;
			this.idx=idx;
		}

		public void run() {
			try {
				// set the commitWithin feature
				server.add(docs,commitWithin);
			} catch (SolrServerException e) {
				logger.warn(e.getMessage());
				e.printStackTrace();
				retry();
			} catch (IOException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		public void retry() {
			if(times_retried < times_to_retry) {
				times_retried ++;
				logger.info("retrying submit of stack of documents that failed");
				boolean succeeded = true;
				try {
					// set the commitWithin feature
					server.add(docs,commitWithin);
				} catch (SolrServerException e) {
					succeeded = false;
					logger.warn("failed");
					e.printStackTrace();
					retry();
				} catch (IOException e) {
					succeeded = false;
					logger.warn(e.getMessage());
					e.printStackTrace();
					retry();
				} catch (Exception e) {
					succeeded = false;
					logger.error(e.getMessage());
					e.printStackTrace();
					// don't know what this exception is. Not retrying
				}
				if(succeeded) logger.info("succeeded!");
				else reportFailure();
			} else {
				logger.error("tried to re-submit stack of documents "+times_retried+" times. Giving up.");
				reportFailure();
			}
		}
		
		public void reportFailure() {
			this.idx.reportThreadFailure();
		}
	}

	// used by threads to alert when a thread failed.
	public void reportThreadFailure() {
		this.failedThreads+=1;
	}
	
	// returns true if any threads reported a failure
	public boolean hasFailedThreads() {
		return this.failedThreads>0;
	}


	/*
	 * The following are convenience methods for populating lookups to be used in generating multiValued fields
	 *  A String -> Set<String> map is always returned. This is to keep all terms unique, which leads to faster indexing in Solr.
	 *  
	 * Example Usage:
	 * 	 String termSynonymQuery="select t.primary_id term_id,ts.synonym "+
	 *		"from term t,term_synonym ts "+
	 *			"where t.term_key=ts.term_key " +
	 *				"and t.vocab_name in ('OMIM','Mammalian Phenotype') ";
	 *
	 *	 Map<String,Set<String>> synonymLookup = populateLookup(termSynonymQuery,"term_id","synonym","synonyms to term IDs");
	 *
	 * You may also pass in an existing map to add to it
	 *   Map<String,Set<String>> synonymLookup = populateLookup(extraTermSynonymQuery,"term_id","synonym","extra synonyms to term IDs",synonymLookup);
	 *   
	 *   When no map is passed in, by default you will get back HashMap<String,HashSet<String>> as a return type.
	 *   You may pass in a different type of Map<String,Set<String>> if you want to use either a different Map implementation, or a different Set implementation
	 *   	However, if the Set class is not passed in, a HashSet will be used (this is a limitation on java reflection)
	 *   Example:
	 *   	 Map<String,Set<String>> orderedSynonymLookup = 
	 *   		populateLookup(termSynonymQuery,"term_id","synonym","synonyms to term IDs",
	 *   			new HashMap<String,LinkedHashSet<String>>(),LinkedHashSet.class);
	 *   
	 *   Alternatively, use the shortcut methods populateLookupOrdered() if you want the default to be LinkedHashSet
	 */
	protected Map<String,Set<String>> populateLookup(String query,String uniqueFieldName,String secondFieldName,String logText) throws Exception {
		return populateLookup(query,uniqueFieldName,secondFieldName,logText,new HashMap<String,Set<String>>());
	}
	
	protected Map<String,Set<String>> populateLookup(String query,String uniqueFieldName,String secondFieldName,String logText, Map<String,? extends Set<String>> lookupRef) throws Exception {
		return populateLookup(query,uniqueFieldName,secondFieldName,logText,lookupRef,HashSet.class);
	}

	protected Map<String,Set<String>> populateLookupOrdered(String query,String uniqueFieldName,String secondFieldName,String logText) throws Exception {
		return populateLookupOrdered(query,uniqueFieldName,secondFieldName,logText,new HashMap<String,Set<String>>());
	}

	protected Map<String,Set<String>> populateLookupOrdered(String query,String uniqueFieldName,String secondFieldName,String logText, Map<String,? extends Set<String>> lookupRef) throws Exception {
		return populateLookup(query,uniqueFieldName,secondFieldName,logText,lookupRef,LinkedHashSet.class);
	}

	@SuppressWarnings("unchecked")
	protected Map<String,Set<String>> populateLookup(String query,String uniqueFieldName,String secondFieldName,String logText, Map<String,? extends Set<String>> lookupRef,@SuppressWarnings("rawtypes") Class<? extends Set> setClass) throws Exception {
		// do some type-casting magic in order to create a new instance of "? extends Set"
		Map<String,Set<String>> returnLookup = (Map<String,Set<String>>) lookupRef;

		logger.info("populating map of "+logText);

		ResultSet rs = ex.executeProto(query);

		int rows = 0;
		while (rs.next()) {
			String uniqueField = rs.getString(uniqueFieldName);
			String secondField = rs.getString(secondFieldName);
			if(!returnLookup.containsKey(uniqueField)) {
				returnLookup.put(uniqueField, setClass.newInstance());
			}
			returnLookup.get(uniqueField).add(secondField);
			rows++;
		}

		rs.close();
		logger.info("finished populating map of "+logText + " with " + rows + " rows for " + returnLookup.size() + " " + uniqueFieldName);
		return returnLookup;
	}

	/*
	 * Convenience method to add the given value for the given solr field.
	 * Is a no-op if either the field or the value are null.
	 */
	protected void addIfNotNull(SolrInputDocument solrDoc, String solrField, Object value) {
		if ((value != null) && (solrField != null)) {
			solrDoc.addField(solrField, value);
		}
	}

	/*
	 * Convenience method to add all items from an iterable to a particular solr field.
	 * Ignores input if null.
	 */
	protected void addAll(SolrInputDocument solrDoc,String solrField,Iterable<String> items) {
		if(items != null) {
			for(Object obj : items) {
				solrDoc.addField(solrField,obj);
			}
		}
	}

	/*
	 * Convenience method to add all items from a lookup map
	 * to a particular solr field. Ignores input if lookupId doesn't exist.
	 */
	protected void addAllFromLookup(SolrInputDocument solrDoc,String solrField,String lookupId,Map<String,Set<String>> lookupRef) {
		if(lookupRef.containsKey(lookupId)) {
			for(Object obj : lookupRef.get(lookupId)) {
				solrDoc.addField(solrField,obj);
			}
		}
	}


	private Map<String,Set<String>> dupTracker = new HashMap<String,Set<String>>();
	protected void addAllFromLookupNoDups(SolrInputDocument solrDoc,String solrField,String lookupId,Map<String,Set<String>> lookupRef) {
		Set<String> uniqueList = getNoDupList(solrField);

		if(lookupRef.containsKey(lookupId)) {
			for(String obj : lookupRef.get(lookupId)) {
				if(uniqueList.contains(obj)) continue;
				else uniqueList.add(obj);
				solrDoc.addField(solrField,obj);
			}
		}
	}

	private Set<String> getNoDupList(String solrField) {
		Set<String> uniqueList;
		if(!dupTracker.containsKey(solrField)) {
			uniqueList = new HashSet<String>();
			dupTracker.put(solrField,uniqueList);
		}
		else uniqueList = dupTracker.get(solrField);
		return uniqueList;
	}
	
	protected void addFieldNoDup(SolrInputDocument solrDoc,String solrField,String value) {
		Set<String> uniqueList = getNoDupList(solrField);
		if(uniqueList.contains(value)) return;
		uniqueList.add(value);
		solrDoc.addField(solrField,value);
	}

	protected void resetDupTracking() {
		dupTracker = new HashMap<String,Set<String>>();
	}

	// fill rows into a temp table using the given 'cmd'.  (may also create the table, depending
	// on the SQL)
	protected void fillTempTable(String cmd) {
		ex.executeVoid(cmd);
		logger.debug("  - populated table in " + ex.getTimestamp());
	}

	private int indexCounter=0;		// counter of indexes created so far (for unique naming)

	// create an index on the given column in the given table
	protected void createTempIndex(String tableName,String column) {
		indexCounter += 1;
		this.ex.executeVoid("create index tmp_idx"+indexCounter+" on "+tableName+" ("+column+")");
		logger.debug("  - created index tmp_idx" + indexCounter + " in " + ex.getTimestamp());
	}
	
	// run 'analyze' on the given table
	protected void analyze(String tableName) {
		this.ex.executeVoid("analyze " + tableName);
		logger.debug("  - analyzed " + tableName + " in " + ex.getTimestamp());
	}

	protected void logFreeMemory() {
		logger.info("  - free memory: " + Runtime.getRuntime().freeMemory() + " bytes");
	}
}
