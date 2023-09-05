package org.jax.mgi.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.SQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexer
 * 
 * @author kstone This is the parent class for all of the indexers, and it
 *         supplies some useful functions that all of the indexers might need.
 * 
 *         It also sets up the sql connection, as well as the connection to a
 *         solr index, which is passed to it during construction time.
 */

public abstract class Indexer implements Runnable {

	private ConcurrentUpdateSolrClient client = null;
	public SQLExecutor ex = new SQLExecutor();

	public Logger logger = LoggerFactory.getLogger(this.getClass());
	private String solrIndexName = "";
	protected DecimalFormat df = new DecimalFormat("#.00");
	protected Runtime runtime = Runtime.getRuntime();
	public boolean indexPassed = true;

	private int docsSinceCommit = 0; // number of documents since the last commit
	private int docsSinceCommitThreshold = 100000; // once we have this many uncommitted docs, do a commit

	// Variables for handling threads
	private List<Thread> currentThreads = new ArrayList<Thread>();
	// maxThreads is configurable. When maxThreads is reached, program waits until
	// they are finished.
	// This is essentially running them in batches

	protected Indexer(String solrIndexName) {
		this.solrIndexName = solrIndexName;

		// Increase stall time detection (in ms). The default is 15 seconds, which is
		// far too quick for
		// substantial commits.
		System.setProperty("solr.cloud.client.stallTime", "119999");
	}

	public void setupConnection() throws Exception {
		logger.info("Setting up the properties");

		InputStream in = Indexer.class.getClassLoader().getResourceAsStream("config.props");
		Properties props = new Properties();
		if (in == null) {
			logger.info("resource config.props not found");
		}
		try {
			props.load(in);
			logger.debug(props.toString());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		logger.info("db connection info: " + ex);

		String solrUrl = props.getProperty("index.url") + "/" + solrIndexName;

		logger.info("Setting up index: " + solrUrl);
		try {
			client = new ConcurrentUpdateSolrClient.Builder(solrUrl).withQueueSize(160).withThreadCount(4).build();
		} catch (Throwable e) {
			logger.info("Failed to set up solr client:");
			e.printStackTrace();
			throw e;
		}

		logger.info("Working with index: " + solrUrl);

		client.setConnectionTimeout(3 * 60000);
		client.setSoTimeout(3 * 60000);

		try {
			logger.info("Deleting current index: " + solrIndexName);
			client.deleteByQuery("*:*");
			logger.info("After delete statement");
			commit();
			logger.info("After commit statement");
		} catch (Throwable e) {
			logger.info("Failed to delete documents from: " + solrIndexName);
			e.printStackTrace();
			throw e;
		}
		logger.info("Done with setupConnection()");
	}

	/*
	 * Code for loading a solr index must be implemented here
	 */
	abstract void index() throws Exception;

	public void run() {
		try {
			setupConnection();
			index();
			closeConnection();
			logger.info("Completed run of " + getClass());
		} catch (Exception e) {
			indexPassed = false;
			logger.error("Indexer: " + getClass() + " failed.", e);
		}
	}

	// closes down the connection and makes sure a last commit is run
	public void closeConnection() {

		logger.info("Indexer: Waiting for " + currentThreads.size() + " Threads to finish, RAM used: " + memoryUsed());

		for (Thread t : currentThreads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}

		commit(true);
		logger.info("Solr Documents are flushed to the server shuting down: " + solrIndexName);
		client.close();
	}

	public void commit() {
		commit(true);
	}

	public void commit(boolean wait) {
		try {
			logger.info("Waiting for Solr Commit");
			checkMemory();
			if (wait) {
				client.commit(wait, wait);
			} else {
				client.commit();
			}
		} catch (SolrServerException | IOException e) {
			logger.info("Exception in commit");
			e.printStackTrace();
		}
	}

	private void checkMemory() {
		if (memoryPercent() > 0.95) {
			logger.info("Memory usage is HIGH!!!: " + memoryUsed());
			printMemory();
		}
	}

	protected void printMemory() {
		logger.info("Used Mem: " + (runtime.totalMemory() - runtime.freeMemory()) + " " + memoryUsed());
		logger.info("Free Mem: " + runtime.freeMemory());
		logger.info("Total Mem: " + runtime.totalMemory());
		logger.info("Max Memory: " + runtime.maxMemory());
	}

	// returns the percentage of total memory used, as a String
	protected String memoryUsed() {
		return df.format(memoryPercent() * 100) + "%";
	}

	// returns the fraction of total memory used, as a Double
	protected double memoryPercent() {
		return ((double) runtime.totalMemory() - (double) runtime.freeMemory()) / (double) runtime.maxMemory();
	}

	// Create a hashmap, of a key -> hashSet mapping.
	// The hashSet is simply a collection for our 1->N cases.
	// This does not belong in this class. It's a straight up utility function
	protected HashMap<String, HashSet<String>> makeHash(String sql, String keyString, String valueString) {
		HashMap<String, String> allValues = new HashMap<String, String>();

		HashMap<String, HashSet<String>> tempMap = new HashMap<String, HashSet<String>>();

		try {
			ResultSet rs = ex.executeProto(sql);

			String key = null;
			String value = null;

			while (rs.next()) {
				key = rs.getString(keyString);
				value = rs.getString(valueString);

				if (allValues.containsKey(value)) {
					value = allValues.get(value);
				} else {
					allValues.put(value, value);
				}

				if (tempMap.containsKey(key)) {
					tempMap.get(key).add(value);
				} else {
					HashSet<String> temp = new HashSet<String>();
					temp.add(value);
					tempMap.put(key, temp);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		allValues.clear();
		return tempMap;
	}

	/*
	 * writes documents to solr. Best practice is to write small batches of
	 * documents to Solr and to commit less frequently. (TIP: this method will
	 * commit documents automatically using commitWithin) Here we also spawn a new
	 * process for each batch of documents.
	 */

	public void writeDocs(Collection<SolrInputDocument> docs) {
		if (docs == null || docs.size() == 0)
			return;

		try {
			client.add(docs);
			docsSinceCommit = docsSinceCommit + docs.size();

			if (docsSinceCommit >= docsSinceCommitThreshold) {
				commit();
				docsSinceCommit = 0;
			}
		} catch (SolrServerException | IOException e) {
			logger.info("Exception in writeDocs");
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return getClass().toString();
	}

	/*
	 * The following are convenience methods for populating lookups to be used in
	 * generating multiValued fields A String -> Set<String> map is always returned.
	 * This is to keep all terms unique, which leads to faster indexing in Solr.
	 * 
	 * Example Usage: String
	 * termSynonymQuery="select t.primary_id term_id,ts.synonym "+
	 * "from term t,term_synonym ts "+ "where t.term_key=ts.term_key " +
	 * "and t.vocab_name in ('Disease Ontology','Mammalian Phenotype') ";
	 *
	 * Map<String,Set<String>> synonymLookup =
	 * populateLookup(termSynonymQuery,"term_id","synonym","synonyms to term IDs");
	 *
	 * You may also pass in an existing map to add to it Map<String,Set<String>>
	 * synonymLookup = populateLookup(extraTermSynonymQuery,"term_id",
	 * "synonym","extra synonyms to term IDs",synonymLookup);
	 * 
	 * When no map is passed in, by default you will get back
	 * HashMap<String,HashSet<String>> as a return type. You may pass in a different
	 * type of Map<String,Set<String>> if you want to use either a different Map
	 * implementation, or a different Set implementation However, if the Set class
	 * is not passed in, a HashSet will be used (this is a limitation on java
	 * reflection) Example: Map<String,Set<String>> orderedSynonymLookup =
	 * populateLookup(termSynonymQuery,"term_id","synonym","synonyms to term IDs",
	 * new HashMap<String,LinkedHashSet<String>>(),LinkedHashSet.class);
	 * 
	 * Alternatively, use the shortcut methods populateLookupOrdered() if you want
	 * the default to be LinkedHashSet
	 */
	protected Map<String, Set<String>> populateLookup(String query, String uniqueFieldName, String secondFieldName, String logText) throws Exception {
		return populateLookup(query, uniqueFieldName, secondFieldName, logText, new HashMap<String, Set<String>>());
	}

	protected Map<String, Set<String>> populateLookup(String query, String uniqueFieldName, String secondFieldName, String logText, Map<String, ? extends Set<String>> lookupRef) throws Exception {
		return populateLookup(query, uniqueFieldName, secondFieldName, logText, lookupRef, HashSet.class);
	}

	protected Map<String, Set<String>> populateLookupOrdered(String query, String uniqueFieldName, String secondFieldName, String logText) throws Exception {
		return populateLookupOrdered(query, uniqueFieldName, secondFieldName, logText, new HashMap<String, Set<String>>());
	}

	protected Map<String, Set<String>> populateLookupOrdered(String query, String uniqueFieldName, String secondFieldName, String logText, Map<String, ? extends Set<String>> lookupRef) throws Exception {
		return populateLookup(query, uniqueFieldName, secondFieldName, logText, lookupRef, LinkedHashSet.class);
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Set<String>> populateLookup(String query, String uniqueFieldName, String secondFieldName, String logText, Map<String, ? extends Set<String>> lookupRef, @SuppressWarnings("rawtypes") Class<? extends Set> setClass) throws Exception {
		// do some type-casting magic in order to create a new instance of "? extends
		// Set"
		Map<String, Set<String>> returnLookup = (Map<String, Set<String>>) lookupRef;

		logger.info("populating map of " + logText);
		long start = runtime.freeMemory();
		ResultSet rs = ex.executeProto(query);

		int rows = 0;
		while (rs.next()) {
			String uniqueField = rs.getString(uniqueFieldName);
			String secondField = rs.getString(secondFieldName);
			if (!returnLookup.containsKey(uniqueField)) {
				returnLookup.put(uniqueField, setClass.newInstance());
			}
			returnLookup.get(uniqueField).add(secondField);
			rows++;
		}

		rs.close();
		long end = runtime.freeMemory();
		logger.info("finished populating map of " + logText + " with " + rows + " rows for " + returnLookup.size() + " " + uniqueFieldName + " Memory Change: " + (end - start) + " bytes");

		return returnLookup;
	}

	protected void populateOMIMNumberPartsForIds(Map<String, Set<String>> idMap) {

		for (String termSetKey : idMap.keySet()) {

			Set<String> ids = idMap.get(termSetKey);

			List<String> newIds = new ArrayList<String>();
			// This adds the Number part of OMIM to the alt ids also for searching
			for (String id : ids) {
				if (id.startsWith("OMIM:")) {
					newIds.add(id.replaceFirst("OMIM:", ""));
				}
			}
			ids.addAll(newIds);
		}
	}

	/*
	 * Convenience method to add the given value for the given solr field. Is a
	 * no-op if either the field or the value are null.
	 */
	protected void addIfNotNull(SolrInputDocument solrDoc, String solrField, Object value) {
		if ((value != null) && (solrField != null)) {
			solrDoc.addField(solrField, value);
		}
	}

	/*
	 * Convenience method to add all items from an iterable to a particular solr
	 * field. Ignores input if null.
	 */
	protected void addAll(SolrInputDocument solrDoc, String solrField, Iterable<String> items) {
		if (items != null) {
			for (Object obj : items) {
				solrDoc.addField(solrField, obj);
			}
		}
	}

	/*
	 * Convenience method to add all items from a lookup map to a particular solr
	 * field. Ignores input if lookupId doesn't exist.
	 */
	protected void addAllFromLookup(SolrInputDocument solrDoc, String solrField, String lookupId, Map<String, Set<String>> lookupRef) {
		if (lookupRef.containsKey(lookupId)) {
			for (Object obj : lookupRef.get(lookupId)) {
				solrDoc.addField(solrField, obj);
			}
		}
	}

	private Map<String, Set<String>> dupTracker = new HashMap<String, Set<String>>();

	protected void addAllFromLookupNoDups(SolrInputDocument solrDoc, String solrField, String lookupId, Map<String, Set<String>> lookupRef) {
		Set<String> uniqueList = getNoDupList(solrField);

		if (lookupRef.containsKey(lookupId)) {
			for (String obj : lookupRef.get(lookupId)) {
				if (uniqueList.contains(obj))
					continue;
				else
					uniqueList.add(obj);
				solrDoc.addField(solrField, obj);
			}
		}
	}

	private Set<String> getNoDupList(String solrField) {
		Set<String> uniqueList;
		if (!dupTracker.containsKey(solrField)) {
			uniqueList = new HashSet<String>();
			dupTracker.put(solrField, uniqueList);
		} else
			uniqueList = dupTracker.get(solrField);
		return uniqueList;
	}

	protected void addFieldNoDup(SolrInputDocument solrDoc, String solrField, String value) {
		Set<String> uniqueList = getNoDupList(solrField);
		if (uniqueList.contains(value))
			return;
		uniqueList.add(value);
		solrDoc.addField(solrField, value);
	}

	protected void resetDupTracking() {
		dupTracker = new HashMap<String, Set<String>>();
	}

	// fill rows into a temp table using the given 'cmd'. (may also create the
	// table, depending
	// on the SQL)
	protected void fillTempTable(String cmd) {
		ex.executeVoid(cmd);
		logger.debug("  - populated table in " + ex.getTimestamp());
	}

	private int indexCounter = 0; // counter of indexes created so far (for unique naming)

	// create an index on the given column in the given table
	protected void createTempIndex(String tableName, String column) {
		indexCounter += 1;
		this.ex.executeVoid("create index tmp_idx" + indexCounter + " on " + tableName + " (" + column + ")");
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
