package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.util.StopwordRemover;

/* Is: an indexer that builds the index supporting the quick search's other ID bucket (aka- bucket 3).
 * 		Each document in the index represents data for a single object (e.g.- probe, clone, sequence, etc.)
 * 		that is no represented in one of the other QS buckets.
 */
public class QSOtherBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	// weights to prioritize different types of search terms / IDs
	private static int PRIMARY_ID_WEIGHT = 1000;
	private static int SECONDARY_ID_WEIGHT = 950;

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	private long uniqueKey = 0;							// ascending counter of documents created

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSOtherBucketIndexerSQL() {
		super("qsOtherBucket");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/
	
	// Add this doc to the batch we're collecting.  If the stack hits our threshold, send it to the server and reset it.
	private void addDoc(SolrInputDocument doc) {
		docs.add(doc);
		if (docs.size() >= solrBatchSize)  {
			writeDocs(docs);
			docs = new ArrayList<SolrInputDocument>();
		}
	}
	
	// Build and return a new SolrInputDocument with the given fields filled in.
	private SolrInputDocument buildDoc(DocBuilder builder, String exactTerm, String searchTermDisplay,
			String searchTermType, Integer searchTermWeight, String primaryID, Long sequenceNum) {

		SolrInputDocument doc = builder.getNewDocument();
		if (exactTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM_EXACT, exactTerm); }

		doc.addField(IndexConstants.QS_SEARCH_TERM_DISPLAY, searchTermDisplay);
		doc.addField(IndexConstants.QS_SEARCH_TERM_TYPE, searchTermType);
		doc.addField(IndexConstants.QS_SEARCH_TERM_WEIGHT, searchTermWeight);
		doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
		return doc;
	}
	
	/* Load accession IDs, build docs, and send to Solr.
	 */
/*	private void indexIDs() throws Exception {
		logger.info(" - indexing strain IDs");

		String cmd = "select distinct t.primary_id, i.logical_db, i.acc_id "
			+ "from strain t, strain_id i "
			+ "where t.strain_key = i.strain_key "
			+ "and i.private = 0 "
			+ "order by 1, 2";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			String primaryID = rs.getString("primary_id");
			String id = rs.getString("acc_id");
			String logicalDB = rs.getString("logical_db");
			
			if (strains.containsKey(primaryID)) {
				QSStrain qst = strains.get(primaryID);

				if (!id.equals(primaryID)) {
					addDoc(buildDoc(qst, id, null, id, logicalDB, SECONDARY_ID_WEIGHT));
				} else {
					addDoc(buildDoc(qst, id, null, id, logicalDB, PRIMARY_ID_WEIGHT));
				}
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " IDs");
	}
*/
	/* Load all synonyms, build docs, and send to Solr.
	 */
/*	private void indexSynonyms() throws Exception {
		logger.info(" - indexing synonyms");
		
		String cmd = "select distinct t.primary_id, s.synonym "
			+ "from strain t, strain_synonym s "
			+ "where t.strain_key = s.strain_key "
			+ "order by 1, 2";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of synonyms processed
		while (rs.next()) {
			ct++;
			String primaryID = rs.getString("primary_id");
			String synonym = rs.getString("synonym");
			
			if (strains.containsKey(primaryID)) {
				QSStrain qst = strains.get(primaryID);
				if (synonym != null) {
					addDoc(buildDoc(qst, null, synonym, synonym, "synonym", SYNONYM_WEIGHT));
					
					// also index it as an exact match, in case of stopwords
					addDoc(buildDoc(qst, synonym, null, synonym, "synonym", SYNONYM_WEIGHT));
				}
			}
		}
		rs.close();

		logger.info(" - indexed " + ct + " synonyms");
	}
*/	
	/* Cache reference count for each strain, populating referenceCounts
	 */
/*	private void cacheReferenceCounts() throws Exception {
		logger.info(" - caching reference counts");

		referenceCounts = new HashMap<String,Integer>();

		String cmd = "select s.primary_id, count(distinct r.reference_key) as refCount " + 
				"from strain s, strain_to_reference r " + 
				"where s.strain_key = r.strain_key " + 
				"group by 1";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			String strainID = rs.getString("primary_id");
			Integer refCount = rs.getInt("refCount");

			if (refCount != null) {
				referenceCounts.put(strainID, refCount);
			}
		}
		rs.close();

		logger.info(" - cached reference counts for " + referenceCounts.size() + " strains");
	}
*/
	/* Cache strain attributes
	 */
/*	private void cacheAttributes() throws Exception {
		logger.info(" - caching attributes");

		attributes = new HashMap<String,Set<String>>();

		String cmd = "select s.primary_id, r.attribute " + 
				"from strain s, strain_attribute r " + 
				"where s.strain_key = r.strain_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			String strainID = rs.getString("primary_id");
			String attribute = rs.getString("attribute");

			if (attribute != null) {
				if (!attributes.containsKey(strainID)) {
					attributes.put(strainID, new HashSet<String>());
				}
				attributes.get(strainID).add(attribute);
			}
		}
		rs.close();

		logger.info(" - cached attributes for " + attributes.size() + " strains");
	}
*/
	/* Load and cache the high-level phenotype (MP) terms that should be used for facets for the strains.
	 */
/*	private void cachePhenotypeFacets() throws Exception {
		logger.info(" - loading phenotype facets");
		phenotypeFacets = new HashMap<String,Set<String>>();

		String cmd = "select s.primary_id, h.heading as header " + 
			"from strain s, strain_grid_cell c, strain_grid_heading h " + 
			"where s.strain_key = c.strain_key " + 
			"and c.heading_key = h.heading_key " + 
			"and h.grid_name = 'MP' " + 
			"and c.value > 0";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		while (rs.next()) {
			String strainID = rs.getString("primary_id");
			String header = rs.getString("header");
			
			if (!phenotypeFacets.containsKey(strainID)) {
				phenotypeFacets.put(strainID, new HashSet<String>());
			}
			phenotypeFacets.get(strainID).add(header);
		}
		rs.close();
		logger.info(" - cached phenotype facets for " + phenotypeFacets.size() + " strains");
	}
*/
	/* Load the strains, cache them, and generate & send the initial set of documents to Solr.
	 * Assumes cachePhenotypeFacets, cacheReferenceCounts, and cacheAttributes have been called.
	 */
/*	private void buildInitialDocs() throws Exception {
		logger.info(" - loading strains");
		strains = new HashMap<String,QSStrain>();
		
		String cmd = "select s.primary_id, s.name, n.by_strain::bigint " + 
				"from strain s, strain_sequence_num n " + 
				"where s.strain_key = n.strain_key";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		while (rs.next())  {  
			String primaryID = rs.getString("primary_id");
			
			// need populate and cache each object
			
			QSStrain qst = new QSStrain(primaryID);
			qst.name = rs.getString("name");
			qst.sequenceNum = rs.getLong("by_strain");
			
			if (this.attributes.containsKey(primaryID)) {
				qst.attributes = this.attributes.get(primaryID);
			}
			if (this.phenotypeFacets.containsKey(primaryID)) {
				qst.phenotypeFacets = this.phenotypeFacets.get(primaryID);
			}
			if (this.referenceCounts.containsKey(primaryID)) {
				qst.referenceCount = this.referenceCounts.get(primaryID);
			}
			strains.put(primaryID, qst);
			
			// now build and save our initial documents for this strain

			addDoc(buildDoc(qst, qst.primaryID, null, qst.primaryID, "ID", PRIMARY_ID_WEIGHT));
			addDoc(buildDoc(qst, null, qst.name, qst.name, "Name", NAME_WEIGHT));
					
			// also index it as an exact match, in case of stopwords
			addDoc(buildDoc(qst, qst.name, null, qst.name, "Name", NAME_WEIGHT));
		}

		rs.close();
		logger.info("done processing initial docs for " + strains.size() + " strains");
	}
*/	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		logger.info("beginning strains");

/*
		cacheAttributes();
		cachePhenotypeFacets();
		cacheReferenceCounts();
		buildInitialDocs();
		indexIDs();
		indexSynonyms();
		
		// any leftover docs to send to the server?  (likely yes)
		if (docs.size() > 0) { writeDocs(docs); }

		// commit all the changes to Solr
		commit();
		logger.info("finished strains");
*/
	}

	// Private class for helping generate Solr documents.  Instantiate with the current object type being worked on.
	private class DocBuilder {
		private String objectType;
		
		private DocBuilder (String objectType, String detailPrefix) {
			this.objectType = objectType;
		}
		
		// compose and return a new SolrInputDocument including the fields for this feature
		public SolrInputDocument getNewDocument() {
			SolrInputDocument doc = new SolrInputDocument();

//			if (this.objectType != null) { doc.addField(IndexConstants.QS_OBJECT_TYPE, this.objectType); }
			return doc;
		}
	}
}