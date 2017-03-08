package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.mgi.shr.jsonmodel.BrowserChild;
import org.jax.mgi.shr.jsonmodel.BrowserID;
import org.jax.mgi.shr.jsonmodel.BrowserParent;
import org.jax.mgi.shr.jsonmodel.BrowserSynonym;
import org.jax.mgi.shr.jsonmodel.BrowserTerm;
import org.jax.mgi.shr.jsonmodel.MappingExperimentSummary;

/* Is: an indexer that builds the index supporting the shared vocabulary browser (beginning with the 
 * 		[Adult] Mouse Anatomy vocabulary, but hopefully being extended to others).  Each document in the
 * 		index represents data for a single vocabulary term.
 * Notes: The index is intended to support searches by accession ID, term, and synonym.  Details of the
 * 		terms are encapsulated in a JSON object in the browserTerm field.
 */
public class VocabBrowserIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	private static String MA_VOCAB = "Adult Mouse Anatomy";		// name of the MA vocabulary
	
	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Map<Integer,BrowserID> primaryIDs;				// term key : primary ID for term
	private Map<Integer,List<BrowserID>> allIDs;			// term key : list of all IDs for term
	private Map<Integer,List<BrowserSynonym>> synonyms;		// term key : list of synonyms for term
	private Map<Integer,BrowserParent> defaultParent;		// term key : default parent of term
	private Map<Integer,List<BrowserParent>> allParents;	// term key : all parents of term
	private Map<Integer,List<BrowserChild>> children;		// term key : all children of term
	private Map<Integer,Integer> annotationCount;			// term key : count of annotations
	private Map<Integer,String> annotationLabel;			// term key : label for annotation link
	private Map<Integer,String> annotationUrl;				// term key : url for annotation link
	
	private ObjectMapper mapper = new ObjectMapper();				// converts objects to JSON

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public VocabBrowserIndexerSQL() {
		super("vocabBrowser");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* Cache annotation data for the given vocabulary name, populating annotationCount, annotationLabel,
	 * and annotationUrl objects
	 */
	private void cacheAnnotations(String vocabName) throws Exception {
		annotationCount = new HashMap<Integer,Integer>();
		annotationLabel = new HashMap<Integer,String>();
		annotationUrl = new HashMap<Integer,String>();

		if (MA_VOCAB.equals(vocabName)) {
			return;							// no annotations for MA vocabulary
		}
	}
	
	/* Cache accession IDs for the given vocabulary name, populating primaryIDs and allIDs objects.
	 */
	private void cacheIDs(String vocabName) throws Exception {
		logger.info(" - caching IDs for " + vocabName);

		primaryIDs = new HashMap<Integer,BrowserID>();
		allIDs = new HashMap<Integer,List<BrowserID>>();
		
		String cmd = "select distinct t.term_key, i.acc_id, i.logical_db, "
			+ "  case when (i.preferred = 1 and t.primary_id = i.acc_id) then 1 "
			+ "  else 0 end as is_primary "
			+ "from term t, term_id i "
			+ "where t.vocab_name = '" + vocabName + "' "
			+ "and t.term_key = i.term_key "
			+ "order by 1, 2";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer termKey = rs.getInt("term_key");
			Integer isPrimary = rs.getInt("is_primary");
			
			BrowserID id = new BrowserID(rs.getString("acc_id"), rs.getString("logical_db"));
			
			if ((isPrimary == 1) && (!primaryIDs.containsKey(termKey))) {
				primaryIDs.put(termKey, id);
			}
			if (!allIDs.containsKey(termKey)) {
				allIDs.put(termKey, new ArrayList<BrowserID>());
			}
			allIDs.get(termKey).add(id);
		}
		rs.close();
		
		logger.info(" - cached " + ct + " IDs for " + primaryIDs.size() + " terms");
	}
	
	private List<BrowserID> getSecondaryIDs(Integer termKey) {
		BrowserID primaryID = getPrimaryID(termKey);
		List<BrowserID> subset = new ArrayList<BrowserID>();
		
		if (allIDs.containsKey(termKey)) {
			if (primaryID == null) {			// odd case with no primary ID and all secondary IDs?
				return allIDs.get(termKey);
			}

			for (BrowserID id : allIDs.get(termKey)) {
				if (!id.getAccID().equals(primaryID.getAccID()) || !id.getLogicalDB().equals(primaryID.getLogicalDB())) {
					subset.add(id);
				}
			}
		}
		return subset;
	}
	
	/* Cache all synonyms for terms in the given vocabulary, populating synonyms object.
	 */
	private void cacheSynonyms(String vocabName) throws Exception {
		logger.info(" - caching synonyms for " + vocabName);
		
		synonyms = new HashMap<Integer,List<BrowserSynonym>>();
		
		String cmd = "select distinct t.term_key, s.synonym, s.synonym_type "
			+ "from term t, term_synonym s "
			+ "where t.vocab_name like '" + vocabName + "' "
			+ "  and t.term_key = s.term_key "
			+ "order by 1, 2, 3";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of synonyms processed
		while (rs.next()) {
			ct++;

			Integer termKey = rs.getInt("term_key");
			BrowserSynonym synonym = new BrowserSynonym(rs.getString("synonym"), rs.getString("synonym_type"));
			
			if (!synonyms.containsKey(termKey)) {
				synonyms.put(termKey, new ArrayList<BrowserSynonym>());
			}
			synonyms.get(termKey).add(synonym);
		}
		rs.close();

		logger.info(" - cached " + ct + " synonyms for " + synonyms.size() + " terms");
	}
	
	/* Cache parents for terms in the given vocabulary, populating defaultParent and allParents objects.
	 * Assumes cacheIDs has been run for the current vocabulary.
	 */
	private void cacheParents(String vocabName) throws Exception {
		logger.info(" - caching parents for " + vocabName);
		
		defaultParent = new HashMap<Integer,BrowserParent>();
		allParents = new HashMap<Integer,List<BrowserParent>>();
		
		String cmd = "select distinct c.child_term_key, c.edge_label, p.term_key, p.term, "
			+ "  case when (d.default_parent_key is not null and p.term_key = d.default_parent_key) then 1 "
			+ "  else 0 end as is_default "
			+ "from term p "
			+ "inner join term_child c on (p.term_key = c.term_key) "
			+ "left outer join term_default_parent d on (c.child_term_key = d.term_key) "
			+ "where p.vocab_name like '" + vocabName + "' "
			+ "order by 1, 5 desc, 4";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of synonyms processed
		while (rs.next()) {
			ct++;

			Integer childKey = rs.getInt("child_term_key");
			Integer parentKey = rs.getInt("term_key");
			Integer isDefault = rs.getInt("is_default");
			
			BrowserID parentID = getPrimaryID(parentKey);
			if (parentID == null) {
				throw new Exception("Unexpected term key (" + parentKey + ") with no primary ID");
			}

			BrowserParent parent = new BrowserParent(parentID.getAccID(), parentID.getLogicalDB(), 
				rs.getString("term"), rs.getString("edge_label"));

			if ((isDefault == 1) && (!defaultParent.containsKey(childKey))) {
				defaultParent.put(childKey, parent);
			}
			if (!allParents.containsKey(childKey)) {
				allParents.put(childKey, new ArrayList<BrowserParent>());
			}
			allParents.get(childKey).add(parent);
		}
		rs.close();

		logger.info(" - cached " + ct + " parents for " + defaultParent.size() + " terms");
	}
	
	/* Cache children for terms in the given vocabulary, populating children object.
	 * Assumes cacheIDs has been run for the current vocabulary.
	 */
	private void cacheChildren(String vocabName) throws Exception {
		logger.info(" - caching children for " + vocabName);
		
		children = new HashMap<Integer,List<BrowserChild>>();
		
		String cmd = "with child_counts as ( "
			+ "    select t.term_key, count(distinct c.child_term_key) as child_count "
			+ "    from term t "
			+ "    left outer join term_child c on (t.term_key = c.term_key) "
			+ "    where t.vocab_name = '" + vocabName + "' "
			+ "    group by 1) "
			+ "select distinct p.term_key, c.child_term_key, c.edge_label, c.child_term, "
			+ "  case when (cc.child_count is not null and cc.child_count > 0) then 1 "
			+ "  else 0 end as has_children "
			+ "from term p "
			+ "inner join term_child c on (p.term_key = c.term_key) "
			+ "left outer join child_counts cc on (c.child_term_key = cc.term_key) "
			+ "where p.vocab_name like '" + vocabName + "' "
			+ "order by 1, 4";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of synonyms processed
		while (rs.next()) {
			ct++;

			Integer parentKey = rs.getInt("term_key");
			Integer childKey = rs.getInt("child_term_key");
			
			BrowserID childID = getPrimaryID(childKey);
			if (childID == null) {
				throw new Exception("Unexpected term key (" + childKey + ") with no primary ID");
			}

			BrowserChild child = new BrowserChild(childID.getAccID(), childID.getLogicalDB(), 
				rs.getString("child_term"), rs.getString("edge_label"), rs.getInt("has_children"));

			if (!children.containsKey(parentKey)) {
				children.put(parentKey, new ArrayList<BrowserChild>());
			}
			children.get(parentKey).add(child);
		}
		rs.close();

		logger.info(" - cached " + ct + " children for " + children.size() + " terms");
	}
	
	/* Return the primary ID for termKey, if there is one.  If not, return null.
	 * Assumes cacheIDs has been run for the current vocabulary.
	 */
	private BrowserID getPrimaryID(Integer termKey) {
		if (primaryIDs.containsKey(termKey)) {
			return primaryIDs.get(termKey);
		}
		return null;
	}
	
	/* build and return a BrowserTerm object for the given input data
	 */
	private BrowserTerm buildBrowserTerm(Integer termKey, String term, String definition) throws Exception {
		BrowserID id = getPrimaryID(termKey);
		if (id == null) {
			throw new Exception("Unexpected term key (" + termKey + ") with no primary ID");
		}

		BrowserTerm t = new BrowserTerm();
		t.setPrimaryID(id);
		t.setTerm(term);
		t.setDefinition(definition);
		
		if (defaultParent.containsKey(termKey)) {
			t.setDefaultParent(defaultParent.get(termKey));
		}
		
		if (synonyms.containsKey(termKey)) {
			t.setSynonyms(synonyms.get(termKey));
		}
			
		List<BrowserID> secondaryIDs = getSecondaryIDs(termKey);
		if (secondaryIDs != null && secondaryIDs.size() > 0) {
			t.setSecondaryIDs(secondaryIDs);
		}
		
		if (allParents.containsKey(termKey)) {
			t.setAllParents(allParents.get(termKey));
		}
		
		if (children.containsKey(termKey)) {
			t.setChildren(children.get(termKey));
		}
		
		if (annotationCount.containsKey(termKey) && (annotationCount.get(termKey) > 0)
				&& annotationLabel.containsKey(termKey) && annotationUrl.containsKey(termKey)) {
			t.setAnnotationCount(annotationCount.get(termKey));
			t.setAnnotationLabel(annotationLabel.get(termKey));
			t.setAnnotationUrl(annotationUrl.get(termKey));
		}
		return t;
	}
	
	/* Process the terms for the give vocabulary name, generating documents and sending them to Solr.
	 * Assumes cacheIDs, cacheSynonyms, cacheParents, and cacheChilren have been run for this vocabulary.
	 */
	private void processTerms(String vocabName) throws Exception {
		logger.info(" - loading terms for " + vocabName);
		
		String cmd = "select t.term_key, t.primary_id, t.term, t.definition, s.by_dfs, t.vocab_name, "
			+ "    case when t.vocab_name = 'GO' then t.display_vocab_name "
			+ "    else null end as dag_name "
			+ "from term t, term_sequence_num s "
			+ "where t.vocab_name = '" + vocabName + "' "
			+ "  and t.term_key = s.term_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			
			Integer termKey = rs.getInt("term_key");
			String dagName = rs.getString("dag_name");
			BrowserTerm browserTerm = buildBrowserTerm(termKey, rs.getString("term"), rs.getString("definition"));

			// start building the solr document 
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.VB_PRIMARY_ID, rs.getString("primary_id"));
			doc.addField(IndexConstants.VB_TERM, rs.getString("term"));
			doc.addField(IndexConstants.VB_SEQUENCE_NUM, rs.getInt("by_dfs"));
			doc.addField(IndexConstants.VB_BROWSER_TERM, mapper.writeValueAsString(browserTerm));
			doc.addField(IndexConstants.VB_VOCAB_NAME, rs.getString("vocab_name"));
			if (dagName != null) {
				doc.addField(IndexConstants.VB_DAG_NAME, dagName);
			}
			
			if (allIDs.containsKey(termKey)) {
				for (BrowserID id : allIDs.get(termKey)) {
					doc.addField(IndexConstants.VB_ACC_ID, id.getAccID());
				}
			}
			
			if (synonyms.containsKey(termKey)) {
				for (BrowserSynonym s : synonyms.get(termKey)) {
					doc.addField(IndexConstants.VB_SYNONYM, s.getSynonym());
				}
			}
			
			if (allParents.containsKey(termKey)) {
				for (BrowserParent p : allParents.get(termKey)) {
					doc.addField(IndexConstants.VB_PARENT_ID, p.getPrimaryID());
				}
			}
			
			// Add this doc to the batch we're collecting.  If the stack hits our
			// threshold, send it to the server and reset it.
			docs.add(doc);
			if (docs.size() >= solrBatchSize)  {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		// any leftover docs to send to the server?  (likely yes)
		writeDocs(docs);
		rs.close();

		logger.info("done processing " + i + " terms for " + vocabName);
	}
	
	/* process the given vocabulary, loading data from the database, composing documents, and writing to Solr.
	 */
	private void processVocabulary(String vocabName) throws Exception {
		logger.info("beginning " + vocabName);
		
		cacheIDs(vocabName);
		cacheSynonyms(vocabName);
		cacheParents(vocabName);
		cacheChildren(vocabName);
		cacheAnnotations(vocabName);
		processTerms(vocabName);
		
		logger.info("finished " + vocabName);
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// process one vocabulary at a time, keeping caches in memory only for the current vocabulary
		processVocabulary(MA_VOCAB);
		
		// commit all the changes to Solr
		commit();
	}
}
