package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.util.EasyStemmer;
import org.jax.mgi.shr.fe.util.StopwordRemover;

/* Is: an indexer that builds the index supporting the quick search's vocab bucket (aka- bucket 2).
 * 		Each document in the index represents data for a single vocabulary term.
 */
public class QSStrainBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	// weights to prioritize different types of search terms / IDs
	private static int PRIMARY_ID_WEIGHT = 1000;
	private static int SECONDARY_ID_WEIGHT = 950;
	private static int NAME_WEIGHT = 900;
	private static int PARTIAL_NAME_WEIGHT = 875;
	private static int SYNONYM_WEIGHT = 850;
	private static int PARTIAL_SYNONYM_WEIGHT = 825;

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	private long uniqueKey = 0;							// ascending counter of documents created

	private Map<String,Integer> referenceCounts;			// primary ID : count of references
	private Map<String,Set<String>> attributes;			// primary ID : attributes of the strain
	
	private Map<String, QSStrain> strains;				// term's primary ID : QSTerm object
	
	private EasyStemmer stemmer = new EasyStemmer();
	private StopwordRemover stopwordRemover = new StopwordRemover();
	
	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSStrainBucketIndexerSQL() {
		super("qsStrainBucket");
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
	private SolrInputDocument buildDoc(QSStrain strain, String exactTerm, String stemmedTerm, String searchTermDisplay,
			String searchTermType, Integer searchTermWeight) {

		SolrInputDocument doc = strain.getNewDocument();
		if (exactTerm != null) { doc.addField(IndexConstants.QS_SEARCH_TERM_EXACT, exactTerm); }
		if (stemmedTerm != null) {
			doc.addField(IndexConstants.QS_SEARCH_TERM_STEMMED, stemmer.stemAll(stopwordRemover.remove(stemmedTerm)));
	 	}
		doc.addField(IndexConstants.QS_SEARCH_TERM_DISPLAY, searchTermDisplay);
		doc.addField(IndexConstants.QS_SEARCH_TERM_TYPE, searchTermType);
		doc.addField(IndexConstants.QS_SEARCH_TERM_WEIGHT, searchTermWeight);
		doc.addField(IndexConstants.UNIQUE_KEY, uniqueKey++);
		return doc;
	}
	
	/* Load accession IDs, build docs, and send to Solr.
	 */
	private void indexIDs() throws Exception {
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

	/* Load all synonyms, build docs, and send to Solr.
	 */
	private void indexSynonyms() throws Exception {
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
					// First index the synonym as an exact match.
					addDoc(buildDoc(qst, synonym, null, synonym, "Synonym", SYNONYM_WEIGHT));
					
					// Then convert the synonym into its component parts.  Index those parts for exact matching.
					List<String> parts = asList(splitIntoIndexablePieces(synonym));
					if (parts.size() > 1) {
						for (String part : parts) {
							addDoc(buildDoc(qst, part, null, synonym, "Synonym", PARTIAL_SYNONYM_WEIGHT));
						}
					}

					// And if any of those parts are only letters and ending with lowercase letters, then we'll
					// assume those could be words.  Remove any stopwords, then index the others for stemmed
					// matching.
					for (String word : this.cullStopwords(this.cullNonWords(parts))) {
						addDoc(buildDoc(qst, null, word, synonym, "Synonym", PARTIAL_SYNONYM_WEIGHT));
					}
				}
			}
		}
		rs.close();

		logger.info(" - indexed " + ct + " synonyms");
	}
	
	/* Cache reference count for each strain, populating referenceCounts
	 */
	private void cacheReferenceCounts() throws Exception {
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

	/* Cache strain attributes
	 */
	private void cacheAttributes() throws Exception {
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

	/* Load and cache the high-level terms that should be used for facets for the strains for
	 * the given facetType.
	 */
	private Map<String, Set<String>> getFacetValues(String facetType) throws Exception {
		logger.info(" - loading facets for " + facetType);
		Map<String, Set<String>> facets = new HashMap<String,Set<String>>();

		String cmd = null;

		if ("MP".equals(facetType)) {
			cmd = "select s.primary_id, h.heading as header " + 
				"from strain s, strain_grid_cell c, strain_grid_heading h " +
				"where s.strain_key = c.strain_key " +
				"and c.heading_key = h.heading_key " +
				"and h.grid_name = 'MP' " +
				"and c.value > 0";
		} else if ("Disease".equals(facetType)) {
			cmd = "select s.primary_id, ha.header " + 
				"from strain_disease sd " + 
				"inner join strain s on (sd.strain_key = s.strain_key) " + 
				"inner join term_ancestor ta on (sd.disease_key = ta.term_key) " + 
				"inner join hdp_annotation ha on (ta.ancestor_term = ha.header)";
		}
		
		if (cmd != null) {
			ResultSet rs = ex.executeProto(cmd, cursorLimit);

			while (rs.next()) {
				String strainID = rs.getString("primary_id");
				String header = rs.getString("header");
			
				if (!facets.containsKey(strainID)) {
					facets.put(strainID, new HashSet<String>());
				}
				facets.get(strainID).add(header);
			}
			rs.close();
		}
		logger.info(" - cached " + facetType + " facets for " + facets.size() + " strains");

		return facets;
	}

	/* Get the largest sequence number for strains, so we can use it later on as padding (in
	 * concert with the list of preferred strains).
	 */
	private long getMaxSequenceNum() throws Exception {
		String cmd = "select max(by_strain)::bigint as ct " + 
			"from strain_sequence_num";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		long ct = 0;
		while (rs.next())  {  
			ct = rs.getLong("ct");
		}
		rs.close();
		
		return ct; 
	}
	
	/* Get a Map from strain primary IDs to its preference level, for those strains
	 * that we wish to prefer when sorting.
	 * Rules:
	 *  1. Move inbred strains to the very top of the list.
	 * 	2. Move strains with zero or one mutated alleles up, but below the inbred strains.
	 */
	private Map<String, Integer> getPreferredStrains() throws Exception {
		Map<String, Integer> preferred = new HashMap<String, Integer>();
		
		String cmd1 = "with multiples as ( " + 
			"select s.primary_id, count(distinct q.allele_key) as ct  " + 
			"from strain s, strain_mutation q  " + 
			"where s.strain_key = q.strain_key  " + 
			"group by 1  " + 
			"having count(distinct q.allele_key) > 1 " + 
			") " + 
			"select s.primary_id " + 
			"from strain s " + 
			"where not exists (select 1 from multiples m " + 
			"  where s.primary_id = m.primary_id)";

		ResultSet rs1 = ex.executeProto(cmd1, cursorLimit);
		logger.debug("  - finished mutation query in " + ex.getTimestamp());

		while (rs1.next())  {  
			preferred.put(rs1.getString("primary_id"), 2);		// second level preference
		}
		rs1.close();
		
		String cmd2 = "select s.primary_id " + 
				"from strain s, strain_attribute a " + 
				"where s.strain_key = a.strain_key " + 
				"and a.attribute = 'inbred strain'";
		
		ResultSet rs2 = ex.executeProto(cmd2, cursorLimit);
		logger.debug("  - finished inbred strain query in " + ex.getTimestamp());

		while (rs2.next())  {  
			preferred.put(rs2.getString("primary_id"), 1);		// first level preference
		}
		rs2.close();

		logger.info("Returning data for " + preferred.size() + " preferred strains");
		return preferred; 
	}
	
	/* Break the input string based on given start & stop grouping characters, paying attention to nesting
	 * and only considering the outermost start/stop characters. That is, working with parentheses:
	 * 	1. "a(b)c" yields [ "a", "b", "c" ]
	 * 	2. "a(b(c))d" yields [ "a", "b(c)", "d" ]
	 * 	3. "a(b(c)d)e" yields [ "a", "b(c)d", "e" ]
	 * 	4. "a(b(c)d)e(fg)" yields [ "a", "b(c)d", "e", "fg" ]
	 */
	private List<String> splitGroup(String s, char startChar, char endChar) {
		List<String> out = new ArrayList<String>();

		// null?  no substrings to deal with.
		if (s == null) { return out; }
		
		// empty string or not having grouping character?  no substrings to deal with.
		String t = s.trim();
		if ((t.length() == 0) || (t.indexOf(startChar) < 0)) { return out; }
		
		boolean inGroup = false;					// are we in the midst of a group?
		int openChars = 0;							// number of unclosed open characters we've collected
		
		StringBuffer sb = new StringBuffer();		// current set of data being collected

		for (int i = 0; i < t.length(); i++) {
			char c = t.charAt(i);

			if (c == startChar) {
				openChars++;

				// If we're already in a group, then just collect the character.
				if (inGroup) {
					sb.append(c);

				} else {
					// Just beginning a group, so save any previous string collected, and begin a new one.
					inGroup = true;
					if (sb.length() > 0) {
						out.add(sb.toString());
						sb = new StringBuffer();
					}
				}
				
			} else if (c == endChar) {
				// If we're already in a group, we need to decrease our openChar count.
				// If 0, we've reached its end and we need to save the string collected.
				// If not, collect the character as we're dealing with a nested group.
				
				if (inGroup) {
					openChars--;
					if (openChars == 0) {
						inGroup = false;
						if (sb.length() > 0) {
							out.add(sb.toString());
							sb = new StringBuffer();
						}
					} else {
						sb.append(c);
					}
					
				} else {
					// Otherwise, we're not in a group, so this is a stray end character.  Just collect it.
					sb.append(c);
				}
			} else {
				sb.append(c);
			}
		}

		// any post-group characters we've collected?  if so, save them.
		if (sb.length() > 0) {
			out.add(sb.toString());
		}

		return out;
	}

	/* Split string s (either a strain name or synonym) into parts that should be added to the
	 * exact match index.  Needs to intelligently handle grouping characters:  parentheses,
	 * square brackets, and angle brackets.
	 */
	private Set<String> splitIntoIndexablePieces(String s) {
		// pieces of s that should be matchable by an exact comparison
		Set<String> pieces = new HashSet<String>();
		
		// stack of strings to analyze, starting with the full string
		Deque<String> stack = new ArrayDeque<String>();
		stack.push(s);
		
		// Keep working our way through our stack of items until it's empty.
		while (!stack.isEmpty()) {
			String toDo = stack.pop();
			
			List<String> chunks = null;
			
			// Angle brackets are most common (60k), so do them first.
			chunks = splitGroup(toDo, '<', '>');
			
			// Then parentheses (46k); do them next.
			if (chunks.size() == 0) {
				chunks = splitGroup(toDo, '(', ')');
			}

			// Finally, square brackets (33 total).
			if (chunks.size() == 0) {
				chunks = splitGroup(toDo, '[', ']');
			}
			
			// No grouping characters at this point, so split on whitespace.
			if (chunks.size() == 0) {
				chunks = Arrays.asList(toDo.split("\\s"));
			}
			
			// No spaces, so split on non-alphanumerics.
			if (chunks.size() == 1) {
				chunks = Arrays.asList(toDo.split("[^A-Za-z0-9]"));
			}
			
			if (!s.equals(toDo)) {
				pieces.add(toDo);
				pieces.add(toDo.replace("-", " "));			// also include a version with hyphens changed to spaces
			}
			
			for (String chunk : chunks) {
				if (!toDo.equals(chunk) && (chunk.trim().length() > 0)) {
					stack.push(chunk); 
				}
			}
		}
		return pieces;
	}

	/* Split string s (either a strain name or synonym) into parts delimited by non-alpha-numeric
	 * characters.  Return a list of those substrings.
	 */
	private Pattern nonAlphaNumeric = Pattern.compile("[^A-Za-z0-9]");
	private Set<String> old_splitIntoChunks(String s) {
		// Need to break strings into as many component pieces as possible, delimited by non-alpha-numerics.
		// For example:
		//	1. a-b yields a, b, a-b
		//	2. a-b-c yields a, b, c, a-b, b-c, a-b-c
		//	3. a-b-c-d yields a, b, c, d, a-b, b-c, c-d, a-b-c, b-c-d, a-b-c-d
		// Should intelligently handle parentheses, square brackets, and angle brackets.  (all appear)
		// Recursion seems to be endlessly looping for some reason, or running out of memory...
		// hangs at 100k records.
		
		String t = s.trim();
		
		Set<String> chunks = new HashSet<String>();
		chunks.add(t);
		
		Matcher matcher = nonAlphaNumeric.matcher(t);
		while (matcher.find()) {
			int start = matcher.start();
			String left = t.substring(0, start);
			String right = t.substring(start + 1);
			
			if (left.trim().length() > 0) { chunks.add(left); }
			if (right.trim().length() > 0) { chunks.addAll(this.splitIntoIndexablePieces(right)); }
		}
		
		return chunks;
	}
	
	/* Keep only potential words (only composed of letters, ending with a lowercase letter).
	 */
	private List<String> cullNonWords(List<String> words) {
		List<String> keepers = new ArrayList<String>();
		
		for (String word : words) {
			if (word.matches("^[A-Za-z]*[a-z]$")) {
				keepers.add(word);
			}
		}
		
		return keepers;
	}

	/* Remove any stopwords from the given list of strings.
	 */
	private List<String> cullStopwords(List<String> words) {
		List<String> keepers = new ArrayList<String>();
		
		for (String word : words) {
			String w = stopwordRemover.remove(word);
			if (w.trim().length() > 0) {
				keepers.add(word);
			}
		}
		
		return keepers;
	}

	private List<String> asList(Set<String> items) {
		List<String> myList = new ArrayList<String>(items.size());
		for (String s : items) {
			myList.add(s);
		}
		return myList;
	}

	/* Load the strains, cache them, and generate & send the initial set of documents to Solr.
	 * Assumes cacheReferenceCounts and cacheAttributes have been called.
	 */
	private void buildInitialDocs() throws Exception {
		logger.info(" - loading strains");
		strains = new HashMap<String,QSStrain>();
		
		// primary ID : set of slim terms for faceting
		Map<String, Set<String>> phenotypeFacets = this.getFacetValues("MP");
		Map<String, Set<String>> diseaseFacets = this.getFacetValues("Disease");

		long padding = this.getMaxSequenceNum();
		Map<String, Integer> preferred = this.getPreferredStrains();
		
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
			
			// Keep preferred strains near the top when sorting, with levels of preference kept
			// together.  Push non-preferred ones down to the bottom.

			if (preferred.containsKey(primaryID) && (preferred.get(primaryID) == 1)) {
				qst.sequenceNum = rs.getLong("by_strain");
			} else if (preferred.containsKey(primaryID) && (preferred.get(primaryID) == 2)) {
				qst.sequenceNum = padding + rs.getLong("by_strain");
			} else {
				qst.sequenceNum = (2 * padding) + rs.getLong("by_strain");
			}
			
			if (this.attributes.containsKey(primaryID)) {
				qst.attributes = this.attributes.get(primaryID);
			}
			if (phenotypeFacets.containsKey(primaryID)) {
				qst.phenotypeFacets = phenotypeFacets.get(primaryID);
			}
			if (diseaseFacets.containsKey(primaryID)) {
				qst.diseaseFacets = diseaseFacets.get(primaryID);
			}
			if (this.referenceCounts.containsKey(primaryID)) {
				qst.referenceCount = this.referenceCounts.get(primaryID);
			}
			strains.put(primaryID, qst);
			
			// now build and save our initial documents for this strain

			addDoc(buildDoc(qst, qst.primaryID, null, qst.primaryID, "ID", PRIMARY_ID_WEIGHT));

			// Index the strain name as an exact match, then also its individual parts for exact matches.
			addDoc(buildDoc(qst, qst.name, null, qst.name, "Name", NAME_WEIGHT));

			List<String> parts = asList(splitIntoIndexablePieces(qst.name));
			if (parts.size() > 1) {
				for (String part : parts) {
					addDoc(buildDoc(qst, part, null, qst.name, "Name", PARTIAL_NAME_WEIGHT));
				}
			}
		}

		rs.close();
		logger.info("done processing initial docs for " + strains.size() + " strains");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		logger.info("beginning strains");

		cacheAttributes();
		cacheReferenceCounts();
		buildInitialDocs();
		indexIDs();
		indexSynonyms();
		
		// any leftover docs to send to the server?  (likely yes)
		if (docs.size() > 0) { writeDocs(docs); }

		// commit all the changes to Solr
		commit();
		logger.info("finished strains");
	}

	// private class for caching vocab term data that will be re-used across multiple documents
	private class QSStrain {
		public String primaryID;
		public String name;
		public Set<String> attributes;
		public int referenceCount = 0;
		public Long sequenceNum;
		public Set<String> phenotypeFacets;
		public Set<String> diseaseFacets;

		// constructor
		public QSStrain(String primaryID) {
			this.primaryID = primaryID;
		}
		
		// compose and return a new SolrInputDocument including the fields for this feature
		public SolrInputDocument getNewDocument() {
			SolrInputDocument doc = new SolrInputDocument();

			if (this.primaryID != null) {
				doc.addField(IndexConstants.QS_PRIMARY_ID, this.primaryID);
				doc.addField(IndexConstants.QS_DETAIL_URI, "/strain/" + this.primaryID); 
			}

			doc.addField(IndexConstants.QS_REFERENCE_COUNT, this.referenceCount);
			if (this.referenceCount > 0) {
				doc.addField(IndexConstants.QS_REFERENCE_URI, "/reference/strain/" + this.primaryID + "?typeFilter=Literature");
			}

			if (this.sequenceNum != null) { doc.addField(IndexConstants.QS_SEQUENCE_NUM, this.sequenceNum); }
			if (this.name != null) { doc.addField(IndexConstants.QS_NAME, this.name); }

			if ((this.attributes != null) && (this.attributes.size() > 0)) { 
				doc.addField(IndexConstants.QS_ATTRIBUTES, this.attributes);
			}
			if ((this.phenotypeFacets != null) && (this.phenotypeFacets.size() > 0)) { 
				doc.addField(IndexConstants.QS_PHENOTYPE_FACETS, this.phenotypeFacets);
			}
			if ((this.diseaseFacets != null) && (this.diseaseFacets.size() > 0)) { 
				doc.addField(IndexConstants.QS_DISEASE_FACETS, this.diseaseFacets);
			}

			return doc;
		}
	}
}