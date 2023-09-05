package org.jax.mgi.indexer;

import java.sql.ResultSet;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.mgi.shr.fe.util.EasyStemmer;
import org.jax.mgi.shr.fe.util.StopwordRemover;
import org.jax.mgi.shr.QSAccIDFormatter;
import org.jax.mgi.shr.QSAccIDFormatterFactory;
import org.jax.mgi.shr.QSExpressionFacetToolkit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/* Is: an indexer that builds the index supporting the quick search's vocab bucket (aka- bucket 2).
 * 		Each document in the index represents data for a single vocabulary term.
 */
public class QSVocabBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	// weights to prioritize different types of search terms / IDs
	private static int PRIMARY_ID_WEIGHT = 1000;
	private static int SECONDARY_ID_WEIGHT = 950;
	private static int TERM_WEIGHT = 900;
	private static int SYNONYM_WEIGHT = 850;
	private static int DEFINITION_WEIGHT = 800;

	private static String INTERPRO_DOMAINS = "InterPro Domains";	// name of InterPro Domains vocabulary
	private static String MP_VOCAB = "Mammalian Phenotype";			// name of the MP vocabulary
	private static String DO_VOCAB = "Disease Ontology";			// name of DO vocabulary
	private static String PIRSF_VOCAB = "PIR Superfamily";			// name of PIRSF vocabulary
	private static String EMAPA_VOCAB = "EMAPA";					// name of the EMAPA vocabulary
	private static String EMAPS_VOCAB = "EMAPS";					// name of the EMAPS vocabulary
	private static String GO_VOCAB = "GO";							// name of the GO vocabulary
	private static String HPO_VOCAB = "Human Phenotype Ontology";	// name of HPO vocabulary
	
	private static String GO_BP = "Process";						// name of Biological Process DAG
	private static String GO_MF = "Function";						// name of Molecular Function DAG
	private static String GO_CC = "Component";						// name of Cellular Component DAG
	
	// what to add between base fewi URL and term ID, to link directly to a detail page
	private static Map<String,String> uriPrefixes;			
	static {
		uriPrefixes = new HashMap<String,String>();
		uriPrefixes.put("Phenotype", "/vocab/mp_ontology/");
		uriPrefixes.put("Disease", "/disease/");
		uriPrefixes.put("Protein Family", "/vocab/pirsf/");
		uriPrefixes.put("Expression", "/vocab/gxd/anatomy/");
		uriPrefixes.put("Human Phenotype", "/vocab/hp_ontology/");
		uriPrefixes.put(GO_VOCAB, "/vocab/gene_ontology/");
		uriPrefixes.put(GO_BP, "/vocab/gene_ontology/");
		uriPrefixes.put(GO_MF, "/vocab/gene_ontology/");
		uriPrefixes.put(GO_CC, "/vocab/gene_ontology/");
	}

	// URI for annotation links, use @@@@ where the term ID should go
	private static Map<String,String> annotationUris;
	static {
		annotationUris = new HashMap<String,String>();
		annotationUris.put("Protein Domain", "/marker/summary?interpro=@@@@");	
		annotationUris.put("Phenotype", "/mp/annotations/@@@@");
		annotationUris.put("Disease", "/disease/@@@@?openTab=models");
		annotationUris.put("Protein Family", "/vocab/pirsf/@@@@");
		annotationUris.put("Expression", "/gxd/structure/@@@@");
		annotationUris.put("Human Phenotype", "/diseasePortal?termID=@@@@");
		annotationUris.put(GO_VOCAB, "/go/term/@@@@");
		annotationUris.put(GO_BP, "/go/term/@@@@");
		annotationUris.put(GO_MF, "/go/term/@@@@");
		annotationUris.put(GO_CC, "/go/term/@@@@");
	}
	
	// what prefix to use before the term in the Term column
	private static Map<String,String> termPrefixes;		
	static {
		termPrefixes = new HashMap<String,String>();
		termPrefixes.put(INTERPRO_DOMAINS, "Protein Domain");
		termPrefixes.put(MP_VOCAB, "Phenotype");
		termPrefixes.put(DO_VOCAB, "Disease");
		termPrefixes.put(PIRSF_VOCAB, "Protein Family");
		termPrefixes.put(EMAPA_VOCAB, "Expression");
		termPrefixes.put(EMAPS_VOCAB, "Expression");
		termPrefixes.put(HPO_VOCAB, "Human Phenotype");
		termPrefixes.put(GO_VOCAB, "GO Term");
		termPrefixes.put(GO_BP, "Process");
		termPrefixes.put(GO_MF, "Function");
		termPrefixes.put(GO_CC, "Component");
	}
	
	// which field to use for faceting a given DAG/vocab
	private static Map<String,String> facetFields;
	static {
		facetFields = new HashMap<String,String>();
		facetFields.put(GO_BP, IndexConstants.QS_GO_PROCESS_FACETS);
		facetFields.put(GO_CC, IndexConstants.QS_GO_COMPONENT_FACETS);
		facetFields.put(GO_MF, IndexConstants.QS_GO_FUNCTION_FACETS);
		facetFields.put(MP_VOCAB, IndexConstants.QS_PHENOTYPE_FACETS);
		facetFields.put(DO_VOCAB, IndexConstants.QS_DISEASE_FACETS);
	}
	
	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	private long uniqueKey = 0;						// ascending counter of documents created

	private Map<String,Long> annotationCount;			// primary ID : count of annotations
	private Map<String,String> annotationLabel;			// primary ID : label for annotation link
	private Map<String,Long> sequenceNum;				// primary ID : sequence num for term (smart-alpha)
	private Map<String,String> tsTag;					// primary ID : Theiler stages for EMAPA terms
	
	private Map<String, QSTerm> terms;				// term's primary ID : QSTerm object
	
	private EasyStemmer stemmer = new EasyStemmer();
	private StopwordRemover stopwordRemover = new StopwordRemover();
	
	private QSExpressionFacetToolkit toolkit = new QSExpressionFacetToolkit();
	private QSAccIDFormatterFactory idFactory = new QSAccIDFormatterFactory();
	
	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSVocabBucketIndexerSQL() {
		super("qsVocabBucket");
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
	private SolrInputDocument buildDoc(QSTerm term, String exactTerm, String stemmedTerm, String searchTermDisplay,
			String searchTermType, Integer searchTermWeight) {

		SolrInputDocument doc = term.getNewDocument();
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
	
	// get the single-word prefix that should go before the term itself in the Term column of the display
	private String getTermPrefix(String vocabName) {
		if (termPrefixes.containsKey(vocabName)) {
			return termPrefixes.get(vocabName);
		}
		logger.info("Missing termPrefix for " + vocabName);
		return vocabName;
	}

	/* Load accession IDs for the given vocabulary name, build docs, and send to Solr.
	 */
	private void indexIDs(String vocabName) throws Exception {
		logger.info(" - caching IDs for " + vocabName);

		String cmd = "select distinct t.primary_id, i.logical_db, i.acc_id, t.display_vocab_name "
			+ "from term t, term_id i "
			+ "where t.vocab_name = '" + vocabName + "' "
			+ "and t.term_key = i.term_key "
			+ "and t.is_obsolete = 0 "
			+ "order by 1, 2";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			String primaryID = rs.getString("primary_id");
			String id = rs.getString("acc_id");
			String logicalDB = rs.getString("logical_db");
			String dagName = rs.getString("display_vocab_name");
			
			// Tweaks to adjust for proper Best Match Type display.  (two vocabs where LDB != ID prefix)
			if ("Mammalian Phenotype".equals(logicalDB)) {
				logicalDB = "MP";
			} else if ("Disease Ontology".equals(logicalDB)) {
				logicalDB = "DOID";
			}

			if (terms.containsKey(primaryID)) {
				QSTerm qst = terms.get(primaryID);
				QSAccIDFormatter idf = idFactory.getFormatter("Term", logicalDB, id);

				if (!id.equals(primaryID)) {
					addDoc(buildDoc(qst, id, null, idf.getMatchDisplay(), idf.getMatchType(), SECONDARY_ID_WEIGHT));
				} else {
					addDoc(buildDoc(qst, id, null, idf.getMatchDisplay(), idf.getMatchType(), PRIMARY_ID_WEIGHT));
				}

				// For OMIM IDs we also need to index them without the prefix.
				if (id.startsWith("OMIM:")) {
					String noPrefix = id.replaceAll("OMIM:", "");
					QSAccIDFormatter idf2 = idFactory.getFormatter("Term", "OMIM", noPrefix);
					addDoc(buildDoc(qst, noPrefix, null, idf2.getMatchDisplay(), idf2.getMatchType(), SECONDARY_ID_WEIGHT));
				}
			}
		}
		rs.close();
		
		logger.info(" - indexed " + ct + " IDs");
	}

	/* Load all synonyms for terms in the given vocabulary, build docs, and send to Solr.
	 */
	private void indexSynonyms(String vocabName) throws Exception {
		logger.info(" - indexing synonyms for " + vocabName);
		
		String cmd = "select distinct t.primary_id, s.synonym "
			+ "from term t, term_synonym s "
			+ "where t.vocab_name like '" + vocabName + "' "
			+ "  and t.term_key = s.term_key "
			+ "  and t.is_obsolete = 0 "
			+ "  and s.synonym_type not in ('Synonym Type 1', 'Synonym Type 2') "
			+ "order by 1, 2";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of synonyms processed
		while (rs.next()) {
			ct++;

			String primaryID = rs.getString("primary_id");
			String synonym = rs.getString("synonym");
			
			if (terms.containsKey(primaryID)) {
				QSTerm qst = terms.get(primaryID);
				if (synonym != null) {
					addDoc(buildDoc(qst, null, synonym, synonym, "synonym", SYNONYM_WEIGHT));
				}
			}
		}
		rs.close();

		logger.info(" - indexed " + ct + " synonyms");
	}
	
	// Retrieve the Theiler Stages for EMAPA terms, build into a string for each term, and cache each.
	// Note: only works for terms with assigned IDs.
	private void cacheTheilerStages(String vocabName) throws Exception {
		logger.info("Loading Theiler Stages");
		this.tsTag = new HashMap<String,String>();
		
		String cmd = "select t.primary_id, e.start_stage, e.end_stage " + 
				"from term t, term_emap e " + 
				"where e.term_key = t.term_key " + 
				"and t.vocab_name in ('" + vocabName + "')";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		while (rs.next()) {
			String primaryID = rs.getString("primary_id");
			String startStage = rs.getString("start_stage");
			String endStage = rs.getString("end_stage");
			
			if ((primaryID != null) && (startStage != null) && (endStage != null)) {
				tsTag.put(primaryID, "TS" + startStage + "-" + endStage);
			}
		}
				
		rs.close();
		logger.info(" - cached " + tsTag.size() + " TS ranges");
	}
	
	// Load terms, sort them in a smart-alpha manner across all vocabs, then assign and cache sequence numbers.
	// Note: only works for terms with assigned IDs.
	private void cacheSequenceNum() throws Exception {
		logger.info("Loading all terms");
		List<SortableTerm> sortableTerms = new ArrayList<SortableTerm>();
		
		// let the database do the initial alpha sort, to get us part of the way toward smart-alpha
		String cmd = "select primary_id, term, display_vocab_name "
				+ "from term "
				+ "where primary_id is not null "
				+ "order by term";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		while (rs.next()) {
			sortableTerms.add(new SortableTerm(rs.getString("display_vocab_name"),
				rs.getString("term"), rs.getString("primary_id")));
		}
		rs.close();
		logger.info(" - loaded " + sortableTerms.size() + " terms");
		
		Collections.sort(sortableTerms, sortableTerms.get(0).getComparator());
		logger.info(" - sorted terms");
		
		long i = 0;
		this.sequenceNum = new HashMap<String,Long>();
		for (SortableTerm st : sortableTerms) {
			this.sequenceNum.put(st.primaryID, ++i);
		}

		logger.info(" - assigned sequence numbers for " + this.sequenceNum.size() + " terms");
	}
	
	// Return a singular or plural version of 'noun' based on the given 'count' of items.
	private String plural(Long count, String noun) {
		if (count == 1) return noun;
		return noun + "s";
	}
	
	// Return a singular or plural version of 'noun' based on the given 'count' of items.
	private String plural(Integer count, String noun) {
		return plural(Long.valueOf(count), noun);
	}
	
	/* Cache annotation data for the given vocabulary name, populating annotationCount and annotationLabel
	 */
	private void cacheAnnotations(String vocabName) throws Exception {
		annotationCount = new HashMap<String,Long>();
		annotationLabel = new HashMap<String,String>();

		String cmd;

		// The term_annotation_counts table only contains data for these vocabs.  (Technically it contains EMAPA/EMAPS data too, but
		// we skip it because it only includes classical data.)
		if (MP_VOCAB.equals(vocabName) || GO_VOCAB.equals(vocabName) || DO_VOCAB.equals(vocabName) || HPO_VOCAB.equals(vocabName)) {
			cmd = "select t.primary_id, c.object_count_with_descendents, c.annot_count_with_descendents "
				+ "from term t, term_annotation_counts c "
				+ "where t.term_key = c.term_key "
				+ "and t.is_obsolete = 0 "
				+ "and t.vocab_name = '" + vocabName + "'";

		} else if (EMAPA_VOCAB.equals(vocabName) || EMAPS_VOCAB.equals(vocabName)) {
			// Just collect zeroes for EMAPA/EMAPS terms, as we'll look those up within the fewi (to be able to accurately 
			// include RNA-Seq data.
			
			cmd = "select t.primary_id, 0 as object_count_with_descendents, 0 as annot_count_with_descendents " + 
					"from term t " + 
					"where vocab_name = '" + vocabName + "' ";

		} else if (INTERPRO_DOMAINS.equals(vocabName) || PIRSF_VOCAB.equals(vocabName)) {
			cmd = "select a.term_id as primary_id, "
				+ "  count(distinct m.marker_key) as object_count_with_descendents, "
				+ "  count(1) as annot_count_with_descendents "
				+ "from annotation a, marker_to_annotation mta, marker m "
				+ "where a.annotation_key = mta.annotation_key "
				+ "and mta.marker_key = m.marker_key "
				+ "and a.vocab_name = '" + vocabName + "' "
				+ "group by 1";
		} else {
			return;
		}

		logger.info(" - caching annotations for " + vocabName);

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			String termID = rs.getString("primary_id");
			Integer objectCount = rs.getInt("object_count_with_descendents");
			Long annotCount = rs.getLong("annot_count_with_descendents");

			annotationCount.put(termID, annotCount);
			if (MP_VOCAB.equals(vocabName)) {
				if (!"MP:0000001".equals(termID)) {
					annotationLabel.put(termID, objectCount + plural(objectCount, " genotype") +
						", " + annotCount + plural(annotCount, " annotation"));
				}

			} else if (DO_VOCAB.equals(vocabName)) {
				annotationLabel.put(termID, objectCount + plural(objectCount, " mouse model"));

			} else if (HPO_VOCAB.equals(vocabName)) {
				annotationLabel.put(termID, objectCount + plural(objectCount, " disease") + " with annotations");

			} else if (PIRSF_VOCAB.equals(vocabName)) {
				annotationLabel.put(termID, objectCount + plural(objectCount, " gene"));

			} else if (EMAPA_VOCAB.equals(vocabName) || EMAPS_VOCAB.equals(vocabName)) {
				annotationLabel.put(termID, "<<gxdCount>>");

			} else {	// GO_VOCAB
				annotationLabel.put(termID, objectCount + plural(objectCount, " gene") + ", " + annotCount +
					plural(annotCount, " annotation"));
			}
		}
		rs.close();

		logger.info(" - cached annotations for " + annotationCount.size() + " terms");
	}

	/* Load the ancestor terms (aka- slim terms) that should be used for facets for the terms in the
	 * specified 'vocabName'.  Currently only works for GO, MP, and DO vocabularies.
	 */
	private Map<String,Set<String>> cacheAncestorFacets(String vocabName) throws Exception {
		String cmd = null;

		// primary ID : list of ancestor terms for faceting
		Map<String,Set<String>> ancestorFacets = new HashMap<String,Set<String>>();

		if (GO_VOCAB.equals(vocabName) || MP_VOCAB.equals(vocabName)) {
			cmd = "select t.primary_id, a.term as header " + 
				"from term t, term a, term_to_header h " + 
				"where t.term_key = h.term_key " +
				"and h.header_term_key = a.term_key " +
				"and t.vocab_name = '" + vocabName + "' "; 

		} else if (DO_VOCAB.equals(vocabName)) {
			cmd = "with headers as (select distinct th.term as header " + 
					"from term t, term_to_header h, term th " + 
					"where t.term_key = h.term_key " + 
					"  and h.header_term_key = th.term_key " + 
					"  and t.vocab_name = 'Disease Ontology') " +
				"select t.primary_id, h.header " + 
				"from term t, term_ancestor a, headers h " + 
				"where t.term_key = a.term_key " + 
				"and a.ancestor_term = h.header " + 
				"and t.vocab_name = '" + vocabName + "' ";

		} else if (EMAPA_VOCAB.equals(vocabName)) {
			// headers for each EMAPA ID, computed based on EMAPS expression annotations
			cmd = toolkit.getHeadersForExpressedEmapaTerms(ex);

		} else if (EMAPS_VOCAB.equals(vocabName)) {
			// headers for each EMAPS ID, computed based on EMAPS expression annotations
			cmd = toolkit.getHeadersForExpressedEmapsTerms(ex);

		} else {
			return ancestorFacets;
		}

		logger.info(" - loading ancestor facets for " + vocabName);

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		while (rs.next()) {
			String termID = rs.getString("primary_id");
			String header = rs.getString("header");
			
			if (!ancestorFacets.containsKey(termID)) {
				ancestorFacets.put(termID, new HashSet<String>());
			}
			ancestorFacets.get(termID).add(header);
		}
		rs.close();
		logger.info(" - cached ancestor facets for " + ancestorFacets.size() + " terms");
		return ancestorFacets;
	}

	/* Load the terms for the given vocabulary name, cache them, and generate & send the initial set of
	 * documents to Solr. Assumes cacheAnnotations and cacheAncestorFacets have been run for this vocabulary.
	 */
	private void buildInitialDocs(String vocabName) throws Exception {
		logger.info(" - loading terms for " + vocabName);
		terms = new HashMap<String,QSTerm>();
		
		Map<String,Set<String>> ancestorFacets = this.cacheAncestorFacets(vocabName);

		String cmd = "select t.primary_id, t.term, t.vocab_name, t.definition, "
			+ "    case when t.vocab_name = 'GO' then t.display_vocab_name "
			+ "    else null end as dag_name "
			+ "from term t "
			+ "where t.vocab_name = '" + vocabName + "' "
			+ "  and t.is_obsolete = 0 ";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		while (rs.next())  {  
			i++;
			
			String primaryID = rs.getString("primary_id");
			String displayVocab = rs.getString("dag_name");
			if (displayVocab == null) {
				displayVocab = rs.getString("vocab_name");
			}

			// need populate and cache each object
			
			QSTerm qst = new QSTerm(primaryID);
			qst.rawVocabName = displayVocab;
			qst.vocabName = getTermPrefix(displayVocab);
			qst.term = rs.getString("term");
			qst.termType = "Term";
			
			if (this.sequenceNum.containsKey(primaryID)) {
				qst.sequenceNum = this.sequenceNum.get(primaryID);
			} else {
				qst.sequenceNum = 0L;
			}
			if (annotationLabel.containsKey(primaryID)) {
				qst.annotationText = annotationLabel.get(primaryID);
			}
			if (annotationCount.containsKey(primaryID)) {
				qst.annotationCount = annotationCount.get(primaryID);
			}
			if (facetFields.containsKey(displayVocab) && (ancestorFacets != null) && ancestorFacets.containsKey(primaryID)) {
				qst.facetField = facetFields.get(displayVocab);
				qst.facetValues = ancestorFacets.get(primaryID);
			}
			terms.put(primaryID, qst);
			
			if (EMAPA_VOCAB.equals(vocabName) || EMAPS_VOCAB.equals(vocabName)) {
				if (ancestorFacets.containsKey(primaryID)) {
					qst.expressionFacets = ancestorFacets.get(primaryID);
				}
			}
			
			// now build and save our initial documents for this term

			// any non-null primary IDs for terms will already be indexed when we do IDs, so skip them here

			addDoc(buildDoc(qst, null, qst.term, qst.term, "Term", TERM_WEIGHT));

			String definition = rs.getString("definition");
			if (definition != null) {
				addDoc(buildDoc(qst, null, definition, definition, "Definition", DEFINITION_WEIGHT));
			}
		}

		rs.close();

		logger.info("done processing " + i + " terms for " + vocabName);
	}

	/* process the given vocabulary, loading data from the database, composing documents, and writing to Solr.
	 */
	private void processVocabulary(String vocabName) throws Exception {
		logger.info("beginning " + vocabName);
		
		cacheAnnotations(vocabName);
		cacheAncestorFacets(vocabName);
		cacheTheilerStages(vocabName);
		buildInitialDocs(vocabName);

		indexIDs(vocabName);
		indexSynonyms(vocabName);
		
		// processTerms(vocabName);
		
		logger.info("finished " + vocabName);
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// caches that cross vocab boundaries
		cacheSequenceNum();
		
		// process one vocabulary at a time, keeping caches in memory only for the current vocabulary
		processVocabulary(EMAPA_VOCAB);
		processVocabulary(EMAPS_VOCAB);
		processVocabulary(MP_VOCAB);
		processVocabulary(GO_VOCAB);
		processVocabulary(HPO_VOCAB);
		processVocabulary(DO_VOCAB);
		processVocabulary(PIRSF_VOCAB);
		processVocabulary(INTERPRO_DOMAINS);
		
		// need to collect annotation counts beyond the 4 current ones
		
		// any leftover docs to send to the server?  (likely yes)
		if (docs.size() > 0) { writeDocs(docs); }

		// commit all the changes to Solr
		commit();
	}

	// private class for caching vocab term data that will be re-used across multiple documents
	private class QSTerm {
		public String vocabName;
		public String rawVocabName;
		public String primaryID;
		public String term;
		public String termType;
		public Long annotationCount;
		public String annotationText;
		public Long sequenceNum;
		public String facetField;			// name of the field with ancestor-based facets for this term
		public Set<String> facetValues;		// ancestors for facets of this term
		public Set<String> expressionFacets;	// set of header terms (EMAPA/EMAPS only) for this term where
												// this term has expression data

		// constructor
		public QSTerm(String primaryID) {
			this.primaryID = primaryID;
		}
		
		// compose and return a new SolrInputDocument including the fields for this feature
		public SolrInputDocument getNewDocument() {
			SolrInputDocument doc = new SolrInputDocument();

			if (this.primaryID != null) {
				doc.addField(IndexConstants.QS_PRIMARY_ID, this.primaryID);
				if (uriPrefixes.containsKey(this.vocabName)) {
					doc.addField(IndexConstants.QS_DETAIL_URI, uriPrefixes.get(this.vocabName) + this.primaryID); 
				}
			}
			
			if (this.rawVocabName != null) { doc.addField(IndexConstants.QS_RAW_VOCAB_NAME, this.rawVocabName); }
			if (this.vocabName != null) { doc.addField(IndexConstants.QS_VOCAB_NAME, this.vocabName); }
			if (this.term != null) {
				// For EMAPS terms, we also need to display the Theiler Stage along with the term itself.  We
				// can recognize these by vocab name, then pull off the final two characters of the primary ID
				// to identify the stage.
				String toDisplay = this.term;
				if ("EMAPS".equals(this.rawVocabName)) {
					if ((this.primaryID != null) && (this.primaryID.length() >= 2)) {
						String ts = this.primaryID.substring(this.primaryID.length() - 2);
						if (ts.startsWith("0")) {
							ts = ts.substring(1);		// trim off leading zero
						}
						toDisplay = "TS" + ts + ": " + this.term;
					}
				} else if (tsTag.containsKey(this.primaryID)) {
					toDisplay = this.term + " " + tsTag.get(primaryID);
				}
				doc.addField(IndexConstants.QS_TERM, toDisplay);
			}
			if (this.termType != null) { doc.addField(IndexConstants.QS_TERM_TYPE, this.termType); }
			if (this.sequenceNum != null) { doc.addField(IndexConstants.QS_SEQUENCE_NUM, this.sequenceNum); }

			if (this.annotationText != null) {
				doc.addField(IndexConstants.QS_ANNOTATION_TEXT, this.annotationText);
				if (this.annotationCount != null) {
					doc.addField(IndexConstants.QS_ANNOTATION_COUNT, annotationCount);
					if ((annotationCount > 0L) || this.rawVocabName.startsWith("EMAP")) {
						if (annotationUris.containsKey(this.vocabName) && (this.primaryID != null)) {
							String uri = annotationUris.get(this.vocabName);
							if (uri != null) {
								doc.addField(IndexConstants.QS_ANNOTATION_URI, uri.replace("@@@@", this.primaryID));
							}
						}
					}
				}
			}

			// If we have vocabulary-based facets, add them.
			if ((this.facetField != null) && (this.facetValues != null) && (this.facetValues.size() > 0)) {
				doc.addField(this.facetField, this.facetValues);
			}

			// If there are expression facets for this term, include them too.
			if ((this.expressionFacets != null) && (this.expressionFacets.size() > 0)) {
				doc.addField(IndexConstants.QS_EXPRESSION_FACETS, this.expressionFacets); 
			}
			return doc;
		}
	}
	
	// convenience class for sorting terms in a smart-alpha manner by: term then vocab then primary ID
	private class SortableTerm {
		public String term;
		public String vocab;
		public String primaryID;

		public SortableTerm(String vocab, String term, String primaryID) {
			this.vocab = vocab;
			this.term = term;
			this.primaryID = primaryID;
		}
		
		public Comparator<SortableTerm> getComparator() {
			SmartAlphaComparator cmp = new SmartAlphaComparator();
			return new TermAndIDComparator(cmp);
		}
			
		private class TermAndIDComparator implements Comparator<SortableTerm> {
			private SmartAlphaComparator cmp;

			public TermAndIDComparator(SmartAlphaComparator cmp) {
				this.cmp = cmp;
			}

			public int compare (SortableTerm a, SortableTerm b) {
				int i = this.cmp.compare(a.term, b.term);
				if (i == 0) {
					i = this.cmp.compare(a.vocab, b.vocab);
					if (i == 0) {
						i = this.cmp.compare(a.primaryID, b.primaryID);
					}
				}
				return i;
			}
		}
	}
}
