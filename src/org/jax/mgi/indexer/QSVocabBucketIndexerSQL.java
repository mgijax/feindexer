package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/* Is: an indexer that builds the index supporting the quick search's vocab + strain bucket (aka- bucket 2).
 * 		Each document in the index represents data for a single vocabulary term.
 */
public class QSVocabBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	private static String INTERPRO_DOMAINS = "InterPro Domains";	// name of InterPro Domains vocabulary
	private static String MP_VOCAB = "Mammalian Phenotype";			// name of the MP vocabulary
	private static String DO_VOCAB = "Disease Ontology";			// name of DO vocabulary
	private static String PIRSF_VOCAB = "PIR Superfamily";			// name of PIRSF vocabulary
	private static String EMAPA_VOCAB = "EMAPA";					// name of the EMAPA vocabulary
	private static String EMAPS_VOCAB = "EMAPS";					// name of the EMAPS vocabulary
	private static String GO_VOCAB = "GO";							// name of the GO vocabulary
	private static String HPO_VOCAB = "Human Phenotype Ontology";	// name of HPO vocabulary
	private static String STRAIN = "Strain";						// name of faux "Strain" vocabulary
	
	private static String GO_MP = "Process";						// name of Biological Process DAG
	private static String GO_MF = "Function";						// name of Molecular Function DAG
	private static String GO_CC = "Component";						// name of Cellular Component DAG
	
	// what to add between base fewi URL and term ID, to link directly to a detail page
	private static Map<String,String> uriPrefixes;			
	static {
		uriPrefixes = new HashMap<String,String>();
		uriPrefixes.put(INTERPRO_DOMAINS, null);				// Protein domains intentionally omitted
		uriPrefixes.put(STRAIN, "/strain/");
		uriPrefixes.put(MP_VOCAB, "/vocab/mp_ontology/");
		uriPrefixes.put(DO_VOCAB, "/disease/");
		uriPrefixes.put(PIRSF_VOCAB, "/vocab/pirsf/");
		uriPrefixes.put(EMAPA_VOCAB, "/vocab/gxd/anatomy/");
		uriPrefixes.put(EMAPS_VOCAB, "/vocab/gxd/anatomy/");
		uriPrefixes.put(HPO_VOCAB, "/vocab/hp_ontology/");
		uriPrefixes.put(GO_VOCAB, "/vocab/gene_ontology/");
		uriPrefixes.put(GO_MP, "/vocab/gene_ontology/");
		uriPrefixes.put(GO_MF, "/vocab/gene_ontology/");
		uriPrefixes.put(GO_CC, "/vocab/gene_ontology/");
	}

	// URI for annotation links, use @@@@ where the term ID should go
	private static Map<String,String> annotationUri;
	static {
		annotationUri = new HashMap<String,String>();
		annotationUri.put(INTERPRO_DOMAINS, "marker/summary?interpro=@@@@");	
		annotationUri.put(MP_VOCAB, "/mp/annotations/@@@@");
		annotationUri.put(DO_VOCAB, "/disease/@@@@?openTab=models");
		annotationUri.put(PIRSF_VOCAB, "/vocab/pirsf/@@@@");
		annotationUri.put(EMAPA_VOCAB, "/gxd/structure/@@@@");
		annotationUri.put(EMAPS_VOCAB, "/gxd/structure/@@@@");
		annotationUri.put(HPO_VOCAB, "/diseasePortal?termID=@@@@");
		annotationUri.put(GO_VOCAB, "/go/term/@@@@");
		annotationUri.put(GO_MP, "/go/term/@@@@");
		annotationUri.put(GO_MF, "/go/term/@@@@");
		annotationUri.put(GO_CC, "/go/term/@@@@");
	}
	
	// what prefix to use before the term in the Term column
	private static Map<String,String> termPrefixes;		
	static {
		termPrefixes = new HashMap<String,String>();
		termPrefixes.put(INTERPRO_DOMAINS, "Protein Domain");
		termPrefixes.put(STRAIN, "Strain");
		termPrefixes.put(MP_VOCAB, "Phenotype");
		termPrefixes.put(DO_VOCAB, "Disease");
		termPrefixes.put(PIRSF_VOCAB, "Protein Family");
		termPrefixes.put(EMAPA_VOCAB, "Expression");
		termPrefixes.put(EMAPS_VOCAB, "Expression");
		termPrefixes.put(HPO_VOCAB, "Human Phenotype");
		termPrefixes.put(GO_VOCAB, "GO Term");
		termPrefixes.put(GO_MP, "Process");
		termPrefixes.put(GO_MF, "Function");
		termPrefixes.put(GO_CC, "Function");
	}
	
	// which field to use for faceting a given DAG/vocab
	private static Map<String,String> facetFields;
	static {
		facetFields = new HashMap<String,String>();
		facetFields.put(GO_MP, IndexConstants.QS_GO_PROCESS_FACETS);
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

	private Map<Integer,String> primaryIDs;				// term key : primary ID for term
	private Map<Integer,List<String>> allIDs;			// term key : list of all IDs for term
	private Map<Integer,List<String>> synonyms;			// term key : list of synonyms for term
	private Map<Integer,Integer> annotationCount;		// term key : count of annotations
	private Map<Integer,String> annotationLabel;		// term key : label for annotation link
	private Map<Integer,List<String>> ancestorFacets;	// term key : list of ancestor terms for a facet
	
	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSVocabBucketIndexerSQL() {
		super("qsVocabBucket");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	// get the field to use for faceting for the given DAG/vocab name
	private String getFacetField(String vocabName) {
		if (facetFields.containsKey(vocabName)) {
			return facetFields.get(vocabName);
		}
		return null;
	}
	
	// get the prefix of the URI for the detail page corresponding to the given vocabName; should just
	// have to append the primary ID to get to the term's detail page
	private String getUriPrefix(String vocabName) {
		if (uriPrefixes.containsKey(vocabName)) {
			return uriPrefixes.get(vocabName);
		}
		return null;
	}
	
	// get the single-word term type that should go before the term itself in the Best Match column of the display
	private String getTermType(String vocabName) {
		if (STRAIN.equals(vocabName)) {
			return "Name";
		}
		return "Term";
	}
	
	// get the single-word prefix that should go before the term itself in the Term column of the display
	private String getTermPrefix(String vocabName) {
		if (termPrefixes.containsKey(vocabName)) {
			return termPrefixes.get(vocabName);
		}
		logger.info("Missing termPrefix for " + vocabName);
		return vocabName;
	}
	
	/* Cache accession IDs for the given vocabulary name, populating primaryIDs and allIDs objects.
	 */
	private void cacheIDs(String vocabName) throws Exception {
		logger.info(" - caching IDs for " + vocabName);

		primaryIDs = new HashMap<Integer,String>();
		allIDs = new HashMap<Integer,List<String>>();
		
		String cmd = "select distinct t.term_key, i.acc_id, "
			+ "  case when (i.preferred = 1 and t.primary_id = i.acc_id) then 1 "
			+ "  else 0 end as is_primary "
			+ "from term t, term_id i "
			+ "where t.vocab_name = '" + vocabName + "' "
			+ "and t.term_key = i.term_key "
			+ "and t.is_obsolete = 0 "
			+ "order by 1, 2";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer termKey = rs.getInt("term_key");
			Integer isPrimary = rs.getInt("is_primary");
			String id = rs.getString("acc_id");
			
			if ((isPrimary == 1) && (!primaryIDs.containsKey(termKey))) {
				primaryIDs.put(termKey, id);
			}
			if (!allIDs.containsKey(termKey)) {
				allIDs.put(termKey, new ArrayList<String>());
			}
			allIDs.get(termKey).add(id);
		}
		rs.close();
		
		logger.info(" - cached " + ct + " IDs for " + primaryIDs.size() + " terms");
	}
	
	/* Cache all synonyms for terms in the given vocabulary, populating synonyms object.
	 */
	private void cacheSynonyms(String vocabName) throws Exception {
		logger.info(" - caching synonyms for " + vocabName);
		
		synonyms = new HashMap<Integer,List<String>>();
		
		String cmd = "select distinct t.term_key, s.synonym "
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

			Integer termKey = rs.getInt("term_key");
			String synonym = rs.getString("synonym");
			
			if (!synonyms.containsKey(termKey)) {
				synonyms.put(termKey, new ArrayList<String>());
			}
			synonyms.get(termKey).add(synonym);
		}
		rs.close();

		logger.info(" - cached " + ct + " synonyms for " + synonyms.size() + " terms");
	}
	
	/* Cache annotation data for the given vocabulary name, populating annotationCount and annotationLabel
	 */
	private void cacheAnnotations(String vocabName) throws Exception {
		annotationCount = new HashMap<Integer,Integer>();
		annotationLabel = new HashMap<Integer,String>();

		String cmd;
		if (MP_VOCAB.equals(vocabName) || GO_VOCAB.equals(vocabName) || DO_VOCAB.equals(vocabName) || HPO_VOCAB.equals(vocabName)) {
			cmd = "select c.term_key, t.primary_id, c.object_count_with_descendents, c.annot_count_with_descendents "
				+ "from term t, term_annotation_counts c "
				+ "where t.term_key = c.term_key "
				+ "and t.is_obsolete = 0 "
				+ "and t.vocab_name = '" + vocabName + "'";
		} else if (INTERPRO_DOMAINS.equals(vocabName) || PIRSF_VOCAB.equals(vocabName)) {
			cmd = "select a.term_key, a.term_id as primary_id, "
				+ "  count(distinct m.marker_key) as object_count_with_descendents, "
				+ "  count(1) as annot_count_with_descendents "
				+ "from annotation a, marker_to_annotation mta, marker m "
				+ "where a.annotation_key = mta.annotation_key "
				+ "and mta.marker_key = m.marker_key "
				+ "and a.vocab_name = '" + vocabName + "' "
				+ "group by 1, 2";
		} else {
			return;
		}

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			String termID = rs.getString("primary_id");
			Integer objectCount = rs.getInt("object_count_with_descendents");
			Integer annotCount = rs.getInt("annot_count_with_descendents");

			annotationCount.put(termKey, annotCount);
			if (MP_VOCAB.equals(vocabName)) {
				if (!"MP:0000001".equals(termID)) {
					annotationLabel.put(termKey, objectCount + " genotypes, " + annotCount + " annotations");
				}

			} else if (DO_VOCAB.equals(vocabName)) {
				annotationLabel.put(termKey, "view human &amp; mouse annotations");

			} else if (HPO_VOCAB.equals(vocabName)) {
				annotationLabel.put(termKey, objectCount + " diseases with annotations");

			} else if (PIRSF_VOCAB.equals(vocabName)) {
				annotationLabel.put(termKey, objectCount + " genes");

			} else {	// GO_VOCAB
				annotationLabel.put(termKey, objectCount + " genes, " + annotCount + " annotations");
			}
		}
		rs.close();

		logger.info(" - cached annotations for " + annotationCount.size() + " terms");
	}
	
	/* Load the ancestor terms (aka- slim terms) that should be used for facets for the terms in the
	 * specified 'vocabName'.
	 */
	private void cacheAncestorFacets(String vocabName) throws Exception {
		logger.info(" - loading ancestor facets for " + vocabName);
		ancestorFacets = new HashMap<Integer,List<String>>();

		String cmd = "select t.term_key, a.term as header " + 
			"from term t, term a, term_to_header h " + 
			"where t.term_key = h.term_key " + 
			"and h.header_term_key = a.term_key " + 
			"and t.vocab_name = 'GO' ";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			String header = rs.getString("header");
			
			if (!ancestorFacets.containsKey(termKey)) {
				ancestorFacets.put(termKey, new ArrayList<String>());
			}
			ancestorFacets.get(termKey).add(header);
		}
		rs.close();
		logger.info(" - cached ancestor facets for " + ancestorFacets.size() + " terms");
	}
	
	/* Process the terms for the give vocabulary name, generating documents and sending them to Solr.
	 * Assumes cacheIDs and cacheSynonyms have been run for this vocabulary.
	 */
	private void processTerms(String vocabName) throws Exception {
		logger.info(" - loading terms for " + vocabName);
		
		String cmd = "select t.term_key, t.primary_id, t.term, s.by_default, t.vocab_name, t.definition, "
			+ "    case when t.vocab_name = 'GO' then t.display_vocab_name "
			+ "    else null end as dag_name "
			+ "from term t, term_sequence_num s "
			+ "where t.vocab_name = '" + vocabName + "' "
			+ "  and t.is_obsolete = 0 "
			+ "  and t.term_key = s.term_key";

		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			
			Integer termKey = rs.getInt("term_key");
			String displayVocab = rs.getString("dag_name");
			if (displayVocab == null) {
				displayVocab = rs.getString("vocab_name");
			}
			String term = rs.getString("term");
			String primaryID = rs.getString("primary_id");
			String uriPrefix = getUriPrefix(vocabName);
			String definition = rs.getString("definition");

			// start building the solr document 
			SolrInputDocument doc = new SolrInputDocument();

			doc.addField(IndexConstants.QS_PRIMARY_ID, primaryID);
			doc.addField(IndexConstants.QS_TERM, term);
			doc.addField(IndexConstants.QS_SEQUENCE_NUM, rs.getInt("by_default"));
			doc.addField(IndexConstants.QS_VOCAB_NAME, getTermPrefix(displayVocab));
			doc.addField(IndexConstants.QS_TERM_TYPE, getTermType(vocabName));

			String facetField = getFacetField(displayVocab);
			if ((facetField != null) && ancestorFacets.containsKey(termKey)) {
				doc.addField(facetField, ancestorFacets.get(termKey));
			}
			
			if ((definition != null) && (definition.length() > 0)) {
				doc.addField(IndexConstants.QS_DEFINITION, definition);
			}
			
			if (uriPrefix != null) {
				doc.addField(IndexConstants.QS_DETAIL_URI, uriPrefix + primaryID);
			}

			if (allIDs.containsKey(termKey)) {
				for (String id : allIDs.get(termKey)) {
					doc.addField(IndexConstants.QS_ACC_ID, id);
				}
			}
			
			if (synonyms.containsKey(termKey)) {
				for (String s : synonyms.get(termKey)) {
					doc.addField(IndexConstants.QS_SYNONYM, s);
				}
			}
			
			Integer annotCount = annotationCount.get(termKey);
			if ((annotCount != null) && (annotCount > 0)) {
				doc.addField(IndexConstants.QS_ANNOTATION_COUNT, annotCount);

				String annotLabel = annotationLabel.get(termKey);
				if ((annotLabel != null) && (annotLabel.length() != 0)) {
					doc.addField(IndexConstants.QS_ANNOTATION_TEXT, annotLabel);

					String annotUri = annotationUri.get(displayVocab);
					if ((annotUri != null) && (annotUri.length() != 0)) {
						doc.addField(IndexConstants.QS_ANNOTATION_URI, annotUri.replaceFirst("@@@@", primaryID));
					}
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
		cacheAnnotations(vocabName);
		cacheAncestorFacets(vocabName);
		processTerms(vocabName);
		
		logger.info("finished " + vocabName);
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// process one vocabulary at a time, keeping caches in memory only for the current vocabulary
		processVocabulary(EMAPA_VOCAB);
		processVocabulary(EMAPS_VOCAB);
		processVocabulary(MP_VOCAB);
		processVocabulary(GO_VOCAB);
		processVocabulary(HPO_VOCAB);
		processVocabulary(DO_VOCAB);
		processVocabulary(PIRSF_VOCAB);
		processVocabulary(INTERPRO_DOMAINS);
		processVocabulary(STRAIN);
		
		// need to add strains and to collect annotation counts beyond the 4 current ones
		
		// commit all the changes to Solr
		commit();
	}
}