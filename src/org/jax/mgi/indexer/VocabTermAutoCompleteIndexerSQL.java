package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

/**
 * VocabTermAutoCompleteIndexerSQL
 * @author kstone
 * This index is has the primary responsibility of populating the vocab term autocomplete index.
 * 
 * Note: refactored during 5.x development
 */

public class VocabTermAutoCompleteIndexerSQL extends Indexer 
{   
	public VocabTermAutoCompleteIndexerSQL () 
	{ super("index.url.vocabTermAC"); }

	public void index() throws Exception
	{    
		Map<String,Integer> termSort = new HashMap<String,Integer>();
		ArrayList<String> termsToSort = new ArrayList<String>();

		logger.info("Getting all distinct vocab terms & synonyms that are not obsolete and in vocabularies (GO,Mammalian Phenotype,OMIM,Human Phenotype Ontology)");
		String query = "select distinct t.term_key, t.term,t.vocab_name,t.display_vocab_name,t.primary_id, "+
				"ts.synonym,ts.synonym_type "+
				", tc.marker_count,tc.gxdlit_marker_count,tc.expression_marker_count "+
				"from term t left outer join term_synonym ts on t.term_key=ts.term_key and ts.synonym_type != 'disease cluster', " +
				"term_counts tc "+
				"where t.is_obsolete=0 and "+
				"t.vocab_name in ('GO','Mammalian Phenotype','OMIM','Human Phenotype Ontology') and "+
				"t.term_key=tc.term_key ";
		ResultSet rs_overall = ex.executeProto(query);
		logger.info("calculating sorts");
		while(rs_overall.next())
		{
			String term = rs_overall.getString("term");
			String synonym = rs_overall.getString("synonym");
			termsToSort.add(term);
			termsToSort.add(synonym);
		}
		//sort the terms
		Collections.sort(termsToSort,new SmartAlphaComparator());
		for(int i=0;i<termsToSort.size();i++)
		{
			termSort.put(termsToSort.get(i), i);
		}

		logger.info("Creating the docs");

		//Map<String,SolrInputDocument> docs = new HashMap<String,SolrInputDocument>();
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		Collection<String> addedTermKeys = new HashSet<String>();

		rs_overall = ex.executeProto(query);
		while (rs_overall.next()) 
		{
			String term_key = rs_overall.getString("term_key");
			String term_id = rs_overall.getString("primary_id");
			String term = rs_overall.getString("term");
			String synonym = rs_overall.getString("synonym");
			String vocab = rs_overall.getString("display_vocab_name");
			String root_vocab = rs_overall.getString("vocab_name");
			String gxdlit_marker_count = rs_overall.getString("gxdlit_marker_count");
			String expression_marker_count = rs_overall.getString("expression_marker_count");
			String simple_marker_count = rs_overall.getString("marker_count");
			String unique_key = term_id;

			// add term only once
			if (term !=null && !term.equals("") && !term.equals(" ") && !addedTermKeys.contains(term_key))
			{
				addedTermKeys.add(term_key);
				SolrInputDocument doc = new SolrInputDocument();

				doc.addField(IndexConstants.VOCABAC_TERM,term);
				doc.addField(IndexConstants.VOCABAC_TERM_LENGTH,term.length());
				doc.addField(IndexConstants.VOCABAC_BY_TERM, termSort.get(term));
				doc.addField(IndexConstants.VOCABAC_IS_SYNONYM, false);
				doc.addField(IndexConstants.VOCABAC_ORIGINAL_TERM,term);
				doc.addField(IndexConstants.VOCABAC_BY_ORIGINAL_TERM, termSort.get(term));
				doc.addField(IndexConstants.VOCABAC_KEY, unique_key);
				doc.addField(IndexConstants.VOCABAC_TERM_ID, term_id);
				doc.addField(IndexConstants.VOCABAC_TERM_KEY, term_key);
				doc.addField(IndexConstants.VOCABAC_VOCAB,vocab);
				System.out.println("Vocab: " + root_vocab);
				doc.addField(IndexConstants.VOCABAC_ROOT_VOCAB,root_vocab);
				doc.addField(IndexConstants.VOCABAC_EXPRESSION_MARKER_COUNT, expression_marker_count);
				doc.addField(IndexConstants.VOCABAC_GXDLIT_MARKER_COUNT,gxdlit_marker_count);
				doc.addField(IndexConstants.VOCABAC_MARKER_COUNT,simple_marker_count);
				docs.add(doc);
			}

			// add synonym if we have any for this term 
			if (synonym != null && !synonym.equals("") && !synonym.equals(" "))
			{
				// need to add a synonym term
				SolrInputDocument doc = new SolrInputDocument();

				doc.addField(IndexConstants.VOCABAC_TERM,synonym);
				doc.addField(IndexConstants.VOCABAC_TERM_LENGTH,synonym.length());
				doc.addField(IndexConstants.VOCABAC_BY_TERM, termSort.get(synonym));
				doc.addField(IndexConstants.VOCABAC_IS_SYNONYM, true);
				doc.addField(IndexConstants.VOCABAC_ORIGINAL_TERM,term);
				doc.addField(IndexConstants.VOCABAC_BY_ORIGINAL_TERM, termSort.get(term));
				doc.addField(IndexConstants.VOCABAC_KEY, unique_key+"-"+synonym);
				doc.addField(IndexConstants.VOCABAC_TERM_ID, term_id);
				doc.addField(IndexConstants.VOCABAC_TERM_KEY, term_key);
				doc.addField(IndexConstants.VOCABAC_VOCAB,vocab);
				doc.addField(IndexConstants.VOCABAC_ROOT_VOCAB,root_vocab);
				doc.addField(IndexConstants.VOCABAC_EXPRESSION_MARKER_COUNT, expression_marker_count);
				doc.addField(IndexConstants.VOCABAC_GXDLIT_MARKER_COUNT,gxdlit_marker_count);
				doc.addField(IndexConstants.VOCABAC_MARKER_COUNT,simple_marker_count);
				docs.add(doc);
			}

		}

		logger.info("Adding the documents to the index.");

		writeDocs(docs);
		commit();
	}
}
