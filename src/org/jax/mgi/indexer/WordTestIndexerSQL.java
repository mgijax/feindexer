package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;

/**
 * VocabTermAutoCompleteIndexerSQL
 * @author kstone
 * This index is has the primary responsibility of populating the vocab term autocomplete index.
 * 
 * Note: refactored during 5.x development
 */

public class WordTestIndexerSQL extends Indexer 
{   
	public WordTestIndexerSQL () 
	{ super("http://mgi-testweb4.jax.org:8995/solr/wordTest/"); }

	private void addTerms(String text, String type, Map<String,Set<String>> map)
	{
		if(text==null) return;
		String[] words = text.split("[^a-zA-Z]+");
		for(String word : words)
		{
			if(notEmpty(word))
			{	
				word = word.toLowerCase();
				if(!map.containsKey(word)) map.put(word, new HashSet<String>());
				map.get(word).add(type);
			}
		}
	}

	public void index() throws Exception
	{    
		ResultSet rs = ex.executeProto("select term,vocab_name from term " +
				"where vocab_name not like 'User%' " +
				"and vocab_name not like '%Codes' " +
				"and vocab_name not like '%Qualifier' ");
		Map<String,Set<String>> termMap = new HashMap<String,Set<String>>();
		logger.info("iterating through all vocab terms");
		while (rs.next()) 
		{
			addTerms(rs.getString("term"),rs.getString("vocab_name"),termMap);
		}


		rs = ex.executeProto("select note from allele_note ");
		logger.info("iterating through all allele notes");
		while (rs.next()) 
		{
			addTerms(rs.getString("note"),"Allele Note",termMap);
		}

		rs = ex.executeProto("select note from marker_note ");
		logger.info("iterating through all marker notes");
		while (rs.next()) 
		{
			addTerms(rs.getString("note"),"Marker Note",termMap);
		}

		rs = ex.executeProto("select note from mp_annotation_note ");
		logger.info("iterating through all MP notes");
		while (rs.next()) 
		{
			addTerms(rs.getString("note"),"MP Annotation Note",termMap);
		}

		rs = ex.executeProto("select note from reference_note ");
		logger.info("iterating through all reference notes");
		while (rs.next()) 
		{
			addTerms(rs.getString("note"),"Reference Note",termMap);
		}

		logger.info("adding them to Solr");
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for(String word : termMap.keySet())
		{
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("wordKey",word);
			doc.addField("wordLength",word.length());
			for(String vocab : termMap.get(word))
			{
				doc.addField("wordType",vocab);
			}
			docs.add(doc);

			if (docs.size() > 10000) 
			{
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		writeDocs(docs);
		commit();

		logger.info("finished");
	}

	private boolean notEmpty(String s)
	{
		return s!=null 
				&& s.length()>2 
				&& !s.contains("aaa") 
				&& (s.contains("a") || s.contains("i") || s.contains("e") || s.contains("o") || s.contains("u"));
	}
}
