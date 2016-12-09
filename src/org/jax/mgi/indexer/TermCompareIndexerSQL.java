package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;

/**
 * This index supports the termCompare query summary
 */
public class TermCompareIndexerSQL extends Indexer 
{   
	private Indexer childIndexer;
	private static String GXD="GXD";
	private static String MP="MP";
	private static String OMIM="OMIM";

	public TermCompareIndexerSQL () 
	{ 
		super("http://mgi-testweb4.jax.org:8995/solr/termCompareGene/"); 
		childIndexer = new GenericIndexer("http://mgi-testweb4.jax.org:8995/solr/termCompare");
	}

	private Map<String,Set<Term>> termParents = new HashMap<String,Set<Term>>();
	private Map<String,Set<Term>> termChildren = new HashMap<String,Set<Term>>();

	public void index() throws Exception
	{    
		childIndexer.setupConnection();
		initTermRelationships();

		ResultSet rs = ex.executeProto("select max(marker_key) as max_marker_key from marker");
		rs.next();

		Integer start = 0;
		Integer stop = rs.getInt("max_marker_key");
		//stop=2000; // for testing
		logger.info("max marker key = "+stop);
		int chunkSize = 10000;

		int modValue = stop.intValue() / chunkSize;
		// Perform the chunking
		for (int i = 0; i <= modValue; i++) 
		{
			start = i * chunkSize;
			stop = start + chunkSize;


			logger.info("Loading data for markers "+start+" to "+stop);
			processMarkers(start,stop);
		}
		logger.info("load completed");
	}
	public void processMarkers(int start, int stop) throws Exception
	{
		Map<String,TermAnnotation> annotMap = getGxdForMarkers(start,stop);

		annotMap = getMpForMarkers(start,stop,annotMap);
		annotMap = getOmimForMarkers(start,stop,annotMap);

		logger.info("adding annotations to Solr");
		addVocabDocs(annotMap);

		logger.info("finished");
	}

	private void addVocabDocs(Map<String,TermAnnotation> annotMap) throws Exception
	{
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		Map<String,List<TermAnnotation>> geneMap = new HashMap<String,List<TermAnnotation>>();

		// write the child docs
		for(String uniqueKey : annotMap.keySet())
		{
			TermAnnotation annot = annotMap.get(uniqueKey);
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("uniqueKey",uniqueKey);
			doc.addField("geneKey",annot.geneKey);
			doc.addField("geneId",annot.geneId);
			doc.addField("geneSymbol",annot.geneSymbol);
			doc.addField("term",annot.term);
			doc.addField("termKey",annot.termKey);
			doc.addField("vocab",annot.vocab);
			String value = notEmpty(annot.value) ? annot.value : "yes";
			doc.addField("value",value);
			docs.add(doc);

			if(!geneMap.containsKey(annot.geneKey))
			{
				geneMap.put(annot.geneKey,new ArrayList<TermAnnotation>(Arrays.asList(annot)));
			}
			else
			{
				geneMap.get(annot.geneKey).add(annot);
			}

			if (docs.size() > 5000) 
			{
				childIndexer.writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		childIndexer.writeDocs(docs);
		childIndexer.commit();

		logger.info("done with children");
		// write the parent docs
		docs = new ArrayList<SolrInputDocument>();
		for(List<TermAnnotation> annots : geneMap.values())
		{
			if(annots.size()>0)
			{
				TermAnnotation geneInfo = annots.get(0);
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField("uniqueKey",geneInfo.geneKey);
				doc.addField("geneKey",geneInfo.geneKey);
				doc.addField("geneId",geneInfo.geneId);
				doc.addField("geneSymbol",geneInfo.geneSymbol);

				Set<String> uniqueAnnots = new HashSet<String>();
				for(TermAnnotation annot : annots)
				{
					String fieldBase = isPos(annot.value) ? "posTerm" : "negTerm";

					// filter out all the duplicates
					String uniqueAnnot = fieldBase+"_"+annot.vocab+"_"+annot.term;
					if(uniqueAnnots.contains(uniqueAnnot)) continue;
					uniqueAnnots.add(uniqueAnnot);

					doc.addField(fieldBase+annot.vocab,annot.term);
				}
				docs.add(doc);
				if (docs.size() > 1000) 
				{
					this.writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>();
				}
			}
		}

		writeDocs(docs);
		commit();
		logger.info("done with parents");
	}

	private void initTermRelationships() throws Exception
	{
		logger.info("gathering all term descendents");
		ResultSet rs = ex.executeProto("select td.* " +
				"from term_descendent td join " +
				"term t on t.term_key=td.term_key " +
				"where t.vocab_name in ('Anatomical Dictionary','Mammalian Phenotype','OMIM') ");
		while(rs.next())
		{
			String termKey = rs.getString("term_key");
			Term t = new Term();
			t.termKey = rs.getString("descendent_term_key");
			t.term = rs.getString("descendent_term");
			if(!termChildren.containsKey(termKey)) termChildren.put(termKey,new HashSet<Term>());
			termChildren.get(termKey).add(t);
		}

		logger.info("gathering all term ancestors");
		rs = ex.executeProto("select tas.term_key,tas.ancestor_term, tast.term_key ancestor_term_key " +
				"from term_ancestor_simple tas join " +
				"term t on t.term_key=tas.term_key join " +
				"term tast on tast.primary_id=tas.ancestor_primary_id " +
				"where t.vocab_name in ('Anatomical Dictionary','Mammalian Phenotype','OMIM') ");
		while(rs.next())
		{
			String termKey = rs.getString("term_key");
			Term t = new Term();
			t.termKey = rs.getString("ancestor_term_key");
			t.term = rs.getString("ancestor_term");
			if(!termParents.containsKey(termKey)) termParents.put(termKey,new HashSet<Term>());
			termParents.get(termKey).add(t);
		}
		logger.info("done initializing term relationships");
	}

	private Map<String,TermAnnotation> getGxdForMarkers(int start, int stop) throws Exception
	{
		ResultSet rs = ex.executeProto("select marker_key,marker_symbol, " +
				"structure term, structure_key term_key, " +
				"is_expressed " +
				"from expression_result_summary ers " +
				"where marker_key > "+start+" and marker_key <= "+stop);
		String vocab=GXD;
		Map<String,TermAnnotation> annotMap = new HashMap<String,TermAnnotation>();
		logger.info("iterating through gxd results");
		while (rs.next()) 
		{
			String mkey=rs.getString("marker_key");
			String tkey=rs.getString("term_key");
			String value = rs.getString("is_expressed");
			// ignore ambiguous
			if(!isNeg(value) && !isPos(value)) continue;

			registerAnnotation(annotMap,mkey,rs.getString("marker_symbol"),vocab,tkey,rs.getString("term"),value);
		}

		// roll up and down the positive and negative (respectively) annotations to parents and children (respectively)
		rollUpAndDown(annotMap,vocab);

		logger.info("done iterating through gxd results");
		return annotMap;
	}

	private Map<String,TermAnnotation> getMpForMarkers(int start, int stop,Map<String,TermAnnotation> annotMap) throws Exception
	{
		ResultSet rs = ex.executeProto("select ha.marker_key,m.symbol marker_symbol, " +
				"ha.term, ha.term_key, " +
				"qualifier_type " +
				"from hdp_annotation ha join " +
				"marker m on m.marker_key=ha.marker_key " +
				"where ha.annotation_type=1002 " +
				"and ha.marker_key > "+start+" and ha.marker_key <= "+stop);
		String vocab=MP;
		if(annotMap==null) annotMap = new HashMap<String,TermAnnotation>();
		logger.info("iterating through mp results");
		while (rs.next()) 
		{
			String mkey=rs.getString("marker_key");
			String tkey=rs.getString("term_key");
			String value = rs.getString("qualifier_type");
			// ignore ambiguous
			if(!isNeg(value) && !isPos(value)) continue;

			registerAnnotation(annotMap,mkey,rs.getString("marker_symbol"),vocab,tkey,rs.getString("term"),value);
		}

		// roll up and down the positive and negative (respectively) annotations to parents and children (respectively)
		rollUpAndDown(annotMap,vocab);

		logger.info("done iterating through mp results");
		return annotMap;
	}

	private Map<String,TermAnnotation> getOmimForMarkers(int start, int stop,Map<String,TermAnnotation> annotMap) throws Exception
	{
		ResultSet rs = ex.executeProto("select ha.marker_key,m.symbol marker_symbol, " +
				"ha.term, ha.term_key, " +
				"qualifier_type " +
				"from hdp_annotation ha join " +
				"marker m on m.marker_key=ha.marker_key " +
				"where ha.annotation_type=1005 " +
				"and ha.marker_key > "+start+" and ha.marker_key <= "+stop);
		String vocab=OMIM;
		if(annotMap==null) annotMap = new HashMap<String,TermAnnotation>();
		logger.info("iterating through omim results");
		while (rs.next()) 
		{
			String mkey=rs.getString("marker_key");
			String tkey=rs.getString("term_key");
			String value = rs.getString("qualifier_type");
			// ignore ambiguous
			if(!isNeg(value) && !isPos(value)) continue;

			registerAnnotation(annotMap,mkey,rs.getString("marker_symbol"),vocab,tkey,rs.getString("term"),value);
		}

		// roll up and down the positive and negative (respectively) annotations to parents and children (respectively)
		rollUpAndDown(annotMap,vocab);

		logger.info("done iterating through omim results");
		return annotMap;
	}

	private void rollUpAndDown(Map<String,TermAnnotation> annotMap, String vocab) throws Exception
	{
		logger.info("registering "+vocab+" parents for positive annotations");
		Collection<TermAnnotation>  originalValues = new ArrayList<TermAnnotation>(annotMap.values());
		for(TermAnnotation ta : originalValues)
		{
			// positive annotations roll up, negatives roll down
			if(isPos(ta.value))
			{
				if(termParents.containsKey(ta.termKey))
				{
					for(Term t : termParents.get(ta.termKey))
					{
						registerAnnotation(annotMap,ta.geneKey,ta.geneSymbol,vocab,t.termKey,t.term,ta.value);
					}
				}
			}
			else
			{
				if(termChildren.containsKey(ta.termKey))
				{
					for(Term t : termChildren.get(ta.termKey))
					{
						registerAnnotation(annotMap,ta.geneKey,ta.geneSymbol,vocab,t.termKey,t.term,ta.value);
					}
				}
			}
		}
	}

	private void registerAnnotation(Map<String,TermAnnotation> annotMap,String markerKey,String markerSymbol, String vocab,
			String termKey,String term,String value) throws Exception
	{
		String annotKey = markerKey+"_"+vocab+"_"+term;
		if(annotMap.containsKey(annotKey))
		{
			TermAnnotation ta = annotMap.get(annotKey);
			// if existing is negative, and new value is positive, update to new value
			if(isNeg(ta.value) && isPos(value)) ta.value = value;
		}
		else
		{
			TermAnnotation ta = new TermAnnotation();
			ta.geneKey=markerKey;
			ta.geneId="";
			ta.geneSymbol=markerSymbol;
			ta.vocab=vocab;
			ta.term=term;
			ta.termKey=termKey;
			ta.value=value;
			annotMap.put(annotKey,ta);
		}

	}


	public class TermAnnotation
	{
		String geneSymbol="";
		String geneId="";
		String geneKey="";
		String value="";
		String term="";
		String termKey="";
		String vocab="";
	}

	public class Term
	{
		String term;
		String termKey;
	}

	// a hacky way of getting access to a second solr server
	private class GenericIndexer extends Indexer
	{
		public GenericIndexer(String url)
		{
			super(url);
		}

		public void index() throws Exception {};
	}

	private boolean isNeg(String value)
	{
		// "no" OR "normal"
		return notEmpty(value) && ("no".equalsIgnoreCase(value) || "normal".equalsIgnoreCase(value));
	}
	private boolean isPos(String value)
	{
		// "yes" OR empty/null
		return (notEmpty(value) && "yes".equalsIgnoreCase(value)) || !notEmpty(value);
	}

	private boolean notEmpty(String s)
	{
		return s!=null && !s.equals("");
	}
}
