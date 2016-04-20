package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;

/**
 * This index supports the termCompare query summary
 */
public class DagIndexerSQL extends Indexer 
{   
	public DagIndexerSQL () 
	{ 
		super("http://mgi-testweb4.jax.org:8995/solr/dag/"); 
	}

	public void index() throws Exception
	{    

		ResultSet rs = ex.executeProto("select max(t.term_key) as max_term_key from term t join " +
				"term_child tc on tc.term_key=t.term_key ");
		rs.next();

		Integer start = 0;
		Integer stop = rs.getInt("max_term_key");
		//stop=2000; // for testing
		logger.info("max term key = "+stop);
		int chunkSize = 10000;

		int modValue = stop.intValue() / chunkSize;
		// Perform the chunking
		for (int i = 0; i <= modValue; i++) 
		{
			start = i * chunkSize;
			stop = start + chunkSize;


			logger.info("Loading data for terms "+start+" to "+stop);
			processTerms(start,stop);
		}
		logger.info("load completed");
	}
	public void processTerms(int start, int stop) throws Exception
	{
		String q = "select t.term parent, " +
				"t.primary_id parent_id, " +
				"td.child_term child, " +
				"td.child_primary_id child_id, " +
				"td.is_leaf " +
				"from term t join " +
				"term_child td on t.term_key=td.term_key " +
				"where t.term_key > "+start+" and t.term_key <= "+stop;

		ResultSet rs = ex.executeProto(q);
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		logger.info("iterating");
		// write the links
		Set<String> uniques = new HashSet<String>();
		while(rs.next())
		{
			SolrInputDocument doc = new SolrInputDocument();
			String parentId = rs.getString("parent_id");
			String childId = rs.getString("child_id");
			String uniqueKey = parentId+"-"+childId;
			if(uniques.contains(uniqueKey)) continue;

			doc.addField("uniqueKey",uniqueKey);
			doc.addField("parentId",parentId);
			doc.addField("parent",rs.getString("parent"));
			doc.addField("childId",childId);
			doc.addField("child",rs.getString("child"));
			doc.addField("isLeaf",rs.getInt("is_leaf"));

			docs.add(doc);

			if (docs.size() > 5000) 
			{
				this.writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		writeDocs(docs);
		commit();

		logger.info("finished");
	}
}
