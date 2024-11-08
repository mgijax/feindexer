package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

/**
 * GXDEmapaAutoCompleteIndexerSQL
 * This index is has the primary responsibility of populating the autocomplete
 * index for EMAPA anatomy terms for the GXD QF.  Since this is such a small
 * index we don't do any actual chunking here.
 */

public class GXDEmapaAutoCompleteIndexerSQL extends Indexer
{
	public GXDEmapaAutoCompleteIndexerSQL ()
	{ super("gxdEmapaAC"); }

	public void index() throws Exception
	{
		Set<String> uniqueIds = new HashSet<String>();
		Map<String,Integer> termSort = new HashMap<String,Integer>();
		ArrayList<String> termsToSort = new ArrayList<String>();

		logger.info("Gathering distinct structures & synonyms");
		String query = "WITH anatomy_synonyms as "
				+ "(select distinct "
				+ "    t.term_key, "
				+ "    t.term structure, "
				+ "    ts.synonym, "
				+ "    t.primary_id, "
				+ "    e.start_stage, "
				+ "    e.end_stage, "
				+ "    case when (exists (select 1 from recombinase_assay_result rar where rar.structure=t.term)) "
				+ "      then true "
				+ "      else false "
				+ "    end as has_cre "
				+ "  from term t "
				+ "    join term_emap e on t.term_key = e.term_key "
				+ "    left outer join term_synonym ts on t.term_key = ts.term_key "
				+ "  where t.vocab_name='EMAPA'), "
				+ "hasCresults as ( "
				+ "  select distinct te.emapa_term_key as term_key "
				+ "  from expression_result_summary s, term_emap te "
				+ "  where s.structure_key = te.term_key "
				+ "), "
				+ "hasCresultsA as ( "
				+ "  select distinct te.emapa_term_key as term_key "
				+ "  from expression_result_summary s, term_ancestor ta, term_emap te "
				+ "  where s.structure_key = ta.term_key "
				+ "  and ta.ancestor_term_key = te.term_key "
				+ "  UNION "
				+ "  select term_key from hasCresults "
				+ "), "
				+ "hasHTresults as "
				+ "  (select distinct cs.emapa_key as term_key "
				+ "  from  expression_ht_consolidated_sample cs "
				+ "  where exists (select 1 from expression_ht_consolidated_sample_measurement csm  "
				+ "  where cs.consolidated_sample_key = csm.consolidated_sample_key)), "
				+ "hasHTresultsA as "
				+ "  (select distinct ta.ancestor_term_key as term_key "
				+ "  from hasHTresults hhr, term_ancestor ta "
				+ "  where hhr.term_key = ta.term_key  "
				+ "  union "
				+ "  select term_key from hasHTresults "
				+ " ) "
				+ "select distinct "
				+ "  a1.structure, "
				+ "  a1.synonym, "
				+ "  a1.has_cre, "
				+ "  a1.primary_id, "
				+ "  a1.start_stage, "
				+ "  a1.end_stage, "
				+ "  case when (exists (select 1 from anatomy_synonyms a2 where a2.structure=a1.synonym)) "
				+ "    then false "
				+ "    else true "
				+ "  end as is_strict_synonym, "
				+ "  case when (exists (select 1 from hasCresultsA hcr where hcr.term_key = a1.term_key)) "
				+ "    then true "
				+ "    else false "
				+ "  end as has_c_results, "
				+ "  case when (exists (select 1 from hasCresultsA hcr where hcr.term_key = a1.term_key)) "
				+ "    then true "
				+ "    else false "
				+ "  end as has_c_results_a, "
				+ "  case when (exists (select 1 from hasHTresults hhr where hhr.term_key = a1.term_key)) "
				+ "    then true "
				+ "    else false "
				+ "  end as has_ht_results, "
				+ "  case when (exists (select 1 from hasHTresultsA hhr where hhr.term_key = a1.term_key)) "
				+ "    then true "
				+ "    else false "
				+ "  end as has_ht_results_a "
				+ "from anatomy_synonyms a1 "
				+ "order by a1.structure ";

		ResultSet rs = ex.executeProto(query);

		Collection<SolrInputDocument> docs =
				new ArrayList<SolrInputDocument>();

		// need to gather the terms and synonyms that will appear in the pick
		// list, so we can compute an ordering for them
		logger.info("calculating sorts");
		while(rs.next())
		{
			String term = rs.getString("structure");
			String synonym = rs.getString("synonym");
			termsToSort.add(term);
			termsToSort.add(synonym);
		}

		// sort the terms and assign a sort value for each in termSort
		Collections.sort(termsToSort,new SmartAlphaComparator());

		for (int i=0; i < termsToSort.size(); i++)
		{
			termSort.put(termsToSort.get(i), i);
		}

		logger.info("Creating the documents");
		rs = ex.executeProto(query);

		while (rs.next())
		{
			// add the synonym structure combo
			String structure = rs.getString("structure");
			String synonym = rs.getString("synonym");
			Boolean hasCre = rs.getBoolean("has_cre");
			String startStage = rs.getString("start_stage");
			String endStage = rs.getString("end_stage");
			String accID = rs.getString("primary_id");
			Boolean hasCresults = rs.getBoolean("has_c_results");
			Boolean hasCresultsA = rs.getBoolean("has_c_results_a");
			Boolean hasHTresults = rs.getBoolean("has_ht_results");
			Boolean hasHTresultsA = rs.getBoolean("has_ht_results_a");

			// structure_key is merely a unique id so that Solr is happy,
			// because structures and synonyms can repeat.
			String tag = "-" + startStage + "-" + accID;
			String structure_key = structure + "-" + synonym + "-" + tag;

			// add an entry for the synonym, if it is defined
			if (synonym != null && !synonym.equals("") && !uniqueIds.contains(structure_key))
			{
				// strict synonym means that this term only exists as a synonym
				Boolean isStrictSynonym = rs.getBoolean("is_strict_synonym");

				uniqueIds.add(structure_key);
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField(IndexConstants.STRUCTUREAC_STRUCTURE, structure);
				doc.addField(IndexConstants.STRUCTUREAC_SYNONYM, synonym);
				doc.addField(IndexConstants.STRUCTUREAC_QUERYTEXT, synonym);
				doc.addField(IndexConstants.GXD_START_STAGE, startStage);
				doc.addField(IndexConstants.GXD_END_STAGE, endStage);
				doc.addField(IndexConstants.ACC_ID, accID);
				doc.addField(IndexConstants.STRUCTUREAC_BY_SYNONYM, termSort.get(synonym));
				doc.addField(IndexConstants.STRUCTUREAC_KEY,structure_key);
				doc.addField(IndexConstants.STRUCTUREAC_IS_STRICT_SYNONYM, isStrictSynonym);
				doc.addField(IndexConstants.STRUCTUREAC_HAS_CRE,hasCre);

				doc.addField(IndexConstants.STRUCTUREAC_SHOW_IN_CPOS,hasCresultsA);
				doc.addField(IndexConstants.STRUCTUREAC_SHOW_IN_CNEG,true);
				doc.addField(IndexConstants.STRUCTUREAC_SHOW_IN_RPOS,hasHTresultsA);
				doc.addField(IndexConstants.STRUCTUREAC_SHOW_IN_RNEG,hasHTresults);
				docs.add(doc);
			}


			structure_key = structure + "-" + structure + "-" + tag;
			if (!uniqueIds.contains(structure_key))
			{
				uniqueIds.add(structure_key);
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField(IndexConstants.STRUCTUREAC_STRUCTURE, structure);
				doc.addField(IndexConstants.STRUCTUREAC_QUERYTEXT, structure);
				doc.addField(IndexConstants.GXD_START_STAGE, startStage);
				doc.addField(IndexConstants.GXD_END_STAGE, endStage);
				doc.addField(IndexConstants.ACC_ID, accID);
				doc.addField(IndexConstants.STRUCTUREAC_BY_SYNONYM, termSort.get(structure));
				doc.addField(IndexConstants.STRUCTUREAC_KEY,structure_key);
				doc.addField(IndexConstants.STRUCTUREAC_IS_STRICT_SYNONYM, false);
				doc.addField(IndexConstants.STRUCTUREAC_HAS_CRE,hasCre);

				doc.addField(IndexConstants.STRUCTUREAC_SHOW_IN_CPOS,hasCresultsA);
				doc.addField(IndexConstants.STRUCTUREAC_SHOW_IN_CNEG,true);
				doc.addField(IndexConstants.STRUCTUREAC_SHOW_IN_RPOS,hasHTresultsA);
				doc.addField(IndexConstants.STRUCTUREAC_SHOW_IN_RNEG,hasHTresults);
				docs.add(doc);
			}
		} // end while loop

		logger.info("Adding the documents to the index.");
		writeDocs(docs);
		commit();
	}
}
