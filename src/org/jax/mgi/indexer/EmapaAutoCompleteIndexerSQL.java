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
 * EmapaAutoCompleteIndexerSQL
 * This index is has the primary responsibility of populating the autocomplete
 * index for EMAPA anatomy terms for the GXD QF.  Since this is such a small
 * index we don't do any actual chunking here.  Copied and modified from the
 * StrucureAutoCompleteIndexerSQL, as we still need that one to support the
 * Cre/recombinase QF.
 *
 */

public class EmapaAutoCompleteIndexerSQL extends Indexer {   
	public EmapaAutoCompleteIndexerSQL () {
		super("emapaAC");
	}

	/* retrieve the set of EMAPA term keys that have associations for GXD high-throughput samples,
	 * including knowledge of which paths exist at which Theiler Stages.
	 */
	private Set<Integer> getEmapaKeysForGxdHT() throws Exception {
		String emapTable = SharedQueries.createEmapTempTable(logger, ex);
		
		String query = "select distinct emapa_ancestor_key " +
			"from " + emapTable + " t, expression_ht_sample s " +
			"where t.emapa_descendant_key = s.emapa_key " +
			"and t.stage = s.theiler_stage";
		ResultSet rs = ex.executeProto(query);
		
		Set<Integer> keys = new HashSet<Integer>();
		while (rs.next()) {
			keys.add(rs.getInt("emapa_ancestor_key"));
		}
		rs.close();
		
		logger.info("Got " + keys.size() + " EMAPA keys for GXD HT");
		return keys;
	}
	
	public void index() throws Exception {    
		Set<String> uniqueIds = new HashSet<String>();
		Map<String,Integer> termSort = new HashMap<String,Integer>();
		ArrayList<String> termsToSort = new ArrayList<String>();

		Set<Integer> emapaWithGxdHT = getEmapaKeysForGxdHT();
		
		logger.info("Getting all distinct structures & synonyms");
		
		// has_gxdhd -- refers to whether a structure is cited in samples in the high-throughput expression data
		String query = "with recombinase_terms as (select t.term_key " +
				"from term t " +
				"where t.vocab_name = 'EMAPA' " +
				" and ( " +
				"  exists (select 1 from term_emap e, recombinase_assay_result r " +
				"    where e.emapa_term_key = t.term_key " +
				"    and r.structure_key = e.term_key) " +
				"  or exists (select 1 from term_emap e, term_ancestor a, recombinase_assay_result r2 " +
				"    where t.term_key = e.emapa_term_key " +
				"    and e.term_key = a.ancestor_term_key " +
				"    and a.term_key = r2.structure_key)" +
				"  ))," +
				"anatomy_synonyms as "+
				"(select distinct t.term structure, ts.synonym, t.term_key, "+
				/* disabled until we want to use the picklist for actually picking a specific
				 * term to search by ID, rather than a set of words
				 *	    "  t.primary_id, " +
				 *	    "  e.start_stage, " +
				 *	    "  e.end_stage, " +
				 */
				 "case when (exists (select 1 from recombinase_terms rt where t.term_key = rt.term_key)) "+
				 "then true else false end as has_cre "+
				 "from term t join term_emap e on t.term_key = e.term_key left outer join " +
				 "term_synonym ts on t.term_key = ts.term_key "+
				 "where t.vocab_name='EMAPA') "+
				 "select distinct a1.structure, a1.synonym, a1.has_cre, a1.term_key, " +
				 /* disabled until we want to use the picklist for actually picking a specific
				  * term to search by ID, rather than a set of words
				  *	    "  a1.primary_id, " +
				  *	    "  a1.start_stage, " +
				  *	    "  a1.end_stage, " +
				  */
				  "  case when (exists (select 1 from anatomy_synonyms a2 where a2.structure=a1.synonym)) "+
				  "    then false else true end as is_strict_synonym "+
				  "from anatomy_synonyms a1 "+
				  "order by a1.structure ";

		ResultSet rs = ex.executeProto(query);

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		// need to gather the terms and synonyms that will appear in the pick
		// list, so we can compute an ordering for them
		logger.info("calculating sorts");
		while(rs.next()) {
			String term = rs.getString("structure");
			String synonym = rs.getString("synonym");
			/* disabled until we want to use the picklist for actually picking a specific
			 * term to search by ID, rather than a set of words
			 *	    String startStage = rs_overall.getString("start_stage");
			 *	    String accID = rs_overall.getString("primary_id");
			 *
			 *	    String tag = "-" + startStage + "-" + accID;
			 *
			 *	    // need to figure in the startStage, to account for terms which
			 *	    // appear as separate terms in different stage ranges
			 *
			 *	    termsToSort.add(term + tag);
			 *	    termsToSort.add(synonym + tag);
			 */
			termsToSort.add(term);
			termsToSort.add(synonym);
		}

		// sort the terms and assign a sort value for each in termSort
		Collections.sort(termsToSort,new SmartAlphaComparator());

		for (int i=0; i < termsToSort.size(); i++) {
			termSort.put(termsToSort.get(i), i);
		}

		logger.info("Creating the documents");
		rs = ex.executeProto(query);

		while (rs.next()) {
			// add the synonym structure combo
			String structure = rs.getString("structure");
			String synonym = rs.getString("synonym");
			Boolean hasCre = rs.getBoolean("has_cre");
			Boolean hasGxdHT = Boolean.FALSE;
			if (emapaWithGxdHT.contains(rs.getInt("term_key"))) {
				hasGxdHT = Boolean.TRUE;
			}

			/* disabled until we want to use the picklist for actually picking a specific
			 * term to search by ID, rather than a set of words
			 *	    String startStage = rs.getString("start_stage");
			 *	    String endStage = rs.getString("end_stage");
			 *	    String accID = rs.getString("primary_id");
			 *
			 *          // structure_key is merely a unique id so that Solr is happy,
			 *	    // because structures and synonyms can repeat. 
			 *
			 * 	    String tag = "-" + startStage + "-" + accID;
			 *          String structure_key = structure+"-"+synonym + "-" + tag;
			 */
			String structure_key = structure + "-" + synonym;

			// add an entry for the synonym, if it is defined
			if (synonym != null && !synonym.equals("") && !uniqueIds.contains(structure_key)) {
				// strict synonym means that this term only exists as a synonym
				Boolean isStrictSynonym = rs.getBoolean("is_strict_synonym");

				uniqueIds.add(structure_key);
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField(IndexConstants.STRUCTUREAC_STRUCTURE, structure);
				doc.addField(IndexConstants.STRUCTUREAC_SYNONYM, synonym);

				/* disabled until we want to use the picklist for actually picking a specific
				 * term to search by ID, rather than a set of words
				 *		doc.addField(IndexConstants.GXD_START_STAGE, startStage);
				 *		doc.addField(IndexConstants.GXD_END_STAGE, endStage);
				 *		doc.addField(IndexConstants.ACC_ID, accID);
				 *	        doc.addField(IndexConstants.STRUCTUREAC_BY_SYNONYM, termSort.get(synonym + tag));
				 */

				doc.addField(IndexConstants.STRUCTUREAC_BY_SYNONYM, termSort.get(synonym));
				doc.addField(IndexConstants.STRUCTUREAC_KEY,structure_key);
				doc.addField(IndexConstants.STRUCTUREAC_IS_STRICT_SYNONYM, isStrictSynonym);
				doc.addField(IndexConstants.STRUCTUREAC_HAS_CRE,hasCre);
				doc.addField(IndexConstants.STRUCTUREAC_HAS_GXDHT,hasGxdHT);
				docs.add(doc);
			}

			// make sure that the base structure gets included as a "synonym"

			/* disabled until we want to use the picklist for actually picking a specific
			 * term to search by ID, rather than a set of words
			 *           structure_key = structure+"-"+structure + "-" + tag;
			 */

			structure_key = structure + "-" + structure;
			if (!uniqueIds.contains(structure_key)) {
				uniqueIds.add(structure_key);
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField(IndexConstants.STRUCTUREAC_STRUCTURE, structure);
				doc.addField(IndexConstants.STRUCTUREAC_SYNONYM, structure);

				/* disabled until we want to use the picklist for actually picking a specific
				 * term to search by ID, rather than a set of words
				 *		doc.addField(IndexConstants.GXD_START_STAGE, startStage);
				 *		doc.addField(IndexConstants.GXD_END_STAGE, endStage);
				 *		doc.addField(IndexConstants.ACC_ID, accID);
				 *		doc.addField(IndexConstants.STRUCTUREAC_BY_SYNONYM, termSort.get(structure + tag));
				 */

				doc.addField(IndexConstants.STRUCTUREAC_BY_SYNONYM, termSort.get(structure));
				doc.addField(IndexConstants.STRUCTUREAC_KEY,structure_key);
				doc.addField(IndexConstants.STRUCTUREAC_IS_STRICT_SYNONYM, false);
				doc.addField(IndexConstants.STRUCTUREAC_HAS_CRE,hasCre);
				doc.addField(IndexConstants.STRUCTUREAC_HAS_GXDHT,hasGxdHT);
				docs.add(doc);
			}
		} // end while loop

		logger.info("Adding the documents to the index.");

		writeDocs(docs);
		commit();
	}
}
