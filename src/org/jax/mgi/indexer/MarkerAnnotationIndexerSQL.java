package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 *
 * markerAnnotation index
 *
 * 	Contains all direct annotations to a Marker object
 *
 * 		- Used for the GO summaries
 *
 */

public class MarkerAnnotationIndexerSQL extends Indexer {


	public MarkerAnnotationIndexerSQL () {
		super("index.url.markerAnnotation");
	}

	public void index() throws Exception
	{
		// count of annotations

		ResultSet rs = ex.executeProto("select count(*) as total from annotation where object_type='Marker' ");
		rs.next();
		int total = rs.getInt("total");

		int start = 0;
		int chunkSize = 50000;
		int modValue = total / chunkSize;

		// ancestor IDs for a given GO term key
		Map<String,Set<String>> ancestorIds = null;
		String goAncestorQuery = "select tas.term_key, ancestor_primary_id " +
				"from term t join term_ancestor_simple tas on tas.term_key=t.term_key " +
				"where t.vocab_name in ('GO','InterPro Domains') " +
				"and tas.ancestor_term not in ('cellular_component','biological_process','molecular_function') ";
		ancestorIds = this.populateLookup(goAncestorQuery, "term_key", "ancestor_primary_id", "term key -> Ancestor Term ID");
		logger.info("Ancestor Id mapping is complete.  Size: " + ancestorIds.size() );


		for (int i = 0; i <= modValue; i++) {
			start = i * chunkSize;

			logger.info("Building documents for annotations, batch " + start + " to " + (start + chunkSize) + " of " + total);

			ex.executeVoid("drop table if exists tmp_mrk_annotation");

			// create a temp table for the current annotation batch
			String annotationsTemp = "select annotation_key into temp tmp_mrk_annotation "
					+ "from annotation "
					+ "where object_type = 'Marker' "
					+ "order by annotation_key "
					+ "limit " + chunkSize + " offset " + start + " ";
			ex.executeVoid(annotationsTemp);
			this.createTempIndex("tmp_mrk_annotation", "annotation_key");

			// find the bounds (min/max) of the batch
			rs = ex.executeProto("select min(annotation_key) minKey, max(annotation_key) maxKey from tmp_mrk_annotation");
			rs.next();
			int minAnnotKey = rs.getInt("minKey");
			int maxAnnotKey = rs.getInt("maxKey");


			// get the references for each annotation
			logger.info("Finding references for annotations");
			String annotToRefSQL = "select  a.annotation_key, reference_key "
					+ "from annotation_reference ar "
					+ "join annotation a on a.annotation_key = ar.annotation_key "
					+ "where a.object_type = 'Marker' "
					+ "	and a.annotation_key between " + minAnnotKey + " and " + maxAnnotKey + " ";

			logger.info(annotToRefSQL);
			Map<String, Set<String>> annotToRefs = this.populateLookup(annotToRefSQL, "annotation_key", "reference_key",
					"annotation_key to reference_keys");

			logger.info("Found refs for " + annotToRefs.size() + " annotations");


			// get the set of slimgrid header terms for each term used in
			// an annotation

			logger.info("Getting slimgrid header terms.");
			String cmd = "select distinct tth.term_key, h.abbreviation as header_term "
					+ "from term_to_header tth "
					+ "join term h on h.term_key = tth.header_term_key "
					+ "join annotation a on a.term_key = tth.term_key "
					+ "where a.object_type = 'Marker' "
					+ "	and a.annotation_key between " + minAnnotKey + " and " + maxAnnotKey + " ";

			Map<String, Set<String>> termToHeaders = this.populateLookup(cmd, "term_key", "header_term",
					"term_key to header_term(s)");

			logger.info("Found header terms for " + termToHeaders.size() + " terms");

			// Setup the main query here

			logger.info("Getting all marker annotations.");
			rs = ex.executeProto("select a.annotation_key, a.vocab_name, a.term, a.evidence_code, a.evidence_term, " +
					"a.term_id, a.qualifier, mta.marker_key, a.dag_name, asn.by_dag_structure, asn.by_vocab_dag_term, " +
					"asn.by_object_dag_term, gec.evidence_category, a.term_key, " +
					"asn.by_isoform, msn.by_symbol " +
					"from annotation as a " +
					"join marker_to_annotation as mta on a.annotation_key = mta.annotation_key " +
					"join annotation_sequence_num as asn on a.annotation_key = asn.annotation_key " +
					"join go_evidence_category as gec on a.evidence_code = gec.evidence_code " +
					"join marker_sequence_num msn on msn.marker_Key = mta.marker_key " +
					"where a.object_type = 'Marker' "
					+ " and a.annotation_key between " + minAnnotKey + " and " + maxAnnotKey + " ");


			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			// Parse the main query results here.

			while (rs.next()) {

				String annotKey = rs.getString("annotation_key");

				String termKey = rs.getString("term_key");
				String qualifier = rs.getString("qualifier");
				String category = rs.getString("evidence_category");

				String byEvidenceTerm = rs.getString("evidence_code") + " - " + rs.getString("evidence_term");

				if (qualifier == null) {
					qualifier = "";
				}
				qualifier = qualifier.toLowerCase();

				SolrInputDocument doc = new SolrInputDocument();
				doc.addField(IndexConstants.MRK_KEY, rs.getString("marker_key"));
				doc.addField(IndexConstants.ANNOTATION_KEY, annotKey);
				doc.addField(IndexConstants.VOC_TERM, rs.getString("term"));
				doc.addField(IndexConstants.VOC_ID, rs.getString("term_id"));
				doc.addField(IndexConstants.VOC_VOCAB, rs.getString("vocab_name"));
				doc.addField(IndexConstants.VOC_DAG_NAME, rs.getString("dag_name"));

				// Sort fields

				doc.addField(IndexConstants.BY_EVIDENCE_CODE, rs.getString("evidence_code"));
				doc.addField(IndexConstants.BY_EVIDENCE_TERM, byEvidenceTerm);
				doc.addField(IndexConstants.VOC_BY_DAG_STRUCT, rs.getString("by_dag_structure"));
				doc.addField(IndexConstants.VOC_BY_DAG_TERM, rs.getString("by_vocab_dag_term"));
				doc.addField(IndexConstants.BY_MRK_DAG_TERM, rs.getString("by_object_dag_term"));
				doc.addField(IndexConstants.BY_ISOFORM, rs.getString("by_isoform"));
				doc.addField(IndexConstants.MRK_BY_SYMBOL, rs.getString("by_symbol"));


				doc.addField(IndexConstants.EVIDENCE_CATEGORY, category);
				doc.addField(IndexConstants.VOC_QUALIFIER, qualifier);

				// include references for each annotation

				if (annotToRefs.containsKey(annotKey)) {
					for (String refsKey : annotToRefs.get(annotKey)) {
						doc.addField(IndexConstants.REF_KEY, refsKey);
					}
				}

				// include header terms where available
				if (termToHeaders.containsKey(termKey)) {
					for (String header : termToHeaders.get(termKey)) {
						doc.addField(IndexConstants.SLIM_TERM, header);
					}
				}


				// include GO IDs of ancestors
				if (ancestorIds.containsKey(termKey)) {
					for (String ancestorId : ancestorIds.get(termKey)) {
						doc.addField(IndexConstants.VOC_ID, ancestorId);
					}
				}

				// if we have ancestors, add their ids, and ancestor synonyms
				//           		if(this.ancestorIds.containsKey(mt.termId))
				//           		{
				//           			for(String ancestorId : this.ancestorIds.get(mt.termId))
				//           			{
				//           				doc.addField(IndexConstants.MRK_TERM_ID,ancestorId);
				//                   		this.addAllFromLookupNoDups(doc,termField,ancestorId,this.termSynonyms);
				//           			}
				//           		}





				docs.add(doc);

				if (docs.size() > 5000) {
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>();
				}
			}
			writeDocs(docs);
			commit();
		}

		logger.info("Done adding to solr; completed markerAnnotation index.");
	}
}
