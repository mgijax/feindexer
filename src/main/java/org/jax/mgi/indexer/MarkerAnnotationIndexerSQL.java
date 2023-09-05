package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
		super("markerAnnotation");
	}

	public Map<String,Set<String>> getAncestorIDs() throws Exception {
		String goAncestorQuery = "select tas.term_key, ancestor_primary_id " +
				"from term t join term_ancestor tas on tas.term_key=t.term_key " +
				"where t.vocab_name in ('GO','InterPro Domains') " +
				"and tas.ancestor_term not in ('cellular_component','biological_process','molecular_function') ";

		Map<String,Set<String>> ancestorIds =
			this.populateLookup(goAncestorQuery, "term_key", "ancestor_primary_id", "term key -> Ancestor Term ID");
		logger.info("Ancestor Id mapping is complete.  Size: " + ancestorIds.size() );
		return ancestorIds;
	}
	
	public Map<String, Integer> getSlimTermSequenceNumbers() throws Exception {
		String slimTermQuery = "select distinct h.abbreviation as term "
			+ "from annotation a, term_to_header t, term h "
			+ "where a.vocab_name = 'GO' "
			+ " and a.term_key = t.term_key "
			+ " and t.header_term_key = h.term_key "
			+ "order by 1";
		
		Map<String, Integer> seqNum = new HashMap<String, Integer>();
		ResultSet rs = ex.executeProto(slimTermQuery);
		while (rs.next()) {
			seqNum.put(rs.getString("term"), seqNum.size());
		}
		rs.close();
		logger.info("Ordered " + seqNum.size() + " slim terms");
		return seqNum;
	}
	
	public Map<String,Integer> getReferenceSequenceNumbers() throws Exception {
		String referenceQuery = "select reference_key, by_primary_id from reference_sequence_num";
		Map<String,Integer> seqNum = new HashMap<String,Integer>();
		ResultSet rs = ex.executeProto(referenceQuery);
		while (rs.next()) {
			seqNum.put(rs.getString("reference_key"), rs.getInt("by_primary_id"));
		}
		rs.close();
		logger.info("Ordered " + seqNum.size() + " references");
		return seqNum;
	}
	
	public Map<String,Set<String>> getHeaderTerms() throws Exception {
		String headerQuery = "select distinct tth.term_key, h.abbreviation as header_term "
			+ "from term_to_header tth "
			+ "join term h on h.term_key = tth.header_term_key "
			+ "where exists (select 1 from annotation a where a.term_key = tth.term_key "
			+ " and a.object_type = 'Marker')";
		Map<String,Set<String>> headers = new HashMap<String,Set<String>>();
		ResultSet rs = ex.executeProto(headerQuery);
		while (rs.next()) {
			String termKey = rs.getString("term_key");
			if (!headers.containsKey(termKey)) {
				headers.put(termKey, new HashSet<String>());
			}
			headers.get(termKey).add(rs.getString("header_term"));
		}
		rs.close();
		logger.info("Got headers for " + headers.size() + " terms");
		return headers;
	}
	
	public void index() throws Exception
	{
		// count of annotations

		ResultSet rs = ex.executeProto("select min(marker_key) as minKey, max(marker_key) as maxKey "
			+ "from marker_to_annotation");
		rs.next();
		int minMarkerKey = rs.getInt("minKey");		// minimum marker key to be processed
		int maxMarkerKey = rs.getInt("maxKey");		// maximum marker key to be processed
		rs.close();

		// ancestor IDs for a given GO term key
		Map<String,Set<String>> ancestorIds = this.getAncestorIDs();

		// sequence numbers for slim terms
		Map<String, Integer> slimTermSequenceNumbers = this.getSlimTermSequenceNumbers();
		
		// sequence numbers for references
		Map<String,Integer> referenceSequenceNumbers = this.getReferenceSequenceNumbers();
		
		// header terms for a given term key
		Map<String,Set<String>> termToHeaders = this.getHeaderTerms();

		int chunkSize = 50000;
		int startKey = minMarkerKey;
		int endKey = startKey + chunkSize;

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		while (startKey <= maxMarkerKey) {
			logger.info("Building documents for markers " + startKey + " to " + (endKey - 1));

			// get the references for each annotation
			logger.info(" - Finding references for annotations");
			String annotToRefSQL = "select ar.annotation_key, ar.reference_key "
					+ "from marker_to_annotation a "
					+ "inner join annotation_reference ar on (a.annotation_key = ar.annotation_key)"
					+ "where a.marker_key >= " + startKey
					+ " and a.marker_key < " + endKey;

			logger.info(annotToRefSQL);
			Map<String, Set<String>> annotToRefs = this.populateLookup(annotToRefSQL, "annotation_key", "reference_key",
					"annotation_key to reference_keys");

			logger.info(" - Found refs for " + annotToRefs.size() + " annotations");

			// Setup the main query here
			// A unique annotation is here defined as: (annotation key, vocab name, term, evidence code,
			//	evidence term, term ID, qualifier, marker key, DAG name, evidence category, term key)

			logger.info(" - Getting all marker annotations.");
			rs = ex.executeProto("select a.annotation_key, a.vocab_name, a.term, a.evidence_code, a.evidence_term, " +
					"a.term_id, a.qualifier, mta.marker_key, a.dag_name, asn.by_dag_structure, asn.by_vocab_dag_term, " +
					"asn.by_object_dag_term, gec.evidence_category, a.term_key, " +
					"asn.by_isoform, msn.by_symbol " +
					"from annotation as a " +
					"join marker_to_annotation as mta on a.annotation_key = mta.annotation_key " +
					"join annotation_sequence_num as asn on a.annotation_key = asn.annotation_key " +
					"join go_evidence_category as gec on a.evidence_code = gec.evidence_code " +
					"join marker_sequence_num msn on msn.marker_Key = mta.marker_key " +
					"where mta.marker_key >= " + startKey + " and mta.marker_key < " + endKey);

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

				int byRefs = 9999999;
				if (annotToRefs.containsKey(annotKey)) {
					for (String refsKey : annotToRefs.get(annotKey)) {
						doc.addField(IndexConstants.REF_KEY, refsKey);
						byRefs = Math.min(byRefs, referenceSequenceNumbers.get(refsKey));
					}
				}
				doc.addField(IndexConstants.BY_REFERENCE, byRefs);

				// include header terms where available
				int byHeaders = 9999999;
				if (termToHeaders.containsKey(termKey)) {
					for (String header : termToHeaders.get(termKey)) {
						doc.addField(IndexConstants.SLIM_TERM, header);
						if (slimTermSequenceNumbers.containsKey(header)) {
							byHeaders = Math.min(byHeaders, slimTermSequenceNumbers.get(header));
						}
					}
				}
				doc.addField(IndexConstants.BY_CATEGORY, byHeaders);

				// include GO IDs of ancestors
				if (ancestorIds.containsKey(termKey)) {
					for (String ancestorId : ancestorIds.get(termKey)) {
						doc.addField(IndexConstants.VOC_ID, ancestorId);
					}
				}

				docs.add(doc);

				if (docs.size() > 5000) {
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>();
				}
			} // end -- while rs.next()
			startKey = endKey;
			endKey = endKey + chunkSize;
		}
		writeDocs(docs);
		commit();
		logger.info("Done adding to solr; completed markerAnnotation index.");
	}
}
