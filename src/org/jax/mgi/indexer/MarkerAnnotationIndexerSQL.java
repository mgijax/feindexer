package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * The template indexer
 * @author mhall
 * Copy this code to create a new indexer, and then just change the appropriate sections.
 * 
 * If you need chunking go and take the code from the sequence indexer.
 * 
 * Note: refactored during 5.x development
 */

public class MarkerAnnotationIndexerSQL extends Indexer {

   
    public MarkerAnnotationIndexerSQL () {
        super("index.url.markerAnnotation");
    }
    
    public void index() throws Exception
    {
            // count of annotations
            
            ResultSet rs_tmp = ex.executeProto("select max(annotation_key) as maxAnnotKey from annotation");
            rs_tmp.next();
            
            System.out.println("Max Term Number: " + rs_tmp.getString("maxAnnotKey") + " Timing: "+ ex.getTiming());
            String start = "0";
            String end = rs_tmp.getString("maxAnnotKey");
                                               
	    // get the references for each annotation

	    logger.info("Finding references for annotations");
	    String annotToRefSQL =
		"select distinct annotation_key, reference_key"
		+ " from annotation_reference";

	    logger.info(annotToRefSQL);
	    HashMap<String, HashSet<String>> annotToRefs = makeHash(
		annotToRefSQL, "annotation_key", "reference_key");

	    logger.info("Found refs for " + annotToRefs.size()
		+ " annotations");

            // Setup the main query here
            
            logger.info("Getting all marker annotations.");
            ResultSet rs_overall = ex.executeProto("select a.annotation_key, a.vocab_name, a.term, " +
            		                                "a.term_id, a.qualifier, mta.marker_key, a.dag_name, asn.by_dag_structure, asn.by_vocab_dag_term, asn.by_object_dag_term " +
                                                    "from annotation as a " +
                                                    "join marker_to_annotation as mta on a.annotation_key = mta.annotation_key " +
                                                    "join annotation_sequence_num as asn on a.annotation_key = asn.annotation_key " +
                                                    "where a.object_type = 'Marker'");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the main query results here.
            
	    String annotKey;			// current annotation key

            logger.info("Parsing marker annotations");
            while (!rs_overall.isAfterLast()) {
		annotKey = rs_overall.getString("annotation_key");

                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.MRK_KEY, rs_overall.getString("marker_key"));
                doc.addField(IndexConstants.ANNOTATION_KEY, annotKey);
                doc.addField(IndexConstants.VOC_TERM, rs_overall.getString("term"));
                doc.addField(IndexConstants.VOC_ID, rs_overall.getString("term_id"));
                doc.addField(IndexConstants.VOC_VOCAB, rs_overall.getString("vocab_name"));
                doc.addField(IndexConstants.VOC_DAG_NAME, rs_overall.getString("dag_name"));
                doc.addField(IndexConstants.VOC_BY_DAG_STRUCT, rs_overall.getString("by_dag_structure"));
                doc.addField(IndexConstants.VOC_BY_DAG_TERM, rs_overall.getString("by_vocab_dag_term"));
                doc.addField(IndexConstants.BY_MRK_DAG_TERM, rs_overall.getString("by_object_dag_term"));

                String qualifier = rs_overall.getString("qualifier");
                if (qualifier == null) {
                    qualifier = "";
                }
                qualifier = qualifier.toLowerCase();
                
                doc.addField(IndexConstants.VOC_QUALIFIER, qualifier);

		// include references for each annotation

		if (annotToRefs.containsKey(annotKey)) {
		    for (String refsKey: annotToRefs.get(annotKey)) {
			doc.addField(IndexConstants.REF_KEY, refsKey);
		    }
		}

		// not sure why this is here or what it's doing; suspect it's
		// old debugging code

                if (annotKey == null) {
                    logger.info("String case" + doc.toString());
                }
                
                rs_overall.next();
                                
                docs.add(doc);
                
                if (docs.size() > 10000) {
                    logger.info("Adding a stack of the documents to Solr");
                    server.add(docs);
                    server.commit();
                    docs = new ArrayList<SolrInputDocument>();
                    logger.info("Done adding to solr, Moving on");
                }
            }
            
            logger.info("Done adding to solr;"
		+ " completed marker annotation index.");
            server.add(docs);
            server.commit();
    }
}
