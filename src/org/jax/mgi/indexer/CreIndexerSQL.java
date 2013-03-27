package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * CreIndexerSQL
 * @author mhall
 * 
 * This class is responsible for populating the cre index.  This index is 
 * fairly straight forward, with only one sub object relationship.
 * 
 * Note: Refactored during 5.x development
 */

public class CreIndexerSQL extends Indexer {

   
    public CreIndexerSQL () {
        super("index.url.cre");
    }
    
    /**
     * This is the method that is responsible for populating the index for cre.  It has one sub 
     * object relationship, and an extremely large main query.
     */
    
    public void index() {
                
        try {
                       
            // The system sub object relationship.  This has no 
            // chunking as it shouldn't be needed.
            
            logger.info("Seleceting all allele systems relationships");
            String allToSystemSQL = "select allele_key, system from recombinase_allele_system where system != '' order by allele_key ";
            System.out.println(allToSystemSQL);
            HashMap <String, HashSet <String>> allToSystems = makeHash(allToSystemSQL, "allele_key", "system");


            // The main sql for cre, this is a very large, but simple sql statement.
            
            logger.info("Getting all cre alleles");
            ResultSet rs_overall = ex.executeProto("select ars.allele_key, " +
                "ars.detected_count, ars.not_detected_count, " + 
                "asn.by_symbol, asn.by_allele_type, asn.by_driver, " +
                "ac.reference_count, a.driver, a.inducible_note, " +
                "aic.strain_count " +  
                "from allele_recombinase_systems as ars " +
                "left outer join allele_sequence_num as asn " +
                "on ars.allele_key = asn.allele_key  " +
                "left outer join allele_counts as ac " +
                "on ars.allele_key = ac.allele_key " +
                "left outer join allele_imsr_counts as aic " + 
                "on ars.allele_key = aic.allele_key  " +
                "left outer join allele as a on ars.allele_key = a.allele_key " +
                "order by ars.allele_key");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the results, again this is a very large query, but fairly
            // flat and straightforward. 
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.ALL_KEY, rs_overall.getString("allele_key"));
                doc.addField(IndexConstants.ALL_DRIVER_SORT, rs_overall.getString("by_driver"));
                doc.addField(IndexConstants.ALL_DRIVER, rs_overall.getString("driver"));
                doc.addField(IndexConstants.ALL_INDUCIBLE, rs_overall.getString("inducible_note"));
                doc.addField(IndexConstants.ALL_IMSR_COUNT, rs_overall.getString("strain_count"));
                doc.addField(IndexConstants.ALL_REFERENCE_COUNT_SORT, rs_overall.getString("reference_count"));
                doc.addField(IndexConstants.ALL_TYPE_SORT, rs_overall.getString("by_allele_type"));
                doc.addField(IndexConstants.ALL_SYMBOL_SORT, rs_overall.getString("by_symbol"));
                doc.addField(IndexConstants.ALL_INDUCIBLE, rs_overall.getString("inducible_note"));
                doc.addField(IndexConstants.CRE_NOT_DETECTED_COUNT, rs_overall.getString("not_detected_count"));
                doc.addField(IndexConstants.CRE_DETECTED_COUNT, rs_overall.getString("detected_count"));
                doc.addField(IndexConstants.CRE_DETECTED_TOTAL_COUNT, rs_overall.getInt("not_detected_count") + rs_overall.getInt("detected_count"));
                
                // Bring in the multi-valued field allele system. 
                
                if (allToSystems.containsKey(rs_overall.getString("allele_key"))) {
                    for (String system: allToSystems.get(rs_overall.getString("allele_key"))) {
                        doc.addField(IndexConstants.CRE_ALL_SYSTEM, system);
                    }
                }
                
                rs_overall.next();
                
                docs.add(doc);
                
                if (docs.size() > 10000) {
                    logger.info("Adding a stack of the documents to Solr");
                    server.add(docs);
                    docs = new ArrayList<SolrInputDocument>();
                    logger.info("Done adding to solr, Moving on");
                }
            }
            logger.info("Adding final stack of the documents to Solr");
            server.add(docs);
            server.commit();
            
        } catch (Exception e) {e.printStackTrace();}
    }
    
    String doBit(String bit) {
        if (bit == null) {
            return "-1";
        }
        if (bit.equals("1")) {
            return "1";
        }
        return "0";
    }
}
