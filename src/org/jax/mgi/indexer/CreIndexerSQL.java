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
            	"ars.in_adipose_tissue, " +
            	"ars.in_alimentary_system, ars.in_branchial_arches, " +
                "ars.in_cardiovascular_system, " +
                "ars.in_cavities_and_linings, " +
                "ars.in_endocrine_system, ars.in_head, " + 
                "ars.in_hemolymphoid_system, ars.in_integumental_system, " +
                "ars.in_limbs, ars.in_liver_and_biliary_system, " +
                "ars.in_mesenchyme, ars.in_muscle, ars.in_nervous_system, " +
                "ars.in_renal_and_urinary_system, " + 
                "ars.in_reproductive_system, ars.in_respiratory_system, " +
                "ars.in_sensory_organs, ars.in_skeletal_system, " + 
                "ars.in_tail, ars.in_early_embryo, " +
                "ars.in_extraembryonic_component, " +
                "ars.in_embryo_other, ars.in_postnatal_other, " + 
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
                doc.addField(IndexConstants.CRE_IN_ADIPOSE_TISSUE, doBit(rs_overall.getString("in_adipose_tissue")));
                doc.addField(IndexConstants.CRE_IN_ALIMENTARY_SYSTEM, doBit(rs_overall.getString("in_alimentary_system")));
                doc.addField(IndexConstants.CRE_IN_BRANCHIAL_ARCHES, doBit(rs_overall.getString("in_branchial_arches")));
                doc.addField(IndexConstants.CRE_IN_CARDIOVASCULAR_SYSTEM, doBit(rs_overall.getString("in_cardiovascular_system")));
                doc.addField(IndexConstants.CRE_IN_CAVITIES_AND_LININGS, doBit(rs_overall.getString("in_cavities_and_linings")));
                doc.addField(IndexConstants.CRE_IN_EARLY_EMBRYO, doBit(rs_overall.getString("in_early_embryo")));
                doc.addField(IndexConstants.CRE_IN_EMBRYO_OTHER, doBit(rs_overall.getString("in_embryo_other")));
                doc.addField(IndexConstants.CRE_IN_ENDOCRINE_SYSTEM, doBit(rs_overall.getString("in_endocrine_system")));
                doc.addField(IndexConstants.CRE_IN_EXTRAEMBRYONIC_COMPONENT, doBit(rs_overall.getString("in_extraembryonic_component")));
                doc.addField(IndexConstants.CRE_IN_HEAD, doBit(rs_overall.getString("in_head")));
                doc.addField(IndexConstants.CRE_IN_HEMOLYMPHOID_SYSTEM, doBit(rs_overall.getString("in_hemolymphoid_system")));
                doc.addField(IndexConstants.CRE_IN_INTEGUMENTAL_SYSTEM, doBit(rs_overall.getString("in_integumental_system")));
                doc.addField(IndexConstants.CRE_IN_LIMBS, doBit(rs_overall.getString("in_limbs")));
                doc.addField(IndexConstants.CRE_IN_LIVER_AND_BILIARY_SYSTEM, doBit(rs_overall.getString("in_liver_and_biliary_system")));
                doc.addField(IndexConstants.CRE_IN_MESENCHYME, doBit(rs_overall.getString("in_mesenchyme")));
                doc.addField(IndexConstants.CRE_IN_MUSCLE, doBit(rs_overall.getString("in_muscle")));
                doc.addField(IndexConstants.CRE_IN_NERVOUS_SYSTEM, doBit(rs_overall.getString("in_nervous_system")));
                doc.addField(IndexConstants.CRE_IN_POSTNATAL_OTHER, doBit(rs_overall.getString("in_postnatal_other")));
                doc.addField(IndexConstants.CRE_IN_RENAL_AND_URINARY_SYSTEM, doBit(rs_overall.getString("in_renal_and_urinary_system")));
                doc.addField(IndexConstants.CRE_IN_REPRODUCTIVE_SYSTEM, doBit(rs_overall.getString("in_reproductive_system")));
                doc.addField(IndexConstants.CRE_IN_RESPIRATORY_SYSTEM, doBit(rs_overall.getString("in_respiratory_system")));
                doc.addField(IndexConstants.CRE_IN_SENSORY_ORGANS, doBit(rs_overall.getString("in_sensory_organs")));
                doc.addField(IndexConstants.CRE_IN_SKELETAL_SYSTEM, doBit(rs_overall.getString("in_skeletal_system")));
                doc.addField(IndexConstants.CRE_IN_TAIL, doBit(rs_overall.getString("in_tail")));
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
