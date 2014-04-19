package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * CreIndexerSQL
 * @author mhall
 * 
 * This class is responsible for populating the cre index.  This index is 
 * fairly straight forward, with only one sub object relationship.
 * 
 * Note: refactored during 5.x development
 */

public class CreAssayResultIndexerSQL extends Indexer {

   
    public CreAssayResultIndexerSQL () {
        super("index.url.creAssayResult");
    }

    /**
     * This is the method that is responsible for populating the index for cre.  It has one sub 
     * object relationship, and an extremely large main query.
     */
    
    public void index() throws Exception
    {       
            // The main sql for cre, this is a very large, but simple sql statement.
            
            logger.info("Getting all cre assay results");
            ResultSet rs_overall = ex.executeProto("select distinct rar.result_key," +
                    " ras.allele_id, ras.system_key, rarsn.by_structure, rarsn.by_age, " +
                    " rarsn.by_level, rarsn.by_pattern, rarsn.by_jnum_id, rarsn.by_assay_type," +
                    " rarsn.by_reporter_gene, rarsn.by_detection_method," + 
                    " rarsn.by_assay_note, rarsn.by_allelic_composition, rarsn.by_sex, " +
                    " rarsn.by_specimen_note, rarsn.by_result_note" +
                    " from recombinase_allele_system as ras" +
                    " join recombinase_assay_result as rar on ras.allele_system_key = rar.allele_system_key" +  
                    " join recombinase_assay_result_sequence_num as rarsn on rar.result_key = rarsn.result_key");
            
            rs_overall.next();
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the results, again this is a very large query, but fairly
            // flat and straightforward. 
            
            logger.info("Parsing them");
            while (!rs_overall.isAfterLast()) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.ALL_ID, rs_overall.getString("allele_id"));
                doc.addField(IndexConstants.CRE_SYSTEM_KEY, rs_overall.getString("system_key"));
                doc.addField(IndexConstants.CRE_ASSAY_RESULT_KEY, rs_overall.getString("result_key"));
                
                // Add in all the sorting columns
                
                doc.addField(IndexConstants.CRE_BY_STRUCTURE, rs_overall.getString("by_structure"));
                doc.addField(IndexConstants.CRE_BY_AGE, rs_overall.getString("by_age"));
                doc.addField(IndexConstants.CRE_BY_LEVEL, rs_overall.getString("by_level"));
                doc.addField(IndexConstants.CRE_BY_PATTERN, rs_overall.getString("by_pattern"));
                doc.addField(IndexConstants.CRE_BY_JNUM_ID, rs_overall.getString("by_jnum_id"));
                doc.addField(IndexConstants.CRE_BY_ASSAY_TYPE, rs_overall.getString("by_assay_type"));
                doc.addField(IndexConstants.CRE_BY_REPORTER_GENE, rs_overall.getString("by_reporter_gene"));
                doc.addField(IndexConstants.CRE_BY_DETECTION_METHOD, rs_overall.getString("by_detection_method"));
                doc.addField(IndexConstants.CRE_BY_ASSAY_NOTE, rs_overall.getString("by_assay_note"));
                doc.addField(IndexConstants.CRE_BY_ALLELIC_COMPOSITION, rs_overall.getString("by_allelic_composition"));
                doc.addField(IndexConstants.CRE_BY_SEX, rs_overall.getString("by_sex"));
                doc.addField(IndexConstants.CRE_BY_SPECIMEN_NOTE, rs_overall.getString("by_specimen_note"));
                doc.addField(IndexConstants.CRE_BY_RESULT_NOTE, rs_overall.getString("by_result_note"));
                
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
