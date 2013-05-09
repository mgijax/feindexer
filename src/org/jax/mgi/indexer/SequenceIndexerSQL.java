package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * SequenceIndexerSQL
 * @author mhall
 * This class has the primary responsibility to fill out the sequence index.
 * This is a very large index, and as such requires chunking.
 * 
 * Refactored during 5.x development
 */

public class SequenceIndexerSQL extends Indexer {
    
    public SequenceIndexerSQL () {
        super("index.url.sequence");
    }
    
    public void index() throws Exception
    {
    	stopThreadLogging();
        
	String provider;
            
            // Find out how big the result set can be, and then setup the chunking.
            
            ResultSet rs_tmp = ex.executeProto("select max(sequence_key) as max_seq_key from sequence");
            rs_tmp.next();
            
            logger.info("Max Seq Number: " + rs_tmp.getString("max_seq_key") + " Timing: "+ ex.getTiming());
            Integer start = 0;
            Integer end = rs_tmp.getInt("max_seq_key");

            // This needs to be a configurable value
            
            int chunkSize = 500000;
            
            int modValue = end.intValue() / chunkSize;
            
            // Perform the chunking, this might become a configurable value later on
            // TODO Figure out whether or not to make this configurable.
            
            for (int i = 0; i <= modValue; i++) {
            
            start = i * chunkSize;
            end = start + chunkSize;

            // Sequence -> References
            
	    logger.info ("Processing seq key > " + start + " and <= " + end);

            //logger.info("Selecting all sequence references");
            String sequenceToRefSQL = "select sequence_key, reference_key from reference_to_sequence where sequence_key > " + start + " and sequence_key <= "+ end;
            //logger.info(sequenceToRefSQL);
            HashMap <String, HashSet <String>> seqToReference = makeHash(sequenceToRefSQL, "sequence_key", "reference_key");

            // Sequence -> Markers
            
            //logger.info("Selecting all sequence Markers");
            String sequenceToMarkerSQL = "select sequence_key, marker_key from marker_to_sequence where sequence_key > " + start + " and sequence_key <= "+ end;
            //logger.info(sequenceToMarkerSQL);
            HashMap <String, HashSet <String>> seqToMarker = makeHash(sequenceToMarkerSQL, "sequence_key", "marker_key");
            
            // Sequence -> IDs
            
            //logger.info("Selecting all sequence ids");
            String sequenceIDs = "select sequence_key, acc_id from sequence_id where private != 1 and sequence_key > " + start + " and sequence_key <= "+ end;
            //logger.info(sequenceIDs);
            HashMap <String, HashSet <String>> seqIDs = makeHash(sequenceIDs, "sequence_key", "acc_id");

            
            // The main query
            
            //logger.info("Getting all sequences");
            ResultSet rs_overall = ex.executeProto("select s.sequence_key, ssn.by_sequence_type, ssn.by_provider, s.length, s.provider " +
                    "from sequence as s inner join sequence_sequence_num ssn on s.sequence_key = ssn.sequence_key where s.sequence_key > " 
                    + start + " and s.sequence_key <= " + end);
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse the main query results here.
            
            //logger.info("Parsing them");
            while (rs_overall.next()) {
                provider = rs_overall.getString("provider");

		if (provider.equals("TrEMBL") ||
		    provider.equals("SWISS-PROT")) {
		    	provider = "UniProt";
		}

                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.SEQ_KEY, rs_overall.getString("sequence_key"));
                doc.addField(IndexConstants.SEQ_PROVIDER, provider);
                doc.addField(IndexConstants.SEQ_TYPE_SORT, rs_overall.getString("by_sequence_type"));
                doc.addField(IndexConstants.SEQ_PROVIDER_SORT, rs_overall.getString("by_provider"));
                if (rs_overall.getString("length") != null && ! rs_overall.getString("length").equals("")) {
                    doc.addField(IndexConstants.SEQ_LENGTH, rs_overall.getString("length"));
                }
                else {
                    doc.addField(IndexConstants.SEQ_LENGTH, "0");
                }
                
            
                // Parse the 1->n reference relationship here, adding in the reference keys
                
                if (seqToReference.containsKey(rs_overall.getString("sequence_key"))) {
                    for (String referenceKey: seqToReference.get(rs_overall.getString("sequence_key"))) {
                        doc.addField(IndexConstants.REF_KEY, referenceKey);
                    }
                }
                
                // Parse the 1->N marker relationship here, adding in the marker keys
                
                if (seqToMarker.containsKey(rs_overall.getString("sequence_key"))) {
                    for (String markerKey: seqToMarker.get(rs_overall.getString("sequence_key"))) {
                        doc.addField(IndexConstants.MRK_KEY, markerKey);
                    }
                }

                // Parse the 1->N sequence id relationships here, adding in the ids
                
                if (seqIDs.containsKey(rs_overall.getString("sequence_key"))) {
                    for (String accID: seqIDs.get(rs_overall.getString("sequence_key"))) {
                        doc.addField(IndexConstants.SEQ_ID, accID);
                    }
                }
                
                docs.add(doc);
                
                if (docs.size() > 20000) {
                    //logger.info("Adding a stack of the documents to Solr");
                    writeDocs(docs);
                    docs = new ArrayList<SolrInputDocument>();
                    //logger.info("Done adding to solr, Moving on");
                }
            }
            // Add in the last set of docs, in case there is less than 10000, since solr
            // will just replace the documents with new copies, there shouldn't be an issue.
            if (! docs.isEmpty()) {
                server.add(docs);
            }
            }
            server.commit();
    }
}
