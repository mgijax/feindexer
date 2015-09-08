package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * The indexer for the interaction index - containing data on which markers
 * interact with other markers.  Added for Feature Relationships project,
 * Spring 2014, team scrum-bob.
 *
 * @author jsb
 */

public class InteractionIndexerSQL extends Indexer {


	public InteractionIndexerSQL () {
		super("index.url.interaction");
	}

	public void index() throws Exception {
		// get maximum mi_key for use in stepping through chunks

		ResultSet rs_tmp = ex.executeProto( "select max(mi_key) as maxRegKey " + "from marker_interaction");
		rs_tmp.next();

		int maxKey = rs_tmp.getInt("maxRegKey");

		// SQL to gather the properties we need to recognize for
		// 'interacts_with' relationships:  score, mature transcript, and
		// notes.

		String propSQL = "select mi_key, value, sequence_num "
				+ "from marker_interaction_property "
				+ "where name = '<NAME>' "
				+ "  and mi_key > <START_KEY> "
				+ "  and mi_key <= <END_KEY> "
				+ "order by sequence_num";

		// SQL to gather basic information about interacting markers

		String basicSQL = "select mrm.mi_key, "
				+ " org.primary_id as organizingMarkerID, "
				+ " org.symbol as organizerSymbol, "
				+ " mrm.interacting_marker_symbol as participantMarkerSymbol, "
				+ " mrm.interacting_marker_id as participantMarkerID, "
				+ " mrm.relationship_term, "
				+ " mrm.qualifier, "
				+ " mrm.evidence_code, "
				+ " mrm.jnum_id, "
				+ " mrm.sequence_num, "
				+ " osn.by_symbol as organizerSeqNum, "
				+ " psn.by_symbol as participantSeqNum "
				+ "from marker_interaction mrm "
				+ "inner join marker org on (mrm.marker_key = org.marker_key) "
				+ "inner join marker_sequence_num osn on (mrm.marker_key = osn.marker_key) "
				+ "inner join marker_sequence_num psn on (mrm.interacting_marker_key = psn.marker_key) "
				+ "where mrm.mi_key > <START_KEY> "
				+ " and mrm.is_reversed = 0 "
				+ " and mrm.mi_key <= <END_KEY> ";

		// collection of Solr documents, waiting to be sent to Solr
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		// iterate through chunks

		int chunkSize = 150000;
		int startKey = 0;
		int endKey = startKey + chunkSize;

		while (startKey < maxKey) {
			logger.info ("Processing mi keys " + startKey + " to " + endKey);

			// gather our sets of optional properties for this chunk

			HashMap<String, HashSet<String>> miToScore =                makeHash(propSQL.replace("<NAME>", "score")                 .replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");
			HashMap<String, HashSet<String>> miToSource =               makeHash(propSQL.replace("<NAME>", "data_source")           .replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");
			HashMap<String, HashSet<String>> miToValidation =           makeHash(propSQL.replace("<NAME>", "validation")            .replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");
			HashMap<String, HashSet<String>> miToTranscript =           makeHash(propSQL.replace("<NAME>", "mature_transcript")     .replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");
			HashMap<String, HashSet<String>> miToNotes =                makeHash(propSQL.replace("<NAME>", "note")                  .replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");
			
			HashMap<String, HashSet<String>> miToAlgorithm =            makeHash(propSQL.replace("<NAME>", "algorithm")             .replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");
			HashMap<String, HashSet<String>> miToParticipantProductID = makeHash(propSQL.replace("<NAME>", "participant_product_ID").replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");
			HashMap<String, HashSet<String>> miToOrganizerProductID =   makeHash(propSQL.replace("<NAME>", "organizer_product_ID")  .replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");
			HashMap<String, HashSet<String>> miToOtherReferences =      makeHash(propSQL.replace("<NAME>", "other_refs")            .replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)),"mi_key", "value");

			// gather our basic interacts_with relationships for this chunk

			ResultSet rs = ex.executeProto (basicSQL.replace("<START_KEY>", Integer.toString(startKey)).replace("<END_KEY>", Integer.toString(endKey)) );

			// walk through our relationships for this chunk

			while (rs.next()) {

				// extract basic data fields from the current row

				String miKey = rs.getString("mi_key");
				String oMarkerID = rs.getString("organizingMarkerID");
				String pMarkerID = rs.getString("participantMarkerID");
				String oMarkerSymbol = rs.getString("organizerSymbol");
				String pMarkerSymbol = rs.getString("participantMarkerSymbol");
				String term = rs.getString("relationship_term");
				String qualifier = rs.getString("qualifier");
				String evidenceCode = rs.getString("evidence_code");
				String jnumID = rs.getString("jnum_id");
				int seqNum = rs.getInt("sequence_num");
				int oSeqNum = rs.getInt("organizerSeqNum");
				int pSeqNum = rs.getInt("participantSeqNum");

				// convert the J: number to just its numeric portion, to
				// use in sorting

				int jnum = 0;
				if (jnumID != null) {
					jnum = Integer.parseInt(jnumID.substring(2));
				}

				// build a new Solr doc & add it to the collection of docs

				SolrInputDocument doc = new SolrInputDocument();

				doc.addField(IndexConstants.REG_KEY, miKey);
				doc.addField(IndexConstants.ORGANIZER_ID, oMarkerID);
				doc.addField(IndexConstants.ORGANIZER_SYMBOL, oMarkerSymbol);
				doc.addField(IndexConstants.PARTICIPANT_ID, pMarkerID);
				doc.addField(IndexConstants.PARTICIPANT_SYMBOL, pMarkerSymbol);
				doc.addField(IndexConstants.RELATIONSHIP_TERM, term);
				doc.addField(IndexConstants.VOC_QUALIFIER, qualifier);
				doc.addField(IndexConstants.EVIDENCE_CODE, evidenceCode);
				doc.addField(IndexConstants.JNUM_ID, jnumID);
				doc.addField(IndexConstants.BY_JNUM_ID, jnum);
				doc.addField(IndexConstants.BY_MARKER_SYMBOL, seqNum);
				doc.addField(IndexConstants.BY_ORGANIZER_SYMBOL, oSeqNum);
				doc.addField(IndexConstants.BY_PARTICIPANT_SYMBOL, pSeqNum);

				String scoreToFilter = null;
				String scoreToSort = null;
				String validationToSort = null;

				if (miToScore.containsKey(miKey)) {
					for (String s : miToScore.get(miKey)) {
						doc.addField(IndexConstants.SCORE_VALUE, s);
						scoreToFilter = s;		// keep the last one
					}
				}

				if (miToSource.containsKey(miKey)) {
					for (String s : miToSource.get(miKey)) {
						doc.addField(IndexConstants.SCORE_SOURCE, s);
					}
				}

				if (miToValidation.containsKey(miKey)) {
					for (String s : miToValidation.get(miKey)) {
						doc.addField(IndexConstants.VALIDATION, s);
						validationToSort = s;	// keep the last one
					}
				}

				if (miToTranscript.containsKey(miKey)) {
					for (String s : miToTranscript.get(miKey)) {
						doc.addField(IndexConstants.MATURE_TRANSCRIPT, s);
					}
				}
								
				if (miToParticipantProductID.containsKey(miKey)) {
					for (String s : miToParticipantProductID.get(miKey)) {
						doc.addField(IndexConstants.PARTICIPANT_PRODUCT_ID, s);
					}
				}
				
				if (miToOrganizerProductID.containsKey(miKey)) {
					for (String s : miToOrganizerProductID.get(miKey)) {
						doc.addField(IndexConstants.ORGANIZER_PRODUCT_ID, s);
					}
				}

				if (miToAlgorithm.containsKey(miKey)) {
					for (String s : miToAlgorithm.get(miKey)) {
						doc.addField(IndexConstants.ALGORITHM, s);
					}
				}
				
				if (miToOtherReferences.containsKey(miKey)) {
					for (String s : miToOtherReferences.get(miKey)) {
						doc.addField(IndexConstants.OTHER_REFERENCES, s);
					}
				}

				if (miToNotes.containsKey(miKey)) {
					for (String s : miToNotes.get(miKey)) {
						doc.addField(IndexConstants.NOTES, s);
					}
				}

				scoreToSort = scoreToFilter;

				// special cases to enable special behavior...
				// 1. include validated and inferred relationships when
				//    filtering by score (so give them a high fake score)
				// 2. when sorting by validation, prefer validated before
				//    inferred before predicted before everything else
				//    (which sorts alphabetically)
				// 3. when sorting by score, ensure that rows without a
				//    score (validated or inferred rows) sort below any
				//    rows with a score

				if ("validated".equals(validationToSort)) {
					validationToSort = "0";
					scoreToFilter = "2.0";
					scoreToSort = "-3";
				} else if ("inferred".equals(validationToSort)) {
					validationToSort = "1";
					scoreToFilter = "2.0";
					scoreToSort = "-2";
				} else if ("predicted".equals(validationToSort)) {
					validationToSort = "2";
				} 

				if (scoreToSort == null) {
					// should not happen
					scoreToSort = "0";
				}

				if (validationToSort != null) {
					doc.addField(IndexConstants.VALIDATION_SORTABLE, validationToSort);
				}
				if (scoreToFilter != null) {
					doc.addField(IndexConstants.SCORE_FILTERABLE, scoreToFilter);
				}
				if (scoreToSort != null) {
					doc.addField(IndexConstants.SCORE_SORTABLE, scoreToSort);
				}

				// add to list of documents to be indexed

				docs.add(doc);

				// keep memory requirements down by writing to Solr every
				// 20k documents

				if (docs.size() > 20000) {
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>();
				}

			} // end while loop - walking through this result set

			startKey = endKey;
			endKey = startKey + chunkSize;

		} // end while loop - processing chunks of mi_key values

		// very likely, we'll have some extra docs left to write

		if (!docs.isEmpty()) {
			server.add(docs);
		}
		server.commit();
		logger.info("Done");
	}
}
