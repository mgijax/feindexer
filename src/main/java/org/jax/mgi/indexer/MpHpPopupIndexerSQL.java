package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;

/**
 * Indexer for MP HP Popup (from HDMC)
 */

public class MpHpPopupIndexerSQL extends Indexer {

    private Map<String, List<String>> termToSynonym = new HashMap<String, List<String>>();

	public MpHpPopupIndexerSQL () {
		super("mpHpPopup");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

    /* get the display value for a term's synonyms
     */
    private String getSynonymText(String termKey) throws Exception {

        String returnString = new String();
        if (termToSynonym.containsKey(termKey)) {
            List<String> thisSynList = termToSynonym.get(termKey);
            returnString = String.join(" | ", thisSynList);
        } else {
            returnString = "";
        }
        return returnString;
    }

    /* get the sort value for a term's vocab
     */
    private String getVocabSort(String searchTermID) throws Exception {

        String returnString = "b"; // lowest
        if (searchTermID.substring(0,2).equals("HP") )  {
            returnString = "a";
        } 
        return returnString;
    }

    /* get the sort value for a term's match value
     */
    private String getMatchTypeSort(String matchType) throws Exception {

        String returnString = "f"; // lowest
        if (matchType.equals("exactMatch") )  {
            returnString = "a";
        } 
        if (matchType.equals("closeMatch") )  {
            returnString = "b";
        } 
        if (matchType.equals("narrowMatch") )  {
            returnString = "c";
        } 
        if (matchType.equals("broadMatch") )  {
            returnString = "d";
        } 
        if (matchType.equals("relatedMatch") )  {
            returnString = "e";
        } 
        return returnString;
    }

    /* get the sort value for a term's match method
     */
    private String getMatchMethodSort(String matchMethod) throws Exception {

        String returnString = "c"; // lowest
        if (matchMethod.equals("ManualMappingCuration") )  {
            returnString = "a";
        } 
        if (matchMethod.equals("LogicalReasoning") )  {
            returnString = "b";
        } 
        return returnString;
    }

	/*------------------------------*/
	/*--- public indexer methods ---*/
	/*------------------------------*/

	public void index() throws Exception
	{
		// pre-gather synonyms; these are 1-n for each term
        logger.info("Selecting all relevant term synonyms");
        ResultSet rs_synonyms = ex.executeProto("SELECT distinct " +
                " t1.primary_id, ts.synonym  " +
                "from term_to_term t2t, term_synonym ts, term t1 " +
                "where relationship_type = 'MP HP Popup' " +
                "  AND t2t.term_key_2 = ts.term_key " +
                "  AND t2t.term_key_2 = t1.term_key " +
                "order by synonym " +
                " "); 
		while (rs_synonyms.next()) {
			String thisTermKey = rs_synonyms.getString("primary_id");
			String thisSynonym = rs_synonyms.getString("synonym");
			if (termToSynonym.containsKey(thisTermKey)) {
				List<String> oldSynList = termToSynonym.get(thisTermKey);
				oldSynList.add(thisSynonym);
				termToSynonym.put(thisTermKey, oldSynList);
			} else {
				List newSynList = new ArrayList<String>();
				newSynList.add(thisSynonym);
				termToSynonym.put(thisTermKey, newSynList);
			}

		}
		logger.info("Terms with synonyms: " + termToSynonym.size());

        logger.info("Selecting all associations from term_to_term");
        ResultSet rs_overall = ex.executeProto("SELECT " +
                " t2t.relationship_type,  " +
                " t2t.evidence,  " +
                " t2t.cross_reference, " +
                " t1.primary_id as searchID, " +
                " t2.primary_id as matchID, " +
                " t1.term as searchTerm, " +
                " t2.term as matchTerm, " +
                " t1.definition as searchTermDefinition, " +
                " t2.definition as matchTermDefinition " +
                "FROM term_to_term t2t, term t1, term t2 " +
                "WHERE relationship_type = 'MP HP Popup' " +
                "  AND t2t.term_key_1 = t1.term_key " +
                "  AND t2t.term_key_2 = t2.term_key " +
                "ORDER BY " +
                "  CASE evidence " +
                "      WHEN 'LexicalMatching' THEN 1 " +
                "      WHEN 'mgiSynonymLexicalMatching' THEN 2 " +
                "      WHEN 'mgiTermLexicalMatching' THEN 3 " +
                "      WHEN 'LogicalReasoning' THEN 4 " +
                "      WHEN 'ManualMappingCuration' THEN 5 " +
                "      ELSE 6 " +
                "      END, " +
                "   CASE cross_reference " +
                "      WHEN 'relatedMatch' THEN 1 " +
                "      WHEN 'broadMatch' THEN 2 " +
                "      WHEN 'narrowMatch' THEN 3 " +
                "      WHEN 'closeMatch' THEN 4 " +
                "      WHEN 'exactMatch' THEN 5 " +
                "      ELSE 6 " +
                "      END "); 

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		logger.info("Creating Docs");
		while (rs_overall.next()) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("uniqueKey", rs_overall.getString("searchID") + "-" + rs_overall.getString("matchID"));
			doc.addField("searchTermID", rs_overall.getString("searchID"));
			doc.addField("searchTerm", rs_overall.getString("searchTerm"));
			doc.addField("searchTermDefinition", rs_overall.getString("searchTermDefinition"));
			doc.addField("matchTermID", rs_overall.getString("matchID"));
			doc.addField("matchTerm", rs_overall.getString("matchTerm"));
			doc.addField("matchType", rs_overall.getString("cross_reference"));
			doc.addField("matchMethod", rs_overall.getString("evidence"));
			doc.addField("matchTermDefinition", rs_overall.getString("matchTermDefinition"));
			doc.addField("matchTermSynonym", getSynonymText(rs_overall.getString("matchID")));
			doc.addField("vocabSort", getVocabSort(rs_overall.getString("searchID")));
			doc.addField("matchTypeSort", getMatchTypeSort(rs_overall.getString("cross_reference")));
			doc.addField("matchMethodSort", getMatchMethodSort(rs_overall.getString("evidence")));
			docs.add(doc);
		}
		logger.info("Created Docs: " + docs.size());

        // write these documents out to the index
		writeDocs(docs);
		commit();

	}
}
