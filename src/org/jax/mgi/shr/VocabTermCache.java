package org.jax.mgi.shr;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Is: a cache of VocabTerm objects, with knowledge of how to retrieve relevant data and instantiate the objects
public class VocabTermCache {
	public static int cursorLimit = 10000;
	
	// maps from term key to VocabTerm object
	Map<Integer,VocabTerm> terms;

	// maps from primary ID to term key
	Map<String,Integer> idToKey;
	
	// logger for this class
	public Logger logger = LoggerFactory.getLogger(this.getClass());

	// constructor: initialize this object for terms with the given 'vocabName', using the given SQLExecutor for db access
	public VocabTermCache (String vocabName, SQLExecutor ex) throws SQLException {
		logger.info("Caching data for " + vocabName);
		this.terms = new HashMap<Integer,VocabTerm>();
		this.idToKey = new HashMap<String,Integer>();
		this.fillTerms(vocabName, ex);
		this.fillIDs(vocabName, ex);
		this.fillSynonyms(vocabName, ex);
		this.fillAncestors(vocabName, ex);
		logger.info(" - Finished caching " + vocabName);
	}
	
	// retrieve the VocabTerm object with the given primary ID
	public VocabTerm getTerm(String primaryID) {
		if (this.idToKey.containsKey(primaryID)) {
			Integer termKey = this.idToKey.get(primaryID);
			if (this.terms.containsKey(termKey)) {
				return this.terms.get(termKey);
			}
		}
		return null;
	}
	
	// retrieve the VocabTerm object with the given key
	public VocabTerm getTerm(Integer termKey) {
		if (this.terms.containsKey(termKey)) {
			return this.terms.get(termKey);
		}
		return null;
	}
	
	// retrieve all the VocabTerm objects that are ancestors of the term with the given key
	public List<VocabTerm> getAncestors(Integer termKey) {
		List<VocabTerm> termList = new ArrayList<VocabTerm>();

		VocabTerm term = this.getTerm(termKey);
		if ((term != null) && (term.getAncestorKeys() != null)) {
			for (Integer ancKey : term.getAncestorKeys()) {
				if ((ancKey != null) && (this.getTerm(ancKey) != null)) {
					termList.add(this.getTerm(ancKey));
				}
			}
		}
		return termList;
	}
	
	// retrieve all the term keys for the vocab specified in the constructor
	public List<Integer> getTermKeys() {
		List<Integer> termKeys = new ArrayList<Integer>();
		for (Integer termKey : this.terms.keySet()) {
			termKeys.add(termKey);
		}
		Collections.sort(termKeys);
		return termKeys;
	}

	// fill in the basic term data for the given vocab name
	private void fillTerms (String vocabName, SQLExecutor ex) throws SQLException {
		String cmd = "select t.term_key, t.primary_id, t.term, t.definition " + 
			"from term t " + 
			"where t.display_vocab_name = '" + vocabName + "'";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		while (rs.next()) {
			VocabTerm term = new VocabTerm();
			term.setTermKey(rs.getInt("term_key"));
			term.setPrimaryID(rs.getString("primary_id"));
			term.setTerm(rs.getString("term"));
			term.setDefinition(rs.getString("definition"));
			this.terms.put(term.getTermKey(), term);
			if (term.getPrimaryID() != null) {
				this.idToKey.put(term.getPrimaryID(), term.getTermKey());
			}
		}

		rs.close();
		logger.debug(" - Got basic data for " + this.terms.size() + " terms");
	}

	// fill in the accession IDs for the given vocab name
	private void fillIDs (String vocabName, SQLExecutor ex) throws SQLException {
		String cmd = "select i.term_key, i.acc_id " + 
			"from term t, term_id i " + 
			"where t.display_vocab_name = '" + vocabName + "' " +
			"and t.term_key = i.term_key " + 
			"and i.private = 0";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			if (this.terms.containsKey(termKey)) {
				VocabTerm term = this.terms.get(termKey);
				if (term.getAllIDs() == null) {
					term.setAllIDs(new ArrayList<String>());
				}
				term.getAllIDs().add(rs.getString("acc_id"));
				ct++;
			}
		}

		rs.close();
		logger.debug(" - Got " + ct + " IDs for terms");
	}

	// fill in the synonyms for the given vocab name
	private void fillSynonyms (String vocabName, SQLExecutor ex) throws SQLException {
		String cmd = "select s.term_key, s.synonym " + 
			"from term t, term_synonym s " + 
			"where t.display_vocab_name = '" + vocabName + "' " +
			"and t.term_key = s.term_key";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			if (this.terms.containsKey(termKey)) {
				VocabTerm term = this.terms.get(termKey);
				if (term.getSynonyms() == null) {
					term.setSynonyms(new ArrayList<String>());
				}
				term.getSynonyms().add(rs.getString("synonym"));
				ct++;
			}
		}

		rs.close();
		logger.debug(" - Got " + ct + " synonyms for terms");
	}

	// fill in the ancestors for the given vocab name
	private void fillAncestors (String vocabName, SQLExecutor ex) throws SQLException {
		String cmd = "select a.term_key, a.ancestor_term_key " + 
			"from term t, term_ancestor a " + 
			"where t.display_vocab_name = '" + vocabName + "' " +
			"and t.term_key = a.term_key";
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;
		while (rs.next()) {
			Integer termKey = rs.getInt("term_key");
			if (this.terms.containsKey(termKey)) {
				VocabTerm term = this.terms.get(termKey);
				if (term.getAncestorKeys() == null) {
					term.setAncestorKeys(new ArrayList<Integer>());
				}
				term.getAncestorKeys().add(rs.getInt("ancestor_term_key"));
				ct++;
			}
		}

		rs.close();
		logger.debug(" - Got " + ct + " ancestors for terms");
	}
}
