package org.jax.mgi.shr;

import java.util.List;

// Is: a single vocab term with its IDs, term, synonyms, definition, and ancestor keys (to aid searching)
public class VocabTerm {
	private Integer termKey;
	private String primaryID;
	private String term;
	private String definition;
	private List<String> allIDs;
	private List<String> synonyms;
	private List<Integer> ancestorKeys;

	public Integer getTermKey() {
		return termKey;
	}
	public void setTermKey(Integer termKey) {
		this.termKey = termKey;
	}
	public String getPrimaryID() {
		return primaryID;
	}
	public void setPrimaryID(String primaryID) {
		this.primaryID = primaryID;
	}
	public String getTerm() {
		return term;
	}
	public void setTerm(String term) {
		this.term = term;
	}
	public String getDefinition() {
		return definition;
	}
	public void setDefinition(String definition) {
		this.definition = definition;
	}
	public List<String> getAllIDs() {
		return allIDs;
	}
	public void setAllIDs(List<String> allIDs) {
		this.allIDs = allIDs;
	}
	public List<String> getSynonyms() {
		return synonyms;
	}
	public void setSynonyms(List<String> synonyms) {
		this.synonyms = synonyms;
	}
	public List<Integer> getAncestorKeys() {
		return ancestorKeys;
	}
	public void setAncestorKeys(List<Integer> ancestorKeys) {
		this.ancestorKeys = ancestorKeys;
	}
}
