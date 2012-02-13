package org.jax.mgi.searchInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * MarkerSearchInfo
 * This object represents all of the search information needed about a marker for a specific index.
 * 
 * It is expected that the index that uses this object will use all of the information that it contains.
 * 
 */

public class MarkerSearchInfo {
	
	private String bySymbol;
	private String symbol;
	private String name;
	private List<String> synonyms = new ArrayList<String>();
	
	/* setters */

	public void addSynonym (String synonym) {
		if (synonym != null) {
			this.synonyms.add (synonym);
		}
	}

	public void setBySymbol(String bySymbol) {
		this.bySymbol = bySymbol;
	}

	public void setSymbol (String symbol) {
		this.symbol = symbol;
	}

	public void setName (String name) {
		this.name = name;
	}

	/* getters */

	public String getSymbol() {
		return this.symbol;
	}

	public String getName() {
		return this.name;
	}

	public List<String> getSynonyms() {
		return this.synonyms;
	}

	public String getBySymbol() {
		return bySymbol;
	}

	/* accessories */

	public List<String> getNomen() {
		List<String> allNomen = new ArrayList();

		if (this.symbol != null) { allNomen.add (symbol); }
		if (this.name != null) { allNomen.add (name); }
		allNomen.addAll (this.synonyms);

		return allNomen;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		if (this.symbol != null) { sb.append ("Symbol: " +
			this.symbol + "\n");
		}
		if (this.name != null) { sb.append ("Name: " +
			this.name + "\n");
		}
		if (this.synonyms.size() > 0) {
			sb.append ("Synonyms: ");

			for (String s : synonyms) {
				sb.append (s);
				sb.append ("; ");
			}

			sb.append ("\n");
		}
		
		return sb.toString();
	}
	
}
