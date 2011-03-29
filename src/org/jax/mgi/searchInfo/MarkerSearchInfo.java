package org.jax.mgi.searchInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * MarkerSearchInfo
 * @author mhall
 * This object represents all of the search information needed about a marker for a specific index.
 * 
 * It is expected that the index that uses this object will use all of the information that it contains.
 * 
 */

public class MarkerSearchInfo {
	
	private List<String> nomen = new ArrayList<String>();
	private String bySymbol;
	
	public void addNomen(String word) {
		nomen.add(word);
	}

	public String getBySymbol() {
		return bySymbol;
	}

	public List<String> getNomen() {
		return nomen;
	}
	
	public void setBySymbol(String bySymbol) {
		this.bySymbol = bySymbol;
	}

	public void setNomen(List<String> nomen) {
		this.nomen = nomen;
	}

	@Override
	public String toString() {
		String outString = "";
		for (String n: nomen) {
			outString += "\n" + n;
		}
		
		return outString;
	};
		
	
	
}
