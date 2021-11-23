package org.jax.mgi.shr;

/* Is: a formatter for homology IDs (usually non-mouse marker IDs) in the Other ID bucket of the Quick Search
 */
public class QSHomologyIDFormatter extends QSAccIDFormatter {
	private String organism = null;
	
	public void setOrganism (String organism) {
		this.organism = organism;
	}

	public String getMatchDisplay() {
		if (organism == null) {
			return accID;
		}
		return accID + " (" + organism + ")";
	}
}
