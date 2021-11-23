package org.jax.mgi.shr;

/* Is: a formatter for annotations to vocabulary terms (in the marker and allele buckets of the Quick Search)
 */
public class QSTermIDFormatter extends QSAccIDFormatter {
	private String term = null;
	private boolean isSubterm = false;
	private String stage = null;
	
	protected void setTerm(String term) {
		this.term = term;
	}

	protected void setIsSubterm(boolean isSubterm) {
		this.isSubterm = isSubterm;
	}

	protected void setTheilerStage(String stage) {
		this.stage = stage;
	}

	private boolean isExpression(String accID) {
		return (accID.startsWith("EMAPA") || accID.startsWith("EMAPS"));
	}
	
	public String getMatchType() {
		if (isSubterm) {
			return objectType + " (subterm)";
		}
		return objectType;
	}

	public String getMatchDisplay() {
		String out = "";
		
		if (isExpression(accID)) {
			if (accID.startsWith("EMAPS")) {		// only include stage for EMAPS annotations
				out = out + stage + ": ";
			}
		}

		out += term;

		if (isSubterm) {
			out = out + " (ancestor term ID: " + accID + ")";
		} else {
			out = out + " (term ID: " + accID + ")";
		}
			
		return out;
	}
}
