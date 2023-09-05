package org.jax.mgi.shr;

/* Is: a formatter for references in the Other ID bucket of the Quick Search
 */
public class QSReferenceIDFormatter extends QSAccIDFormatter {
	public String getMatchType() {
		if (accID.startsWith("J:")) {
			return "MGI Reference ID";
		} else if (logicalDB.equalsIgnoreCase("Journal Link")) {
			return "doi ID";
		}
		return super.getMatchType();
	}
}
