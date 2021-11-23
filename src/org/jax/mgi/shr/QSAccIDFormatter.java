package org.jax.mgi.shr;

/* Is: a formatter to help standardize the display of ID-related "Best Match" value in the Quick Search,
 *    across all five buckets.
 */
public class QSAccIDFormatter {
	protected String objectType = null;
	protected String logicalDB = null;
	protected String accID = null;
	
	// use protected constructor to prevent direct instantiation by indexers
	protected QSAccIDFormatter() {};

	protected void initialize(String objectType, String logicalDB, String accID) {
		this.objectType = objectType;
		this.logicalDB = logicalDB;
		this.accID = accID;
	}
	
	public String getMatchType() {
		if (accID.startsWith(logicalDB)) {
			return objectType + " ID";
		}
		return logicalDB + " ID";
	}

	public String getMatchDisplay() {
		return accID;
	}
}
