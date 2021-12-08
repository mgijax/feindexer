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
		return tweakedLogicalDB(logicalDB) + " ID";
	}

	public String getMatchDisplay() {
		return accID;
	}
	
	// There are certain logical databases that require a slightly tweaked display value.  Do the
	// conversion here as needed.
	public String tweakedLogicalDB(String s) {
		if ("Lexicon Genetics".equalsIgnoreCase(logicalDB)) {
			return "Lexicon";
		} else if ("CSD-KOMP".equalsIgnoreCase(logicalDB)) {
			return "KOMP-CSD";
		} else if ("TrEMBL".equalsIgnoreCase(logicalDB) || "SWISS-PROT".equalsIgnoreCase(logicalDB)) {
			return "UniProt, EBI";
		} else if ("Sequence DB".equalsIgnoreCase(logicalDB)) {
			return "Genbank, ENA, DDBJ";
		}
		return s;
	}
}
