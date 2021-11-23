package org.jax.mgi.shr;

// returns a reused (sometimes type-specific) QSAccIDFormatter object (finish with the
// one you request before requesting another one, because they are reused to save
// instantiation costs).
public class QSAccIDFormatterFactory {
	// one formatter for each type of object with special handling
	QSAccIDFormatter generalFormatter = new QSAccIDFormatter();			// basic formatter for general cases
	QSReferenceIDFormatter referenceFormatter = new QSReferenceIDFormatter();
	QSTermIDFormatter termFormatter = new QSTermIDFormatter();
	QSHomologyIDFormatter homologyFormatter = new QSHomologyIDFormatter();
	
	// parameterless default constructor (overkill, but prefer to specify for clarity)
	public QSAccIDFormatterFactory() {};
	
	// general builder
	public QSAccIDFormatter getFormatter (String objectType, String logicalDB, String accID) {
		// Special handling needed for references with MGI J# or DOI IDs (recognized by logical db)
		if (accID.startsWith("J:") || logicalDB.equalsIgnoreCase("Journal Link")) {
			referenceFormatter.initialize(objectType, logicalDB, accID);
			return referenceFormatter;
		}	
		return getFormatter(objectType, logicalDB, accID, null, null, null);
	}

	// homology ID builder
	public QSAccIDFormatter getFormatter (String objectType, String logicalDB, String accID, String organism) {
		homologyFormatter.initialize(objectType, logicalDB, accID);
		homologyFormatter.setOrganism(organism);
		return homologyFormatter;
	}

	// builder for annotations using non-EMAPA/EMAPS vocab terms
	public QSAccIDFormatter getFormatter (String objectType, String logicalDB, String accID, String term, Boolean isSubterm) {
		return getFormatter(objectType, logicalDB, accID, term, isSubterm, null);
	}
	
	// builder for annotations using EMAPA/EMAPS terms (with general logic for others too)
	public QSAccIDFormatter getFormatter (String objectType, String logicalDB, String accID, String term, Boolean isSubterm, String theilerStage) {
		if ((term != null) || (isSubterm != null) || (theilerStage != null)) {
			// Recognized terms based on term-specific fields being non-null.
			termFormatter.initialize(objectType, logicalDB, accID);
			termFormatter.setIsSubterm(isSubterm);
			termFormatter.setTheilerStage(theilerStage);
			return termFormatter;
		}

		generalFormatter.initialize(objectType, logicalDB, accID);
		return generalFormatter;
	}
}
