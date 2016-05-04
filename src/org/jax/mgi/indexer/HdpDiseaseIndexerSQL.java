package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.DistinctSolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;

/* Is: an indexer that builds the index supporting the Disease tab of the 
 *		HMDC summary page.  Each document in the index represents data for
 *		a single disease, and each disease is included in a single document.
 */
public class HdpDiseaseIndexerSQL extends HdpIndexerSQL {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public HdpDiseaseIndexerSQL() {
		super("index.url.diseasePortalDisease");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* Pull the disease data from the database and add them to the index.  If a
	 * disease has no annotations, we still allow matches to it by disease name
	 * and ID.  For diseases with annotations, we add the full suite of fields
	 * for searching.
	 */
	private void processDiseases() throws Exception {
		logger.info("loading disease terms");

		// main query - OMIM disease terms that are no obsolete.  We don't bother to
		// specify an order, because we will compute ordering in-memory to ensure it
		// is smart-alpha.  (The term.sequence_num field is not.)
		String diseaseTermQuery = "select term_key, term, primary_id "
				+ "from term "
				+ "where vocab_name = '" + omim + "' "
				+ "  and is_obsolete = 0 ";

		ResultSet rs = ex.executeProto(diseaseTermQuery, cursorLimit);
		logger.debug("  - finished disease query in " + ex.getTimestamp());

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			uniqueKey += 1;			// used as a counter of diseases processed

			Integer termKey = rs.getInt("term_key");
			String term = rs.getString("term");
			String termId = rs.getString("primary_id");

			DistinctSolrInputDocument doc = new DistinctSolrInputDocument();
			doc.addField(DiseasePortalFields.UNIQUE_KEY, termKey);
			doc.addField(DiseasePortalFields.TERM, term);
			doc.addField(DiseasePortalFields.TERM_ID, termId);
			doc.addField(DiseasePortalFields.TERM_TYPE, omim);

			// add the corresponding HPO terms, their synonyms, their IDs, and the same data for their ancestors
			addHpoData(doc, termKey);

			// find sort for the term name
			int termSort = getTermSequenceNum(term);
			doc.addField(DiseasePortalFields.BY_TERM_NAME, termSort);

			// add any synonyms for this disease term and any alternate IDs
			addAllFromLookup(doc,DiseasePortalFields.TERM_SYNONYM, termId, termSynonymMap);
			addAll(doc, DiseasePortalFields.TERM_ALT_ID, getAlternateTermIds(termId));

			// add term headers for the disease
			if (headersPerTerm.containsKey(termId)) {
				addAllFromLookup(doc, DiseasePortalFields.TERM_HEADER, termId, headersPerTerm);
			} else {
				doc.addField(DiseasePortalFields.TERM_HEADER, term);
			}

			// add reference count and model count for disease
			doc.addField(DiseasePortalFields.DISEASE_REF_COUNT, getDiseaseReferenceCount(termId));
			doc.addField(DiseasePortalFields.DISEASE_MODEL_COUNTS, getDiseaseModelCount(termId));

			// add terms and IDs from related annotations (those annotations for the same genocluster
			// for mouse annotations, or for the same marker for human annotations)

			addAll(doc, DiseasePortalFields.MP_TERM_FOR_DISEASE_TEXT, getRelatedPhenotypesForTerm(termKey, true, false));
			addAll(doc, DiseasePortalFields.MP_TERM_FOR_DISEASE_ID, getRelatedPhenotypesForTerm(termKey, false, true));

			// For diseases, we only consider mouse annotations for relationships.  We don't cross
			// that bridge for human gene/disease annotations.
			addAll(doc, DiseasePortalFields.OMIM_TERM_FOR_DISEASE_TEXT, getRelatedDiseasesForTerm(termKey, true, false));
			addAll(doc, DiseasePortalFields.OMIM_TERM_FOR_DISEASE_ID, getRelatedDiseasesForTerm(termKey, false, true));

			// add marker-related fields, if any markers area associated with the disease
			Set<String> associatedMarkerKeys = this.getMarkersByDisease(termId);
			if ((associatedMarkerKeys != null) && (associatedMarkerKeys.size() > 0)) {
				// use sets to collect data prone to redundancy across markers, then add their
				// contents to the document after getting through all related markers
				Set<String> featureTypes = new HashSet<String>();
				Set<String> markerSynonyms = new HashSet<String>();
				Set<String> orthologNomen = new HashSet<String>();
				Set<String> orthologIds = new HashSet<String>();

				for (String stringMarkerKey : associatedMarkerKeys) {
					Integer markerKey = Integer.parseInt(stringMarkerKey);
					String markerSymbol = getMarkerSymbol(markerKey);
					Integer gridClusterKey = getGridClusterKey(markerKey);

					doc.addField(DiseasePortalFields.MARKER_KEY, markerKey);
					addIfNotNull(doc, DiseasePortalFields.MARKER_SYMBOL, markerSymbol);
					addIfNotNull(doc, DiseasePortalFields.MARKER_NAME, getMarkerName(markerKey));
					addIfNotNull(doc, DiseasePortalFields.MARKER_MGI_ID, getMarkerID(markerKey));

					if (markerSynonymMap.containsKey(markerKey.toString())) {
						markerSynonyms.addAll(markerSynonymMap.get(markerKey.toString()));
					}
					addAll(doc, DiseasePortalFields.MARKER_ID, getMarkerIds(markerKey));

					// add feature types for all markers in the grid cluster (if the marker is part of one)
					if (gridClusterKey != null) {
						String gckString = gridClusterKey.toString();
						doc.addField(DiseasePortalFields.GRID_CLUSTER_KEY, gckString);
						if (featureTypeMap.containsKey(gckString)) {
							featureTypes.addAll(featureTypeMap.get(gckString));
						}
					} else {
						// add feature types for markers not in grid clusters
						Set<String> mFeatureTypes = getMarkerFeatureTypes(markerKey);
						if (mFeatureTypes != null) {
							featureTypes.addAll(mFeatureTypes);
						}
					}

					if (this.isHuman(markerKey)) {
						doc.addField(DiseasePortalFields.TERM_HUMANSYMBOL, markerSymbol);
						addAll(doc, DiseasePortalFields.HUMAN_COORDINATE, getMarkerCoordinates(markerKey));
					} else {
						doc.addField(DiseasePortalFields.TERM_MOUSESYMBOL, markerSymbol);
						addAll(doc, DiseasePortalFields.MOUSE_COORDINATE, getMarkerCoordinates(markerKey));
					}

					// collect nomen and ID data for orthologs of this marker
					Set<Integer> orthologousMarkerKeys = getMarkerOrthologs(markerKey);
					if (orthologousMarkerKeys != null) {
						for (Integer orthoMarkerKey : orthologousMarkerKeys) {
							String orthoSymbol = getMarkerSymbol(orthoMarkerKey);
							String orthoName = getMarkerName(orthoMarkerKey);
							Set<String> orthoIds = getMarkerIds(orthoMarkerKey);
							Set<String> orthoSynonyms = getMarkerSynonyms(orthoMarkerKey);

							if (orthoSymbol != null) { orthologNomen.add(orthoSymbol); }
							if (orthoName != null) { orthologNomen.add(orthoName); }
							if (orthoSynonyms != null) { orthologNomen.addAll(orthoSynonyms); }
							if (orthoIds != null) { orthologIds.addAll(orthoIds); }
						}
					}
				}

				// add the data we collected in Sets to minimize duplication across markers
				if (featureTypes.size() > 0) {
					addAll(doc, DiseasePortalFields.FILTERABLE_FEATURE_TYPES, featureTypes);
				}
				if (markerSynonyms.size() > 0) {
					addAll(doc, DiseasePortalFields.MARKER_SYNONYM, markerSynonyms);
				}
				if (orthologNomen.size() > 0) {
					addAll(doc, DiseasePortalFields.ORTHOLOG_NOMEN, orthologNomen);
				}
				if (orthologIds.size() > 0) {
					addAll(doc, DiseasePortalFields.ORTHOLOG_ID, orthologIds);
				}
			} // end of processing associated markers

			// Add this doc to the batch we're collecting.  If the stack hits our
			// threshold, send it to the server and reset it.
			docs.add(doc);
			if (docs.size() >= solrBatchSize)  {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		// any leftover docs to send to the server?  (likely yes)
		writeDocs(docs);
		commit();
		rs.close();

		logger.info("done processing " + uniqueKey + " disease terms");
	}

	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// collect various mappings needed for data lookup
		getFeatureTypeMap();		// gridcluster keys to feature types
		getTermSynonymMap();		// term IDs to term synonyms
		getMarkerSynonymMap();		// marker keys to marker synonyms
		getMarkerCoordinateMap();	// coordinates per marker
		cacheHeadersPerTerm();		// disease IDs to term headers
		getMarkerAllIdMap();		// marker key to searchable marker IDs

		processDiseases();
	}
}
