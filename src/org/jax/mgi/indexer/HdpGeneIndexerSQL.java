package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.reporting.Timer;
import org.jax.mgi.shr.DistinctSolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;

/* Is: an indexer that builds the index supporting the Gene tab of the 
 *		HMDC summary page.  Each document in the index represents data for
 *		a single gene, and each gene is included in a single document.  (We say
 *		"gene" for this index, but it includes all marker types except BAC/YAC end
 *		and DNA segment.)
 */
public class HdpGeneIndexerSQL extends HdpIndexerSQL {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private Map<Integer,Set<Integer>> markerToPheno = null;	// marker key -> set of MP term keys
	private Map<Integer,Set<Integer>> markerToDisease = null;	// marker key -> set of OMIM term keys
	private Map<Integer,Set<Integer>> genoclusterTerms = null;	// genocluster key -> set of term keys

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public HdpGeneIndexerSQL() {
		super("index.url.diseasePortalGene");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* cache the sets of phenotype and disease annotations for markers
	 */
	protected void cacheMarkerAssociations() throws Exception {
		if (markerToPheno != null) { return; }

		logger.info("Caching marker MP/OMIM associations");
		Timer.reset();
		
		markerToPheno = new HashMap<Integer,Set<Integer>>();
		markerToDisease = new HashMap<Integer,Set<Integer>>();
		
		// Normal and null qualifiers are okay; NOT qualifiers (for OMIM) are not
		String assocQuery = "select a.vocab_name, m.marker_key, a.term_key "
			+ "from marker m, marker_to_annotation t, annotation a "
			+ "where m.marker_key = t.marker_key "
			+ "  and t.annotation_key = a.annotation_key "
			+ "  and m.organism in ('human', 'mouse') "
			+ "  and m.status = 'official' "
			+ "  and (a.qualifier != 'NOT' or a.qualifier is null) "
			+ "  and m.marker_type not in ('BAC/YAC end', 'DNA Segment') "
			+ "  and a.vocab_name in ('OMIM', 'Mammalian Phenotype')";

		Map<Integer,Set<Integer>> myMap;	// current map, based on vocab name
		
		ResultSet rs = ex.executeProto(assocQuery, cursorLimit);
		while (rs.next()) {
			if (omim.equals(rs.getString("vocab_name"))) {
				myMap = markerToDisease;
			} else {
				myMap = markerToPheno;
			}
			
			Integer markerKey = rs.getInt("marker_key");
			if (!myMap.containsKey(markerKey)) {
				myMap.put(markerKey, new HashSet<Integer>());
			}
			myMap.get(markerKey).add(rs.getInt("term_key"));
		}
		rs.close();
		logger.info("Finished caching marker MP/OMIM associations " + Timer.getElapsedMessage());
	}
	
	/* return a Set of term keys for diseases associated with the marker, excluding
	 * those with NOT qualifiers.
	 */
	protected Set<Integer> getAssociatedDiseases (int markerKey) throws Exception {
		if (markerToDisease == null) { cacheMarkerAssociations(); }
		if (markerToDisease.containsKey(markerKey)) {
			return markerToDisease.get(markerKey);
		}
		return null;
	}

	/* cache the set of phenotype and disease term keys associated with each genocluster
	 */
	protected void cacheGenoclusterTerms() throws Exception {
		if (genoclusterTerms != null) { return; }
		
		logger.info("Caching genocluster MP/OMIM associations");
		Timer.reset();
		
		genoclusterTerms = new HashMap<Integer,Set<Integer>>();

		String gcQuery = "select distinct hdp_genocluster_key, term_key "
			+ "from hdp_genocluster_annotation "
			+ "where qualifier_type is null "
			+ "  and term_key is not null";
		
		ResultSet rs = ex.executeProto(gcQuery, cursorLimit);
		while (rs.next()) {
			Integer gcKey = rs.getInt("hdp_genocluster_key");
			if (!genoclusterTerms.containsKey(gcKey)) {
				genoclusterTerms.put(gcKey, new HashSet<Integer>());
			}
			genoclusterTerms.get(gcKey).add(rs.getInt("term_key"));
		}
		rs.close();
		logger.info("Finished caching genocluster MP/OMIM associations " + Timer.getElapsedMessage());
	}
	
	/* get the set of keys for disease and phenotype terms annotated to the given genocluster
	 */
	protected Set<Integer> getTermsForGenocluster (Integer genoclusterKey) throws Exception {
		if (genoclusterKey == null) { return null; }
		if (genoclusterTerms == null) { cacheGenoclusterTerms(); }
		if (genoclusterTerms.containsKey(genoclusterKey)) {
			return genoclusterTerms.get(genoclusterKey);
		}
		return null;
	}
	
	/* return a Set of term keys for phenotypes associated with the marker, excluding
	 * those with Normal qualifiers.
	 */
	protected Set<Integer> getAssociatedPhenotypes (int markerKey) throws Exception {
		if (markerToPheno == null) { cacheMarkerAssociations(); }
		if (markerToPheno.containsKey(markerKey)) {
			return markerToPheno.get(markerKey);
		}
		return null;
	}
	
	/* add fields to 'doc' that relate to the given 'termKey'
	 */
	protected void addTermFields (DistinctSolrInputDocument doc, int termKey, boolean isHumanMarker) throws Exception {
		String termId = getTermId(termKey);
		doc.addDistinctField(DiseasePortalFields.TERM, getTerm(termKey));
		addAllFromLookup(doc,DiseasePortalFields.TERM_SYNONYM, termId, termSynonymMap);

		doc.addDistinctField(DiseasePortalFields.TERM_ID, termId);
		addAll(doc, DiseasePortalFields.TERM_ALT_ID, getAlternateTermIds(termKey));
					
		doc.addAllDistinct(DiseasePortalFields.TERM_ANCESTOR_ID, getTermAncestorIDs(termKey));
		doc.addAllDistinct(DiseasePortalFields.TERM_ANCESTOR_TEXT, getTermAncestorText(termKey));
		
		/* For OMIM terms, if we have a human marker, we need to add the corresponding HPO phenotypes.
		 * For MP terms, we need to consider the DAG and add the ancestor data.
		 */
		if (omim.equals(getVocabulary(termKey))) {
			if (isHumanMarker) {
				addHpoData(doc, termKey);
			}
		} else {
			doc.addAllDistinct(DiseasePortalFields.TERM_ANCESTOR_ID, getTermAncestorIDs(termKey));
			doc.addAllDistinct(DiseasePortalFields.TERM_ANCESTOR_TEXT, getTermAncestorText(termKey));
		}
	}
	
	/* Pull the marker data from the database and add them to the index.  If a
	 * marker has no annotations, we still allow matches to it by marker nomenclature
	 * and IDs.  For markers with annotations, we add the full suite of fields
	 * for searching.
	 */
	private void processGenes() throws Exception {
		logger.info("loading markerss");

		// main query - human and mouse markers with official (not withdrawn) nomenclature, which
		// are not BAC/YAC ends and are not DNA Segments.  Order by symbol, name, and organism
		// to ensure a consistent order across runs.  We also bring in the genocluster keys 
		// associated with the markers, so this will lead to multiple rows (and documents)
		// per marker.
		String markerQuery = "select n.by_symbol, n.by_name, n.by_organism, n.by_location, "
			+ "  m.marker_key, m.symbol, m.name, m.primary_id, m.organism, m.marker_type, "
			+ "  m.location_display, m.coordinate_display, m.build_identifier, m.marker_subtype, "
			+ "  c.reference_count, c.disease_relevant_reference_count, c.imsr_count, "
			+ "  gc.hdp_genocluster_key, a.term_key "
			+ "from marker m "
			+ "inner join marker_sequence_num n on (m.marker_key = n.marker_key) "
			+ "inner join marker_counts c on (m.marker_key = c.marker_key) "
			+ "left outer join hdp_genocluster gc on (m.marker_key = gc.marker_key) "
			+ "left outer join hdp_annotation a on (m.marker_key = a.marker_key and a.organism_key = 2)"
			+ "where m.marker_type not in ('BAC/YAC end', 'DNA Segment') "
			+ "  and m.organism in ('mouse', 'human') "
			+ "  and m.status = 'official' "
			+ "order by 1, 2, 3";

		ResultSet rs = ex.executeProto(markerQuery, cursorLimit);
		logger.debug("  - finished marker query in " + ex.getTimestamp());

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			uniqueKey += 1;			// used as a counter of markers processed

			Integer markerKey = rs.getInt("marker_key");
			Integer gridClusterKey = getGridClusterKey(markerKey);
			Integer genoClusterKey = rs.getInt("hdp_genocluster_key");
			boolean isHumanMarker = isHuman(markerKey);

			DistinctSolrInputDocument doc = new DistinctSolrInputDocument();
			
			// basic fields (key, symbol, name, IDs, organism, grid cluster)
			
			doc.addField(DiseasePortalFields.UNIQUE_KEY, uniqueKey);
			doc.addField(DiseasePortalFields.MARKER_KEY, markerKey);
			addIfNotNull(doc, DiseasePortalFields.MARKER_SYMBOL, rs.getString("symbol"));
			addIfNotNull(doc, DiseasePortalFields.MARKER_NAME, rs.getString("name"));
			addIfNotNull(doc, DiseasePortalFields.MARKER_MGI_ID, rs.getString("primary_id"));
			doc.addAllDistinct(DiseasePortalFields.MARKER_ID, getMarkerIds(markerKey));
			addIfNotNull(doc, DiseasePortalFields.GRID_CLUSTER_KEY, gridClusterKey);
			addIfNotNull(doc, DiseasePortalFields.ORGANISM, rs.getString("organism"));

			// hybrid homology cluster key, if any
			doc.addField(DiseasePortalFields.HOMOLOGY_CLUSTER_KEY, getHomologyClusterKey(markerKey));

			// synonyms, if any exist
			
			if (markerSynonymMap.containsKey(markerKey.toString())) {
				doc.addAllDistinct(DiseasePortalFields.MARKER_SYNONYM, markerSynonymMap.get(markerKey.toString()));
			}

			// genetic and genomic locations 

			addIfNotNull(doc, DiseasePortalFields.LOCATION_DISPLAY, rs.getString("location_display"));
			addIfNotNull(doc, DiseasePortalFields.COORDINATE_DISPLAY, rs.getString("coordinate_display"));
			addIfNotNull(doc, DiseasePortalFields.BUILD_IDENTIFIER, rs.getString("build_identifier"));
			if (isHumanMarker) {
				doc.addAllDistinct(DiseasePortalFields.HUMAN_COORDINATE, getMarkerCoordinates(markerKey));
			} else {
				doc.addAllDistinct(DiseasePortalFields.MOUSE_COORDINATE, getMarkerCoordinates(markerKey));
			}

			// nomen and ID data for orthologs

			Set<String> orthologNomen = new HashSet<String>();
			Set<String> orthologIds = new HashSet<String>();

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

			if (orthologNomen.size() > 0) {
				addAll(doc, DiseasePortalFields.ORTHOLOG_NOMEN, orthologNomen);
			}
			if (orthologIds.size() > 0) {
				addAll(doc, DiseasePortalFields.ORTHOLOG_ID, orthologIds);
			}

			// feature types
			
			String featureType = rs.getString("marker_subtype");
			if (featureType == null) {
				featureType = rs.getString("marker_type");
			}
			addIfNotNull(doc, DiseasePortalFields.FILTERABLE_FEATURE_TYPES, featureType);
			addIfNotNull(doc, DiseasePortalFields.MARKER_FEATURE_TYPE, featureType);
			
			// pre-computed sorts
			
			addIfNotNull(doc, DiseasePortalFields.BY_MARKER_ORGANISM, rs.getString("by_organism"));
			addIfNotNull(doc, DiseasePortalFields.BY_MARKER_SYMBOL, rs.getString("by_symbol"));
			addIfNotNull(doc, DiseasePortalFields.BY_MARKER_LOCATION, rs.getString("by_location"));
			
			// pre-computed counts
			
			addIfNotNull(doc, DiseasePortalFields.MARKER_ALL_REF_COUNT, rs.getString("reference_count"));
			addIfNotNull(doc, DiseasePortalFields.MARKER_DISEASE_REF_COUNT, rs.getString("disease_relevant_reference_count"));
			addIfNotNull(doc, DiseasePortalFields.MARKER_IMSR_COUNT, rs.getString("imsr_count"));
			
			// just the diseases and phenotypes associated with this BSU (basic search unit), which is
			// a genocluster/gridcluster pair for mouse data or a marker/disease pair for human data, plus
			// the BSU key itself.  These are to allow searching multiple fields AND-ed together within
			// a genocluster, rather than across them all.

			BSU bsu = null;
			if ("mouse".equals(rs.getString("organism"))) {
				if (genoClusterKey != null && gridClusterKey != null) {
					bsu = getMouseBsu(genoClusterKey, gridClusterKey);
					Set<Integer> termKeys = getTermsForGenocluster(genoClusterKey);
					if (termKeys != null) {
						for (Integer termKey : termKeys) {
							addTermFields(doc, termKey, isHumanMarker);
						}
					}
				}
			} else if (isHumanMarker) {
				Integer termKey = rs.getInt("term_key");
				if (termKey != null) {
					bsu = getHumanBsu(markerKey, termKey);
					addTermFields(doc, termKey, isHumanMarker);
				}
			}
			
			if (bsu != null) {
				doc.addField(DiseasePortalFields.GRID_KEY, bsu.bsuKey);
			}
			
			// all diseases associated with the marker (for display)
			
			Set<Integer> diseaseKeys = getAssociatedDiseases(markerKey);

			if (diseaseKeys != null) {
				List<String> diseases = new ArrayList<String>();
				for (Integer diseaseKey : diseaseKeys) {
					diseases.add(getTerm(diseaseKey));
				}
				Collections.sort(diseases);
				doc.addAllDistinct(DiseasePortalFields.MARKER_DISEASE, diseases);
			}
			
			// all phenotypes associated with the marker (for display)

			Set<Integer> phenotypeKeys = getAssociatedPhenotypes(markerKey);
			if (phenotypeKeys != null) {
				Set<String> mpHeaders = new HashSet<String>();
				for (Integer phenotypeKey : phenotypeKeys) {
					mpHeaders.addAll(getHeadersPerTerm(phenotypeKey));
				}
				if (mpHeaders.contains("normal phenotype")) {
					mpHeaders.remove("normal phenotype");
				}
				List<String> phenotypes = Arrays.asList(mpHeaders.toArray(new String[0]));
				Collections.sort(phenotypes);

				if (isHumanMarker) {
					doc.addAllDistinct(DiseasePortalFields.HUMAN_MARKER_SYSTEM, phenotypes);
				} else {
					doc.addAllDistinct(DiseasePortalFields.MOUSE_MARKER_SYSTEM, phenotypes);
				}
			}
			
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

		logger.info("done processing " + uniqueKey + " markers");
	}

	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// collect various mappings needed for data lookup
		getTermSynonymMap();		// term IDs to term synonyms
		getMarkerSynonymMap();		// marker keys to marker synonyms
		getMarkerCoordinateMap();	// coordinates per marker
		cacheHeadersPerTerm();		// disease IDs to term headers
		getMarkerAllIdMap();		// marker key to searchable marker IDs

		processGenes();
	}
}
