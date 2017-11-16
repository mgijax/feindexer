package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * Indexer for MP/Genotype annotations, copied and modified from
 * MarkerAnnotationIndexerSQL.
 */

public class MPAnnotationIndexerSQL extends Indexer {

	private String vocabName = "Mammalian Phenotype";
	private	String annotationType = "Mammalian Phenotype/Genotype";

	public MPAnnotationIndexerSQL () {
		super("mpAnnotation");
	}

	// get highest annotation key for MP/genotype annotations
	private Integer getMaxAnnotationKey() throws SQLException {
		ResultSet rs_tmp = ex.executeProto("select max(annotation_key) as maxAnnotKey from annotation where annotation_type = '" + annotationType + "'");
		rs_tmp.next();
		return rs_tmp.getInt("maxAnnotKey");
	}
	
	public HashMap<String, HashSet<String>> getGenotypeToMarkersMap() {
		// Now for each annotation, we need to find the markers associated
		// with the alleles in the genotype (so we can match based on MP
		// term and marker when linking from the batch query).  There are
		// currently no exclusions for MP annotations as there are for
		// DO disease annotations, so that simplifies things.
		//
		// Also, in addition to the traditional marker-allele pairs, we
		// must now consider "mutation involves" and "expresses component"
		// relationships.  These are brought in by the second part of the
		// union.

		String genotypeMarkers = "select distinct ga.genotype_key, "
				+ "  ma.marker_key "
				+ "from annotation a, "
				+ "  genotype_to_annotation ga, "
				+ "  allele_to_genotype ag, "
				+ "  marker_to_allele ma "
				+ "where ga.genotype_key = ag.genotype_key "
				+ "  and a.annotation_type = '" + annotationType + "' "
				+ "  and a.annotation_key = ga.annotation_key "
				+ "  and ag.allele_key = ma.allele_key "
				+ "union "
				+ "select distinct ga.genotype_key, ma.related_marker_key "
				+ "from annotation a, "
				+ "  genotype_to_annotation ga, "
				+ "  allele_to_genotype ag, "
				+ "  allele_related_marker ma "
				+ "where ga.genotype_key = ag.genotype_key "
				+ "  and a.annotation_type = '" + annotationType + "' "
				+ "  and a.annotation_key = ga.annotation_key "
				+ "  and ag.allele_key = ma.allele_key "
				+ "  and ma.relationship_category in ('mutation_involves', 'expresses_component')"; 

		HashMap<String, HashSet<String>> genotypeToMarkers = makeHash(genotypeMarkers, "genotype_key", "marker_key");
		logger.info("Found markers for " + genotypeToMarkers.size() + " genotypes");
		return genotypeToMarkers;
	}
	
	public HashMap<String, HashSet<String>> getAnnotationToRefsMap() {
		// And each annotation will have one or more references as
		// supporting evidence.  We need the J: numbers for each of them.

		String references = "select distinct a.annotation_key, "
				+ "  r.jnum_id, r.sequence_num "
				+ "from annotation a, "
				+ "  annotation_reference r "
				+ "where a.annotation_key = r.annotation_key "
				+ "  and a.annotation_type = '" + annotationType + "' "
				+ "order by 1, 3";

		HashMap<String, HashSet<String>> annotationToRefs = makeHash(references, "annotation_key", "jnum_id");
		logger.info("Found references for " + annotationToRefs.size() + " annotations");
		return annotationToRefs; 
	}

	// dataType should be either 'background_strain' or 'combination_1', depending on what you want
	public HashMap<String, HashSet<String>> getGenotypeInfoMap(String dataType) {
		String genotypeQuery = "select distinct g.genotype_key, "
				+ "  g." + dataType + " "
				+ "from annotation a, "
				+ "  genotype_to_annotation ga, "
				+ "  genotype g "
				+ "where ga.genotype_key = g.genotype_key"
				+ "  and ga.annotation_key = a.annotation_key"
				+ "  and a.annotation_type = '" + annotationType + "' ";

		HashMap<String, HashSet<String>> genotypeToData = makeHash(genotypeQuery, "genotype_key", dataType);
		logger.info("Found " + dataType + " for " + genotypeToData.size() + " genotypes");
		return genotypeToData;
	}

	// dataType should either be 'term' or 'primary_id', depending on what you want
	public HashMap<String, HashSet<String>> getTermDataMap (String dataType) {
		String cmd = "select term_key, " + dataType + " "
			+ "from term "
			+ "where vocab_name = '" + vocabName + "'";

		HashMap<String, HashSet<String>> termKeyToData = makeHash(cmd, "term_key", dataType);
		logger.info("Found " + dataType + " for " + termKeyToData.size() + " terms");
		return termKeyToData;
	}

	public HashMap<String, HashSet<String>> getAnnotationToSeqNumMap() {
		String seqnumQuery = "select a.annotation_key, "
				+ "  s.by_object_dag_term "
				+ "from annotation a, "
				+ "  annotation_sequence_num s "
				+ "where a.annotation_key = s.annotation_key "
				+ "  and a.annotation_type = '" + annotationType + "' ";

		HashMap<String, HashSet<String>> annotationToSeqNum = makeHash(seqnumQuery, "annotation_key", "by_object_dag_term");
		logger.info("Found sequence numbers for " + annotationToSeqNum.size() + " annotations");
		return annotationToSeqNum;
	}

	public HashMap<String, HashSet<String>> getTermAncestorMap() {
		String ancestorQuery = "select a.term_key, a.ancestor_term_key "
			+ "from term_ancestor a, term t " 
			+ "where a.term_key = t.term_key " 
			+ "and t.vocab_name = '" + vocabName + "'";
		
		HashMap<String, HashSet<String>> ancestorMap = makeHash(ancestorQuery, "term_key", "ancestor_term_key");
		logger.info("Found ancestors for " + ancestorMap.size() + " terms");
		return ancestorMap;
	}

	// returns a single value for the given 'key' from 'map' and throws an Exception if more than one value exists
	// or if if 'key' does not exist in 'map'.
	public String getOne (HashMap<String, HashSet<String>> map, String key) throws Exception {
		if (!map.containsKey(key)) {
			throw new Exception(key + " is not a key of map");
		}
		if (map.get(key).size() > 1) {
			throw new Exception(map.get(key).size() + " values for key " + key + " in map");
		}
		for (String s : map.get(key)) {
			return s;
		}
		throw new Exception(key + " has an empty set as a value");
	}
	
	public void index() throws Exception
	{
		HashMap<String, HashSet<String>> genotypeToMarkers = getGenotypeToMarkersMap();
		HashMap<String, HashSet<String>> annotationToRefs = getAnnotationToRefsMap();
		HashMap<String, HashSet<String>> annotationToSeqNum = getAnnotationToSeqNumMap();
		HashMap<String, HashSet<String>> genotypeToStrain = getGenotypeInfoMap("background_strain");
		HashMap<String, HashSet<String>> genotypeToAlleles = getGenotypeInfoMap("combination_1");
		HashMap<String, HashSet<String>> termKeyToTerm = getTermDataMap("term");
		HashMap<String, HashSet<String>> termKeyToID = getTermDataMap("primary_id");
		HashMap<String, HashSet<String>> ancestors = getTermAncestorMap();
		
		// Our main relationship is between annotation keys and MP term
		// IDs, while also including the ordering by genotype and by
		// MP term.  In order to drive the display from the Solr index
		// without hitting the database, we also need to include the
		// genotype's allele pairs and strain backgrounds, the annotated
		// term and its ID (not just the ID used for searching).  We will
		// look up the markers for each annotated genotype from the
		// HashMap composed above (genotypeToMarkers) when we get to
		// building Solr documents.  And, we will include references for
		// each annotation from annotationToRefs.

		Integer start = 0;
		Integer end = getMaxAnnotationKey();
		int chunkSize = 10000;

		int modValue = end.intValue() / chunkSize;

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		int uniqueKey = 0;

		for (int i = 0; i <= modValue; i++) 
		{    
			start = i * chunkSize;
			end = start + chunkSize;

			logger.info ("Processing annotation key > " + start + " and <= " + end);
			String mainQuery = "select a.annotation_key, a.term_key, g.genotype_key " 
				+ "from annotation a, genotype_to_annotation t, genotype g " 
				+ "where a.annotation_type = '" + annotationType + "' "
				+ "and a.annotation_key > " + start + " and a.annotation_key <= " + end + " "
				+ "and a.annotation_key = t.annotation_key " 
				+ "and t.genotype_key = g.genotype_key " 
				+ "and g.combination_1 is not null";

			ResultSet rs_overall = ex.executeProto(mainQuery);

			boolean hasRecord = rs_overall.next();

			// Finally we need to bundle the data into Solr documents and push
			// them over to the Solr server for indexing.

			logger.info("Parsing annotation/genotype pairs");

			while (hasRecord) 
			{
				uniqueKey++;

				String annotKey = rs_overall.getString("annotation_key");
				String genotypeKey = rs_overall.getString("genotype_key");
				String termKey = rs_overall.getString("term_key");

				SolrInputDocument doc = new SolrInputDocument();

				doc.addField(IndexConstants.UNIQUE_KEY, annotKey);
				doc.addField(IndexConstants.ANNOTATION_KEY, annotKey);
				doc.addField(IndexConstants.GENOTYPE_KEY, genotypeKey);
				
				// make record searchable by this term ID and the IDs of all its ancestors

				if (termKeyToID.containsKey(termKey)) {
					String id = getOne(termKeyToID, termKey);
					doc.addField(IndexConstants.TERM_ID, id);
					doc.addField(IndexConstants.ANNOTATED_TERM_ID, id);
				}

				if (ancestors.containsKey(termKey)) {
					for (String ancestorKey : ancestors.get(termKey)) {
						doc.addField(IndexConstants.TERM_ID, getOne(termKeyToID, ancestorKey));
					}
				}

				// annotated term for display
				if (termKeyToTerm.containsKey(termKey)) {
					doc.addField(IndexConstants.TERM, getOne(termKeyToTerm, termKey));
				}

				// background strain for genotype
				if (genotypeToStrain.containsKey(genotypeKey)) {
					doc.addField(IndexConstants.BACKGROUND_STRAIN, getOne(genotypeToStrain, genotypeKey));
				}

				// allele pairs for genotype
				if (genotypeToAlleles.containsKey(genotypeKey)) {
					doc.addField(IndexConstants.ALLELE_PAIRS, getOne(genotypeToAlleles, genotypeKey));
				}

				// sequence number
				if (annotationToSeqNum.containsKey(annotKey)) {
					doc.addField(IndexConstants.BY_GENOTYPE_TERM, getOne(annotationToSeqNum, annotKey));
				}

				// include markers for each annotation's genotype

				if (genotypeToMarkers.containsKey(genotypeKey)) {
					for (String markerKey: genotypeToMarkers.get(genotypeKey)) {
						doc.addField(IndexConstants.MRK_KEY, markerKey);
					}
				}

				// include references for each annotation

				if (annotationToRefs.containsKey(annotKey)) {
					for (String jnumID: annotationToRefs.get(annotKey)) {
						doc.addField(IndexConstants.JNUM_ID, jnumID);
					}
				}

				hasRecord = rs_overall.next();

				docs.add(doc);

				if (docs.size() > 1000) {
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>();
				}
			}
		}

		if (docs.size() > 0) {
			writeDocs(docs);
			commit();
		}
		logger.info("Done adding " + uniqueKey + " docs to solr; completed MP/Annotation index.");
	}
}
