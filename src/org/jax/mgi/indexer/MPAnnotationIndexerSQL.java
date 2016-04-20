package org.jax.mgi.indexer;

import java.sql.ResultSet;
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

	public MPAnnotationIndexerSQL () {
		super("index.url.mpAnnotation");
	}

	public void index() throws Exception
	{
		String annotationType = "Mammalian Phenotype/Genotype";

		// get highest annotation key for genotype annotations

		ResultSet rs_tmp = ex.executeProto("select max(annotation_key) as maxAnnotKey from annotation where annotation_type = '" + annotationType + "'");
		rs_tmp.next();
		Integer maxAnnotKey = rs_tmp.getInt("maxAnnotKey");

		String tmpAnnotations = "select a.annotation_key, "
				+ "  ga.genotype_key, "
				+ "  a.term_id as search_term_id, "
				+ "  a.term_id, "
				+ "  a.qualifier "
				+ "into temp tmp_mp_annotations "
				+ "from annotation a, "
				+ "  genotype_to_annotation ga, "
				+ "  genotype g "
				+ "where a.annotation_type = '" + annotationType + "' "
				+ "  and ga.genotype_key = g.genotype_key "
				+ "  and g.combination_1 is not null "
				+ "  and a.annotation_key = ga.annotation_key "
				+ "union "
				+ "select distinct a.annotation_key, "
				+ "  ga.genotype_key, "
				+ "  ta.ancestor_primary_id as search_term_id, "
				+ "  a.term_id, "
				+ "  a.qualifier "
				+ "from term t, term_ancestor ta, annotation a, "
				+ "  genotype_to_annotation ga, genotype g "
				+ "where a.annotation_type = '" + annotationType + "' "
				+ "  and a.annotation_key = ga.annotation_key "
				+ "  and ga.genotype_key = g.genotype_key "
				+ "  and g.combination_1 is not null "
				+ "  and a.term_id = t.primary_id "
				+ "  and t.term_key = ta.term_key ";
		logger.info("creating temp table tmp_mp_annotations");
		ex.executeVoid(tmpAnnotations);
		logger.info("done creating temp table tmp_mp_annotations");

		// Now for each annotation, we need to find the markers associated
		// with the alleles in the genotype (so we can match based on MP
		// term and marker when linking from the batch query).  There are
		// currently no exclusions for MP annotations as there are for
		// OMIM disease annotations, so that simplifies things.
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
				+ "  and ma.relationship_category in ('mutation_involves', "
				+ "    'expresses_component')"; 

		HashMap<String, HashSet<String>> genotypeToMarkers = makeHash(
				genotypeMarkers, "genotype_key", "marker_key");
		logger.info("Found markers for " + genotypeToMarkers.size()
				+ " genotypes");

		// And each annotation will have one or more references as
		// supporting evidence.  We need the J: numbers for each of them.

		String references = "select distinct a.annotation_key, "
				+ "  r.jnum_id, r.sequence_num "
				+ "from annotation a, "
				+ "  annotation_reference r "
				+ "where a.annotation_key = r.annotation_key "
				+ "  and a.annotation_type = '" + annotationType + "' "
				+ "order by 1, 3";

		HashMap<String, HashSet<String>> annotationToRefs = makeHash(
				references, "annotation_key", "jnum_id");
		logger.info("Found references for " + annotationToRefs.size()
				+ " annotations");

		// These should be able to be gathered in the mainQuery, but it
		// hangs inexplicably when run in parallel with other gatherers.
		// So, we're trying to work around that here by pulling out
		// pieces of the query.

		String termQuery = "select annotation_key, term, term_id "
				+ "from annotation "
				+ "where annotation_type = '" + annotationType + "' ";

		HashMap<String, HashSet<String>> annotationToTerm = makeHash(
				termQuery, "annotation_key", "term");
		logger.info("Found terms for " + annotationToTerm.size()
				+ " annotations");

		HashMap<String, HashSet<String>> annotationToTermID = makeHash(
				termQuery, "annotation_key", "term_id");
		logger.info("Found term IDs for " + annotationToTermID.size()
				+ " annotations");

		String genotypeQuery = "select a.annotation_key, "
				+ "  g.background_strain, "
				+ "  g.combination_1 "
				+ "from annotation a, "
				+ "  genotype_to_annotation ga, "
				+ "  genotype g "
				+ "where ga.genotype_key = g.genotype_key"
				+ "  and ga.annotation_key = a.annotation_key"
				+ "  and a.annotation_type = '" + annotationType + "' ";

		HashMap<String, HashSet<String>> annotationToStrain = makeHash(
				genotypeQuery, "annotation_key", "background_strain");
		logger.info("Found strains for " + annotationToStrain.size()
				+ " annotations");

		HashMap<String, HashSet<String>> annotationToAlleles = makeHash(
				genotypeQuery, "annotation_key", "combination_1");
		logger.info("Found alleles for " + annotationToAlleles.size()
				+ " annotations");

		String seqnumQuery = "select a.annotation_key, "
				+ "  s.by_object_dag_term "
				+ "from annotation a, "
				+ "  annotation_sequence_num s "
				+ "where a.annotation_key = s.annotation_key "
				+ "  and a.annotation_type = '" + annotationType + "' ";

		HashMap<String, HashSet<String>> annotationToSeqNum = makeHash(
				seqnumQuery, "annotation_key", "by_object_dag_term");
		logger.info("Found sequence numbers for "
				+ annotationToSeqNum.size() + " annotations");

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
		Integer end = maxAnnotKey;
		int chunkSize = 100000;

		int modValue = end.intValue() / chunkSize;

		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		int uniqueKey = 0;

		for (int i = 0; i <= modValue; i++) 
		{    
			start = i * chunkSize;
			end = start + chunkSize;

			logger.info ("Processing annotation key > " + start + " and <= " + end);
			String mainQuery = "select t.annotation_key, t.search_term_id, t.genotype_key "
					+ "from tmp_mp_annotations t "
					+ "where t.annotation_key > "+start+" and t.annotation_key <= "+end;

			ResultSet rs_overall = ex.executeProto(mainQuery);
			//logger.info("Found annotation/genotype pairs");

			boolean hasRecord = rs_overall.next();

			// Finally we need to bundle the data into Solr documents and push
			// them over to the Solr server for indexing.


			logger.info("Parsing annotation/genotype pairs");

			while (hasRecord) 
			{
				uniqueKey++;

				String annotKey = rs_overall.getString("annotation_key");
				String genotype = rs_overall.getString("genotype_key");
				String searchTermID = rs_overall.getString("search_term_id");

				SolrInputDocument doc = new SolrInputDocument();

				doc.addField(IndexConstants.UNIQUE_KEY,
						Integer.toString(uniqueKey));
				doc.addField(IndexConstants.ANNOTATION_KEY, annotKey);
				doc.addField(IndexConstants.GENOTYPE_KEY, genotype);
				doc.addField(IndexConstants.TERM_ID, searchTermID);

				// add in pieces pulled out of mainQuery

				if (annotationToTerm.containsKey(annotKey)) {
					for (String term: annotationToTerm.get(annotKey)) {
						doc.addField(IndexConstants.TERM, term);
					}
				}

				if (annotationToTermID.containsKey(annotKey)) {
					for (String termID: annotationToTermID.get(annotKey)) {
						doc.addField(IndexConstants.ANNOTATED_TERM_ID, termID);
					}
				}

				if (annotationToStrain.containsKey(annotKey)) {
					for (String strain: annotationToStrain.get(annotKey)) {
						doc.addField(IndexConstants.BACKGROUND_STRAIN, strain);
					}
				}

				if (annotationToAlleles.containsKey(annotKey)) {
					for (String alleles: annotationToAlleles.get(annotKey)) {
						doc.addField(IndexConstants.ALLELE_PAIRS, alleles);
					}
				}

				if (annotationToSeqNum.containsKey(annotKey)) {
					for (String seqNum: annotationToSeqNum.get(annotKey)) {
						doc.addField(IndexConstants.BY_GENOTYPE_TERM, seqNum);
					}
				}

				// include markers for each annotation's genotype

				if (genotypeToMarkers.containsKey(genotype)) {
					for (String markerKey: genotypeToMarkers.get(genotype)) {
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

				if (docs.size() > 10000) {
					writeDocs(docs);
					// skip logging info, as once we hit 16kb of log, process hangs when run via
					// a Python wrapper:
					//                    logger.info("Committed " + docs.size() + " docs to solr");
					docs = new ArrayList<SolrInputDocument>();
				}
			}
		}

		if (docs.size() > 0) {
			writeDocs(docs);
			commit();
			// skip logging info, as once we hit 16kb of log, process hangs when run via
			// a Python wrapper:
			//                logger.info("Committed " + docs.size() + " docs to solr");
		}
		logger.info("Done adding " + uniqueKey + " docs to solr; completed MP/Annotation index.");
	}
}
