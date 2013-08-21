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
            
            System.out.println("Max Genotype annotation_key: " + rs_tmp.getString("maxAnnotKey") + " Timing: "+ ex.getTiming());
            String start = "0";
            String end = rs_tmp.getString("maxAnnotKey");
                           
	    // Create temp table for genotype/annotation/term relationships.
	    // We will use this temp table to store both direct annotations
	    // to the term plus annotations to descendents of the term.

	    String tempTable = "mp_annotations";

	    String createTemp = "create temp table " + tempTable + " ("
		+ " annotation_key	int		not null, "
		+ " genotype_key	int		not null, "
		+ " search_term_id	varchar(40)	null, "
		+ " annotated_term_id	varchar(40)	null, "
		+ " qualifier		varchar(80)	null)";

	    logger.info(createTemp);
	    ex.executeUpdate (createTemp);
	    logger.info("Created temp table " + tempTable
		+ ", Timing: " + ex.getTiming());

	    // Add annotations for their directly annotated terms.

	    String addDirect = "insert into " + tempTable + " "
		+ "select a.annotation_key, "
		+ "  ga.genotype_key, "
		+ "  a.term_id, "
		+ "  a.term_id, "
		+ "  a.qualifier "
		+ "from annotation a, "
		+ "  genotype_to_annotation ga, "
		+ "  genotype g "
		+ "where a.annotation_type = '" + annotationType + "' "
		+ "  and ga.genotype_key = g.genotype_key "
		+ "  and g.combination_1 is not null "
		+ "  and a.annotation_key = ga.annotation_key ";

	    logger.info(addDirect);
	    ex.executeUpdate (addDirect);
	    logger.info("Added direct annotations to " + tempTable
		+ ", Timing: " + ex.getTiming());

	    // Go up the DAG, adding each annotation for each of its ancestors
	    // (so a search for an ancestor term ID also returns its
	    // descendents).

	    String addAncestors = "insert into " + tempTable + " "
		+ "select distinct a.annotation_key, "
		+ "  ga.genotype_key, "
		+ "  ta.ancestor_primary_id, "
		+ "  a.term_id, "
		+ "  a.qualifier "
		+ "from term t, term_ancestor ta, annotation a, "
		+ "  genotype_to_annotation ga, genotype g "
		+ "where a.annotation_type = '" + annotationType + "' "
		+ "  and a.annotation_key = ga.annotation_key "
		+ "  and ga.genotype_key = g.genotype_key "
		+ "  and g.combination_1 is not null "
		+ "  and a.term_id = t.primary_id "
		+ "  and t.term_key = ta.term_key";

	    logger.info(addAncestors);
	    ex.executeUpdate (addAncestors);
	    logger.info("Added annotations to ancestor terms to " + tempTable
		+ ", Timing: " + ex.getTiming());

	    // Now for each annotation, we need to find the markers associated
	    // with the alleles in the genotype (so we can match based on MP
	    // term and marker when linking from the batch query).  There are
	    // currently no exclusions for MP annotations as there are for
	    // OMIM disease annotations, so that simplifies things.

	    String genotypeMarkers = "select distinct t.genotype_key, "
		+ "  ma.marker_key "
		+ "from " + tempTable + " t, "
		+ "  allele_to_genotype ag, "
		+ "  marker_to_allele ma "
		+ "where t.genotype_key = ag.genotype_key "
		+ "  and ag.allele_key = ma.allele_key";

	    logger.info(genotypeMarkers);
	    HashMap<String, HashSet<String>> genotypeToMarkers = makeHash(
		genotypeMarkers, "genotype_key", "marker_key");
	    logger.info("Found markers for " + genotypeToMarkers.size()
		+ " genotypes");

	    // And each annotation will have one or more references as
	    // supporting evidence.  We need the J: numbers for each of them.

	    String references = "select distinct t.annotation_key, "
		+ "  r.jnum_id, r.sequence_num "
		+ "from " + tempTable + " t, "
		+ "  annotation_reference r "
		+ "where t.annotation_key = r.annotation_key "
		+ "order by 1, 3";

	    logger.info(references);
	    HashMap<String, HashSet<String>> annotationToRefs = makeHash(
		references, "annotation_key", "jnum_id");
	    logger.info("Found references for " + annotationToRefs.size()
		+ " annotations");

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
	    
	    String mainQuery = "select t.annotation_key, "
		+ "  t.search_term_id, "
		+ "  t.genotype_key, "
		+ "  s.by_object_dag_term, "
		+ "  a.term_id, "
		+ "  a.term, "
		+ "  g.background_strain, "
		+ "  g.combination_1 "
		+ "from " + tempTable + " t, "
		+ "  annotation a, "
		+ "  annotation_sequence_num s, "
		+ "  genotype g "
		+ "where t.annotation_key = a.annotation_key "
		+ "  and t.genotype_key = g.genotype_key "
		+ "  and a.annotation_key = s.annotation_key";

	    logger.info(mainQuery);
	    ResultSet rs_overall = ex.executeProto(mainQuery);
	    logger.info("Found annotation/genotype pairs");

            rs_overall.next();

	    // Finally we need to bundle the data into Solr documents and push
	    // them over to the Solr server for indexing.

            Collection<SolrInputDocument> docs =
		new ArrayList<SolrInputDocument>();
            
	    String annotKey;			// current annotation key
	    String genotype;			// current genotype key
	    int uniqueKey = 0;

	    logger.info("Parsing annotation/genotype pairs");

            while (!rs_overall.isAfterLast()) {
		uniqueKey++;
		annotKey = rs_overall.getString("annotation_key");

                SolrInputDocument doc = new SolrInputDocument();

                doc.addField(IndexConstants.ANNOTATION_KEY, annotKey);
                doc.addField(IndexConstants.TERM_ID,
			rs_overall.getString("search_term_id"));
                doc.addField(IndexConstants.BY_GENOTYPE_TERM,
			rs_overall.getString("by_object_dag_term"));
		doc.addField(IndexConstants.UNIQUE_KEY,
			Integer.toString(uniqueKey));
		doc.addField(IndexConstants.GENOTYPE_KEY,
			rs_overall.getString("genotype_key"));
		doc.addField(IndexConstants.ALLELE_PAIRS,
			rs_overall.getString("combination_1"));
		doc.addField(IndexConstants.BACKGROUND_STRAIN,
			rs_overall.getString("background_strain"));
                doc.addField(IndexConstants.TERM,
			rs_overall.getString("term"));
                doc.addField(IndexConstants.ANNOTATED_TERM_ID,
			rs_overall.getString("term_id"));

		// include markers for each annotation's genotype

		genotype = rs_overall.getString("genotype_key");

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

                rs_overall.next();
                                
                docs.add(doc);
                
                if (docs.size() > 10000) {
                    logger.info("Adding a stack of the documents to Solr");
                    server.add(docs);
                    server.commit();
                    docs = new ArrayList<SolrInputDocument>();
                    logger.info("Done adding to solr, Moving on");
                }
            }
            
            server.add(docs);
            server.commit();
            logger.info("Done adding to solr; completed MP/Annotation index.");
    }
}
