package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/* Is: an indexer that builds the index supporting the quick search's feature (marker + allele) bucket (aka- bucket 1).
 * 		Each document in the index represents data for a single marker or allele.
 */
public class QSFeatureBucketIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- class variables ---*/
	/*--------------------------*/

	private static String MARKER = "marker";	// name of InterPro Domains vocabulary
	private static String ALLELE = "allele";	// name of InterPro Domains vocabulary

	// what to add between base fewi URL and marker or allele ID, to link directly to a detail page
	private static Map<String,String> uriPrefixes;			
	static {
		uriPrefixes = new HashMap<String,String>();
		uriPrefixes.put(MARKER, "/marker/");
		uriPrefixes.put(ALLELE, "/allele/");
	}

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private Map<Integer,List<String>> allIDs;		// marker or allele key : list of all IDs
	private Map<Integer,List<String>> synonyms;		// marker or allele key : list of synonyms

	private Map<Integer,String> chromosome;			// marker or allele key : chromosome
	private Map<Integer,String> startCoord;			// marker or allele key : start coordinate
	private Map<Integer,String> endCoord;			// marker or allele key : end coordinate
	private Map<Integer,String> strand;				// marker or allele key : strand
	
	// TODO : ORTHOLOG NOMENCLATURE
	
	private Map<Integer,Set<String>> goProcessAnnotations;		// marker or allele key : annotated GO Process term, ID, synonym
	private Map<Integer,Set<String>> goFunctionAnnotations;		// marker or allele key : annotated GO Function term, ID, synonym
	private Map<Integer,Set<String>> goComponentAnnotations;	// marker or allele key : annotated GO Component term, ID, synonym
	private Map<Integer,Set<String>> mpAnnotations;				// marker or allele key : annotated MP term, ID, synonym
	private Map<Integer,Set<String>> hpoAnnotations;			// marker or allele key : annotated HPO term, ID, synonym
	private Map<Integer,Set<String>> diseaseAnnotations;		// marker or allele key : annotated DO term, ID, synonym
	private Map<Integer,Set<String>> proteinDomains;			// marker or allele key : annotated protein domains, ID, synonym
	private Map<Integer,Set<String>> gxdAnnotations;			// marker or allele key : annotated EMAPA structure, ID, synonym
	private Map<Integer,Set<String>> gxdAnnotationsWithTS;		// marker or allele key : annotated EMAPA structure, ID, synonym, with TS prepended

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public QSFeatureBucketIndexerSQL() {
		super("qsFeatureBucket");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* Cache accession IDs for the given feature type (marker or allele), populating the allIDs object.
	 */
	private void cacheIDs(String featureType) throws Exception {
		logger.info(" - caching IDs for " + featureType);

		allIDs = new HashMap<Integer,List<String>>();
		String cmd;
		
		if (MARKER.equals(featureType)) {
			cmd = "select i.marker_key as feature_key, i.acc_id " + 
				"from marker m, marker_id i " + 
				"where m.marker_key = i.marker_key " + 
				"and m.status = 'official' " + 
				"and m.organism = 'mouse' " + 
				"and i.private = 0";
		} else {
			cmd = "select i.allele_key as feature_key, i.acc_id " + 
					"from allele_id i " + 
					"where i.private = 0";
		}

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of IDs processed
		while (rs.next()) {
			ct++;
			Integer featureKey = rs.getInt("feature_key");
			String id = rs.getString("acc_id");
			
			if (!allIDs.containsKey(featureKey)) {
				allIDs.put(featureKey, new ArrayList<String>());
			}
			allIDs.get(featureKey).add(id);
		}
		rs.close();
		
		logger.info(" - cached " + ct + " IDs for " + allIDs.size() + " terms");
	}
	
	/* Cache all synonyms for the given feature type (markers or alleles), populating the synonyms object.
	 */
	private void cacheSynonyms(String featureType) throws Exception {
		logger.info(" - caching synonyms for " + featureType);
		
		synonyms = new HashMap<Integer,List<String>>();
		
		String cmd;
		if (MARKER.equals(featureType)) {
			cmd = "select s.marker_key as feature_key, s.synonym " + 
					"from marker_synonym s";
		} else {
			cmd = "select s.allele_key as feature_key, s.synonym " + 
					"from allele_synonym s";
		}
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of synonyms processed
		while (rs.next()) {
			ct++;

			Integer featureKey = rs.getInt("feature_key");
			String synonym = rs.getString("synonym");
			
			if (!synonyms.containsKey(featureKey)) {
				synonyms.put(featureKey, new ArrayList<String>());
			}
			synonyms.get(featureKey).add(synonym);
		}
		rs.close();

		logger.info(" - cached " + ct + " synonyms for " + synonyms.size() + " " + featureType + "s");
	}
	
	/* cache the chromosomes, coordinates, and strands for objects of the given feature type
	 */
	private void cacheLocations(String featureType) throws Exception {
		chromosome = new HashMap<Integer,String>();
		startCoord = new HashMap<Integer,String>();
		endCoord = new HashMap<Integer,String>();
		strand = new HashMap<Integer,String>();
	
		String cmd;
		if (MARKER.equals(featureType)) {
			// may need to pick up chromosome from either coordinates, centimorgans, or cytogenetic band
			// (in order of preference)
			cmd = "select m.marker_key as feature_key, coord.chromosome, coord.start_coordinate, coord.end_coordinate, "
				+ " coord.strand, cm.chromosome as cm_chromosome, cyto.chromosome as cyto_chromosome " + 
				"from marker m " + 
				"left outer join marker_location coord on (m.marker_key = coord.marker_key "
				+ " and coord.location_type = 'coordinates') " + 
				"left outer join marker_location cyto on (m.marker_key = cyto.marker_key "
				+ " and cyto.location_type = 'cytogenetic') " + 
				"left outer join marker_location cm on (m.marker_key = cm.marker_key "
				+ " and cm.location_type = 'centimorgans') " + 
				"where m.organism = 'mouse' " + 
				"and m.status = 'official'";
		} else {
			cmd = "select a.allele_key as feature_key, coord.chromosome, coord.start_coordinate, coord.end_coordinate, "
				+ " coord.strand, cm.chromosome as cm_chromosome, cyto.chromosome as cyto_chromosome " + 
				"from allele a " +
				"inner join marker_to_allele m on (a.allele_key = m.allele_key) " + 
				"left outer join marker_location coord on (m.marker_key = coord.marker_key "
				+ " and coord.location_type = 'coordinates') " + 
				"left outer join marker_location cyto on (m.marker_key = cyto.marker_key "
				+ " and cyto.location_type = 'cytogenetic') " +
				"left outer join marker_location cm on (m.marker_key = cm.marker_key "
				+ " and cm.location_type = 'centimorgans')";
		}

		ResultSet rs = ex.executeProto(cmd, cursorLimit);

		int ct = 0;							// count of locations processed
		while (rs.next()) {
			ct++;

			Integer featureKey = rs.getInt("feature_key");
			String chrom = rs.getString("chromosome");
			
			if (chrom == null) {
				chrom = rs.getString("cm_chromosome");
				if (chrom == null) {
					chrom = rs.getString("cyto_chromosome");
				}
			}
			
			chromosome.put(featureKey, chrom);
			startCoord.put(featureKey, rs.getString("start_coordinate"));
			endCoord.put(featureKey, rs.getString("end_coordinate"));
			strand.put(featureKey, rs.getString("strand"));
		}
		rs.close();

		logger.info(" - cached " + ct + " locations for " + chromosome.size() + " " + featureType + "s");
	}
	
	/* Process the features of the given type, generating documents and sending them to Solr.
	 * Assumes cacheIDs, cacheLocations, and cacheSynonyms have been run for this featureType.
	 */
	private void processFeatures(String featureType) throws Exception {
		logger.info(" - loading " + featureType);
		
		String uriPrefix = uriPrefixes.get(featureType);

		String cmd;
		if (MARKER.equals(featureType)) { 
			cmd = "select m.marker_key as feature_key, m.primary_id, m.symbol, m.name, m.marker_subtype as subtype, " + 
					"s.by_symbol as sequence_num " +
				"from marker m, marker_sequence_num s " + 
				"where m.organism = 'mouse' " + 
				"and m.marker_key = s.marker_key " +
				"and m.status = 'official'";
		} else {
			cmd = "select a.allele_key as feature_key, a.primary_id, a.symbol, a.name, a.allele_type as subtype, " + 
					"s.by_symbol as sequence_num " +
				"from allele a, allele_sequence_num s " + 
				"where a.allele_key = s.allele_key";
		}
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.debug("  - finished query in " + ex.getTimestamp());

		int i = 0;		// counter and sequence number for terms
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			Integer featureKey = rs.getInt("feature_key");

			// start building the solr document 
			SolrInputDocument doc = new SolrInputDocument();

			doc.addField(IndexConstants.QS_PRIMARY_ID, rs.getString("primary_id"));
			doc.addField(IndexConstants.QS_SYMBOL, rs.getString("symbol"));
			doc.addField(IndexConstants.QS_NAME, rs.getString("name"));
			doc.addField(IndexConstants.QS_SEQUENCE_NUM, rs.getInt("sequence_num"));
			doc.addField(IndexConstants.QS_FEATURE_TYPE, rs.getString("subtype"));

			if (MARKER.equals(featureType)) {
				doc.addField(IndexConstants.QS_IS_MARKER, 1);		// feature is a marker
			} else {
				doc.addField(IndexConstants.QS_IS_MARKER, 0);		// feature is an allele
			}
			
			if (uriPrefix != null) {
				doc.addField(IndexConstants.QS_DETAIL_URI, uriPrefix + rs.getString("primary_id"));
			}

			if (allIDs.containsKey(featureKey)) {
				for (String id : allIDs.get(featureKey)) {
					doc.addField(IndexConstants.QS_ACC_ID, id);
				}
			}
			
			if (synonyms.containsKey(featureKey)) {
				for (String s : synonyms.get(featureKey)) {
					doc.addField(IndexConstants.QS_SYNONYM, s);
				}
			}
			
			if (chromosome.containsKey(featureKey)) {
				doc.addField(IndexConstants.QS_CHROMOSOME, chromosome.get(featureKey));
			}
			
			if (startCoord.containsKey(featureKey)) {
				doc.addField(IndexConstants.QS_START_COORD, startCoord.get(featureKey));

				if (endCoord.containsKey(featureKey)) {
					doc.addField(IndexConstants.QS_END_COORD, endCoord.get(featureKey));
				}

				if (strand.containsKey(featureKey)) {
					doc.addField(IndexConstants.QS_STRAND, strand.get(featureKey));
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
		rs.close();

		logger.info("done processing " + i + " " + featureType + "s");
	}
	
	/* process the given fetaure type, loading data from the database, composing documents, and writing to Solr.
	 */
	private void processFeatureType(String featureType) throws Exception {
		logger.info("beginning " + featureType);
		
		cacheIDs(featureType);
		cacheSynonyms(featureType);
		cacheLocations(featureType);
		processFeatures(featureType);
		
		logger.info("finished " + featureType);
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// process one vocabulary at a time, keeping caches in memory only for the current vocabulary
		processFeatureType(MARKER);
		processFeatureType(ALLELE);
		
		// need to add strains and to collect annotation counts beyond the 4 current ones
		
		// commit all the changes to Solr
		commit();
	}
}