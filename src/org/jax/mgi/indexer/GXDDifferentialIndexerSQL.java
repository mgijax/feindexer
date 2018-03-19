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
import org.jax.mgi.shr.GXDDifferentialMarkerTracker;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;

/**
 * GXDDifferentialIndexerSQL
 * @author kstone
 * This index is used primarily for performing GXD differential searches.
 * Each document represents a marker with a marker key,
 * 	which will then be used to filter results from the gxdResult index in a two step process.
 * 
 */

public class GXDDifferentialIndexerSQL extends Indexer 
{   
	//--- instance variables ---//
	
	// shared empty set object to make code simpler later on
	private Set<String> emptySet = new HashSet<String>();
	
	// maps from EMAPS structure key to all of its EMAPS ancestor keys (not just parents)
	private Map<String,Set<String>> emapaAncestors = null;
	
	//--- constructors ---//
	
	public GXDDifferentialIndexerSQL () 
	{ super("gxdDifferentialMarker"); }

	//--- methods ---//
	
	// get the mapping from each EMAPS term key to its EMAPA ancestor keys
	public void fillEmapaAncestors() throws Exception {
		emapaAncestors = new HashMap<String,Set<String>>();
		
		String cmd = "select distinct a.term_key, te.emapa_term_key, e.emapa_term_key as ancestor_term_key "
			+ "from term_ancestor a, term t, term_emap e, term_emap te "
			+ "where a.term_key = t.term_key "
			+ " and a.ancestor_term_key = e.term_key "
			+ " and t.term_key = te.term_key "
			+ " and t.vocab_name = 'EMAPS'";
		
		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			String termKey = rs.getString("term_key");
			if (!emapaAncestors.containsKey(termKey)) {
				emapaAncestors.put(termKey, new HashSet<String>());
				emapaAncestors.get(termKey).add(rs.getString("emapa_term_key"));
			}
			emapaAncestors.get(termKey).add(rs.getString("ancestor_term_key"));
		}
		rs.close();
		logger.info("Got EMAPA ancestors for " + emapaAncestors.size() + " EMAPS terms");
	}
	
	// get the set of EMAPA structure keys that are ancestors of the given EMAPS structure key,
	// including EMAPA key for 'emapsKey' itself
	public Set<String> getEmapaAncestors(String emapsKey) throws Exception {
		if (emapaAncestors == null) {
			fillEmapaAncestors();
		}
		if (emapaAncestors.containsKey(emapsKey)) {
			return emapaAncestors.get(emapsKey);
		}
		return emptySet;
	}
	
	// go through the list of results per marker and identify the exclusive structures, returning a 
	// mapping from:
	//		marker key (String) : set of structure keys (Strings)
	// Because everything traces up the DAG to 'mouse', everything should at least have one exclusive
	// structure returned.
	// Use:  This field is used when the user specifies a structure, specifies no stages, and
	//		checks the "nowhere else" checkbox.
	private Map<String, Set<String>> findExclusiveStructures(Map<Integer, List<Result>> markerResults) throws Exception {
		// The easiest way to find the list of exclusive structures appears to be to find the intersection
		// of the sets of ancestors (each with its annotated structure) for each result.
		
		Map<String, Set<String>> exStructures = new HashMap<String, Set<String>>();
		for (Integer markerKey : markerResults.keySet()) {
			Set<String> commonStructures = null;

			for (Result result : markerResults.get(markerKey)) {
				if (result.expressed) {
					Set<String> resultStructures = new HashSet<String>();
					resultStructures.addAll(getEmapaAncestors(result.structureKey));

					if (commonStructures == null) {
						commonStructures = resultStructures;
					} else {
						commonStructures.retainAll(resultStructures);
					}
				}
			}
			exStructures.put(markerKey + "", commonStructures);
		}
		return exStructures;
	}

	// go through the list of results per marker and identify the exclusive stages, returning a 
	// mapping from:
	//		marker key (String) : set of stages (Strings)
	// Note: Because the QF stage field is allows multiple selections, queries against this field
	//		will likely need to be NOT searches... ie- user selects TS10 and TS12, so we search
	//		for markers where the list of exclusive stages does not contain TS1-9, TS11, or TS13-28.
	// Use:  This field is used when the user does not specify a structure, specifies 1+ stages, and
	//		checks the "nowhere else" checkbox.
	private Map<String, Set<String>> findExclusiveStages(Map<Integer, List<Result>> markerResults) {
		Map<String, Set<String>> exStages = new HashMap<String, Set<String>>();
		for (Integer markerKey : markerResults.keySet()) {
			String stringKey = markerKey + "";
			exStages.put(stringKey, new HashSet<String>());

			for (Result result : markerResults.get(markerKey)) {
				if (result.expressed) {
					exStages.get(stringKey).add(result.stage);
				}
			}
		}
		return exStages;
	}


	// Note:
	// For searches where the user specifies a structure and one or more stages, then checks the
	// "and nowhere else" checkbox, we will need to perform that search as:
	//		1. specified structure is in the set of exclusive structures for the marker, AND
	//		2. no other stages are in the set of exclusive stages for the marker
	// A third field is not needed.  (One had been proposed for structure/stage pairs.)
	
	
	// main logic for building the index
	public void index() throws Exception
	{    
		Map<String,List<String>> structureAncestorIdMap = new HashMap<String,List<String>>();
		logger.info("building map of structure ancestors");
		String structureAncestorQuery = SharedQueries.GXD_EMAP_ANCESTOR_QUERY;
		ResultSet rs = ex.executeProto(structureAncestorQuery);

		while (rs.next())
		{
			String skey = rs.getString("structure_term_key");
			String ancestorId = rs.getString("ancestor_id");
			String structureId = rs.getString("structure_id");
			if(!structureAncestorIdMap.containsKey(skey))
			{
				structureAncestorIdMap.put(skey, new ArrayList<String>());
				// Include original term
				structureAncestorIdMap.get(skey).add(structureId);
			}
			structureAncestorIdMap.get(skey).add(ancestorId);
		}
		logger.info("done gathering structure ancestors");

		Map<String,Structure> structureInfoMap = new HashMap<String,Structure>();
		logger.info("building map of structure info");
		// This map queries EMAPS terms with their stages, stores them by EMAPS ID, but saves the EMAPA ID to be the indexed value
		String structureInfoQuery = "select emapa.primary_id as emapa_id, "
				+ 	"t.primary_id emaps_id, " 
				+ 	"e.stage as theiler_stage "
				+ "from term t join "
				+ 	"term_emap e on t.term_key = e.term_key join "
				+ 	"term emapa on emapa.term_key=e.emapa_term_key "
				+ "where t.vocab_name in ('EMAPS') ";

		rs = ex.executeProto(structureInfoQuery);

		while (rs.next())
		{
			String emapaId = rs.getString("emapa_id");
			String emapsId = rs.getString("emaps_id");
			String ts = rs.getString("theiler_stage");
			structureInfoMap.put(emapsId,new Structure(emapaId,ts));
		}
		logger.info("done gathering structure info");

		ResultSet rs_tmp = ex.executeProto("select count(distinct marker_key) as cnt from expression_result_summary");
		rs_tmp.next();

		int start = 0;
		int end = rs_tmp.getInt("cnt");
		int chunkSize = 2000;

		int modValue = end / chunkSize;

		logger.info("Getting "+end+" marker keys + differential data");

		for (int i = 0; i <= (modValue+1); i++) 
		{
			start = i * chunkSize;

			logger.info ("Processing markers keys (ignoring ambiguous results) " + start + " to " + (start+chunkSize));
			String query = "WITH expr_markers AS (select distinct  marker_key from expression_result_summary ers "+
					" order by marker_key limit "+chunkSize+" offset "+start+") " +
					"select ers.is_expressed, " +
					"ers.structure_key, " +
					"ers.structure_printname, " +
					"emaps.primary_id emaps_id, " +
					"ers.theiler_stage," +
					"ers.marker_key "+
					"from expr_markers em, " +
					"expression_result_summary ers join " +
					"term emaps on ers.structure_key=emaps.term_key "+
					"where em.marker_key=ers.marker_key " +
					"and ers.is_expressed != 'Unknown/Ambiguous' " +
					"and ers.assay_type != 'Recombinase reporter' "+
					"and ers.assay_type != 'In situ reporter (transgenic)' " +
					"and (ers.is_wild_type = 1 or ers.genotype_key=-1) ";
			// "order by is_expressed desc ";
			rs = ex.executeProto(query);
			Map<Integer,List<Result>> markerResults = new HashMap<Integer,List<Result>>();

			logger.info("Organising them");
			while (rs.next()) 
			{           
				int marker_key = rs.getInt("marker_key");
				boolean is_expressed = rs.getString("is_expressed").equals("Yes");
				String structure_key = rs.getString("structure_key");
				String emapsId = rs.getString("emaps_id");
				String stage = rs.getString("theiler_stage");
				if(!markerResults.containsKey(marker_key))
				{
					markerResults.put(marker_key,new ArrayList<Result>());
				}
				markerResults.get(marker_key).add(new Result(structure_key,emapsId,stage,is_expressed));
			}

			// can walk through markerResults here to find for each marker:
			// 1. structures where expression happens exclusively (nowhere outside that structure and its descendants)
			// 2. stages where expression happens exclusively (at no other structures)
			
			Map<String,Set<String>> exclusiveStructures = findExclusiveStructures(markerResults);
			Map<String,Set<String>> exclusiveStages = findExclusiveStages(markerResults);
			
			// ready, set, compose documents!  (note: compose is only one letter different than compost.)
			
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			logger.info("Creating Solr Documents");
			for(int markerKey : markerResults.keySet())
			{   
				String mrkKey = "" + markerKey;
				
				SolrInputDocument doc = new SolrInputDocument();
				// Add the single value fields
				doc.addField(GxdResultFields.KEY, mrkKey);
				doc.addField(GxdResultFields.MARKER_KEY, markerKey);
				
				// populate the exclusive structures and stages fields...
				if (exclusiveStructures.containsKey(mrkKey)) {
					doc.addField(GxdResultFields.DIFF_EXCLUSIVE_STRUCTURES, exclusiveStructures.get(mrkKey));
				}
				if (exclusiveStages.containsKey(mrkKey)) {
					doc.addField(GxdResultFields.DIFF_EXCLUSIVE_STAGES, exclusiveStages.get(mrkKey));
				}

				// create a result tracker for each marker to manage stage/structure combos
				// also calculates when a marker is expressed "exclusively" in a structure
				GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();

				// iterate this marker's results to build various search fields.

				Set<String> posAncestors = new HashSet<String>();
				for(Result result : markerResults.get(markerKey))
				{
					// add the differential ancestor search fields for this marker (for positive results only)
					if(result.expressed)
					{
						// get term ID of ancestors
						if(structureAncestorIdMap.containsKey(result.structureKey))
						{
							List<String> structureAncestorIds = structureAncestorIdMap.get(result.structureKey);
							for (String structureAncestorId : structureAncestorIds)
							{
								// find all the terms + synonyms for each ancestorID
								if(structureInfoMap.containsKey(structureAncestorId))
								{
									Structure ancestor = structureInfoMap.get(structureAncestorId);
									posAncestors.add(ancestor.toString());
									mTracker.addResultStructureId(result.stage,result.structureId,ancestor.structureId);
								}
							}
						}
						// also add the annotated structure to the list of positive ancestors
						posAncestors.add(structureInfoMap.get(result.structureId).toString());
						mTracker.addResultStructureId(result.stage,result.structureId,result.structureId);
					}

				}
				// add the unique positive ancestors (including original structure)
				for(String posAncestor : posAncestors)
				{
					doc.addField(GxdResultFields.DIFF_POS_ANCESTORS, posAncestor);
				}

				// calculate the "exclusively" expressed structures for this marker
				mTracker.calculateExclusiveStructures();

				for(String exclusiveStructureValue : mTracker.getExclusiveStructuresAnyStage())
				{
					doc.addField(GxdResultFields.DIFF_EXC_ANCESTORS,exclusiveStructureValue);
				}

				//add the unique exclusive all stage structures
				for(String exclusiveAllStageStructure : mTracker.getExclusiveStructuresAllStages())
				{
					doc.addField(GxdResultFields.DIFF_EXC_ANCESTORS_ALL_STAGES,exclusiveAllStageStructure);
				}

				docs.add(doc);
				if (docs.size() > 1000) 
				{
					//logger.info("Adding a stack of the documents to Solr");
					startTime();
					writeDocs(docs);
					long endTime = stopTime();
					if(endTime > 500)
					{
						logger.info("time to call writeDocs() "+stopTime());
					}
					docs = new ArrayList<SolrInputDocument>();
				}
			}
			writeDocs(docs);
		}
		commit();
	}

	/*
	 * For debugging purposes only
	 */
	private long startTime = 0;
	
	public void startTime() {
		startTime = System.nanoTime();
	}
	
	public long stopTime() {
		long endTime = System.nanoTime();
		return (endTime - startTime)/1000000;

	}

	// helper classes
	public class Result
	{
		public String structureKey;
		public String structureId;
		public String stage;
		public boolean expressed;

		public Result(String structureKey,String structureId,String stage,boolean expressed)
		{
			this.structureKey=structureKey;
			this.structureId=structureId;
			this.stage=stage;
			this.expressed=expressed;
		}
	}
	public class Structure
	{
		public String structureId;
		public String stage;
		public Structure(String structureId,String stage)
		{
			this.structureId=structureId;
			this.stage=stage;
		}
		@Override
		public String toString() {
			return structureToSolrString(stage,structureId);
		}
	}

	public static String structureToSolrString(String stage,String structureId)
	{
		return "TS"+stage+":"+structureId;
	}
}
