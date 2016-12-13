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
	public GXDDifferentialIndexerSQL () 
	{ super("gxdDifferentialMarker"); }

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

			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			logger.info("Creating Solr Documents");
			for(int markerKey : markerResults.keySet())
			{   
				SolrInputDocument doc = new SolrInputDocument();
				// Add the single value fields
				doc.addField(GxdResultFields.KEY, ""+markerKey);
				doc.addField(GxdResultFields.MARKER_KEY, markerKey);

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
			commit();
		}
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
