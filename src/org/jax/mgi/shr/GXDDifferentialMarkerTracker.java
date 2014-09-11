package org.jax.mgi.shr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jax.mgi.indexer.GXDDifferentialIndexerSQL;

public class GXDDifferentialMarkerTracker 
{    	
	public Map<String,ExclusiveStageTracker> stageTrackers = new HashMap<String,ExclusiveStageTracker>();
	
	public void addResultStructureId(String stage,String annotatedId, String ancestorId)
	{
		ExclusiveStageTracker est = getStageTracker(stage);
		est.addResultStructureId(annotatedId,ancestorId);
	}
	
	public void calculateExclusiveStructures()
	{
		for(ExclusiveStageTracker est : stageTrackers.values())
		{
			est.calculateExclusiveStructures();
		}
	}
	
	/*
	 * Returns structures that are exclusive in a stage (doesn't matter which stage)
	 *  We return not just the structureId here, but the full query string with "TS:<stage>" prepended
	 */
	public Set<String> getExclusiveStructuresAnyStage()
	{
		Set<String> exclusiveStructureIds = new HashSet<String>();
		for(ExclusiveStageTracker est : stageTrackers.values())
		{
			for(String structureId : est.getExclusiveStructures())
			{
    			exclusiveStructureIds.add(GXDDifferentialIndexerSQL.structureToSolrString(est.stage,structureId));
			}
		}
		return exclusiveStructureIds;
	}
	
	/*
	 * Returns structures that are exclusive throughout all stages that this marker has annotations for
	 */
	public Set<String> getExclusiveStructuresAllStages()
	{    		
		// further go through the calculated "by stage" exclusive structures 
    	// and apply special rules to get exclusive structures for all annotated stages
    	Set<String> exclusiveAllStageStructures = new HashSet<String>();
    	for(ExclusiveStageTracker et : stageTrackers.values())
    	{
    		// I know some folks don't like loop labels, but I'm sorry, this particular logic calls for it.
    		structure1Loop:
    		for(String structureId : et.getExclusiveStructures())
    		{
    			// only include this term if this exact term exists at least once in every stage
    			// OR is a substring of at least one term in every stage (Got that?)
    			// Ok, good. Let's begin...
    			
    			stage2Loop:
    			for(ExclusiveStageTracker et2 : stageTrackers.values())
            	{
    				//we must search every other stage's exclusive structures
    				if(et.stage!=et2.stage)
    				{
                		for(String structureId2 : et2.getExclusiveStructures())
                		{
                			// I'm not totally sure what the contains was doing. I don't think we need it for ID searches now
                			//if(structureId.equals(structureId2) || structureId2.contains(structureId))
                    		if(structureId.equals(structureId2))
                			{
                				// found match! continue to next stage
                				continue stage2Loop;
                			}
                		}
                		// we did not find a match... have to continue to next structure
                		continue structure1Loop;
            		}
            	}
    			// Congratulations. You made it this far!
    			exclusiveAllStageStructures.add(structureId);
    		}
    	}
		return exclusiveAllStageStructures;
	}
	
	private ExclusiveStageTracker getStageTracker(String stage)
	{
		if(stageTrackers.containsKey(stage))
		{
			return stageTrackers.get(stage);
		}
		ExclusiveStageTracker est = new ExclusiveStageTracker(stage);
		stageTrackers.put(stage,est);
		return est;
	}
	
	// classes to help calculate exclusive structure fields
    /*
     * ExclusiveResultTracker
     * for each given positive result, it tracks all the ancestorIds of the annotated structureId
     */
    public class ExclusiveResultTracker
    {
    	public Set<String> ancestorStructures = new HashSet<String>();
    	
    	public ExclusiveResultTracker(String annotatedId)
    	{
    		ancestorStructures.add(annotatedId);
    	}
    	public void addAncestor(String structureId)
    	{
    		ancestorStructures.add(structureId);
    	}
    }
    
    /*
     * ExclusiveStageTracker
     * tracks exclusive structures within a theiler stage
     */
    public class ExclusiveStageTracker
    {
    	public String stage;
    	
    	private final Map<String,ExclusiveResultTracker> resultTrackers = new HashMap<String,ExclusiveResultTracker>();
    	private Set<String> exclusiveStructures = new HashSet<String>();
    	
    	ExclusiveStageTracker(String stage)
    	{
    		this.stage=stage;
    	}
    	public void addResultTracker(String structureId,ExclusiveResultTracker ert)
    	{
    		if(!resultTrackers.containsKey(structureId))
    		{
    			resultTrackers.put(structureId,ert);
    		}
    	}
    	private ExclusiveResultTracker getResultTracker(String annotatedId)
    	{
    		if(resultTrackers.containsKey(annotatedId))
    		{
    			return resultTrackers.get(annotatedId);
    		}
    		ExclusiveResultTracker ert = new ExclusiveResultTracker(annotatedId);
    		resultTrackers.put(annotatedId,ert);
    		return ert;
    	}
    	
    	public void addResultStructureId(String annotatedId,String ancestorId)
    	{
    		ExclusiveResultTracker ert = getResultTracker(annotatedId);
    		ert.addAncestor(ancestorId);
    	}
    	
    	
    	// determine from the current list of structures, which ones are exclusive
    	public void calculateExclusiveStructures()
    	{
    		// do set difference of all ancestors for each result tracker
    		for(ExclusiveResultTracker ert : resultTrackers.values())
    		{
    			if(exclusiveStructures.size()==0) exclusiveStructures = ert.ancestorStructures;
    			else exclusiveStructures.retainAll(ert.ancestorStructures);
    		}
    	}
    	public Set<String> getExclusiveStructures()
    	{
    		return exclusiveStructures;
    	}
    }
    
}
