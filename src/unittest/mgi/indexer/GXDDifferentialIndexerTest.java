package unittest.mgi.indexer;

import java.util.Set;

import org.jax.mgi.shr.GXDDifferentialMarkerTracker;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests of functions used by the GXD differential indexer
 * 
 */
public class GXDDifferentialIndexerTest 
{	
	/*
	 * Test MarkerResultTracker component
	 */
	/*
	 * Test exclusive any stage structures
	 */
	@Test
	public void testTrackerExclusiveAnyStageEmpty() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		Assert.assertEquals(0,getExclusiveAnyStageStructures(mTracker).size());
	}
	
	@Test
	public void testTrackerExclusiveAnyStageNoAncestors() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","123","123");
		Set<String> exclusiveStructures = getExclusiveAnyStageStructures(mTracker);
		Assert.assertEquals(1,exclusiveStructures.size());
		Assert.assertTrue("does not contain annotated structure 123",exclusiveStructures.contains("TS2:123"));
	}
	
	@Test
	public void testTrackerExclusiveAnyStageOneAncestor() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","123","1234");
		Set<String> exclusiveStructures = getExclusiveAnyStageStructures(mTracker);
		Assert.assertTrue("does not contain ancestor 1234",exclusiveStructures.contains("TS2:1234"));
	}
	
	@Test
	public void testTrackerExclusiveAnyStageSameIdDifferentStages() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","123","1234");
		mTracker.addResultStructureId("28","123","1234");

		Set<String> exclusiveStructures = getExclusiveAnyStageStructures(mTracker);
		Assert.assertTrue("does not contain ancestor TS2:1234",exclusiveStructures.contains("TS2:1234"));
		Assert.assertTrue("does not contain ancestor TS28:1234",exclusiveStructures.contains("TS28:1234"));
	}
	
	@Test
	public void testTrackerExclusiveAnyStageNoAncestorsDifferentStages() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","123","123");
		mTracker.addResultStructureId("28","123","123");

		Set<String> exclusiveStructures = getExclusiveAnyStageStructures(mTracker);
		Assert.assertTrue("does not contain annotated structure TS2:1234",exclusiveStructures.contains("TS2:123"));
		Assert.assertTrue("does not contain annotated structure TS28:1234",exclusiveStructures.contains("TS28:123"));
	}
	
	@Test
	public void testTrackerExclusiveAnyStageMultipleStagesAndStructures() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ANC:5");
		mTracker.addResultStructureId("13","ID:4","ANC:8");

		Set<String> exclusiveStructures = getExclusiveAnyStageStructures(mTracker);
		Assert.assertTrue("does not contain annotated structure TS2:ID:1",exclusiveStructures.contains("TS2:ID:1"));
		Assert.assertTrue("does not contain ancestor TS2:ANC:5",exclusiveStructures.contains("TS2:ANC:5"));
		Assert.assertTrue("does not contain annotated structure TS13:ID:4",exclusiveStructures.contains("TS13:ID:4"));
		Assert.assertTrue("does not contain ancestor TS2:ANC:8",exclusiveStructures.contains("TS13:ANC:8"));
	}
	
	@Test
	public void testTrackerExclusiveAnyStageDuplicateResults() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ANC:5");
		mTracker.addResultStructureId("2","ID:1","ANC:5");

		Set<String> exclusiveStructures = getExclusiveAnyStageStructures(mTracker);
		Assert.assertTrue("does not contain annotated structure TS2:ID:1",exclusiveStructures.contains("TS2:ID:1"));
		Assert.assertTrue("does not contain ancestor TS2:ANC:5",exclusiveStructures.contains("TS2:ANC:5"));
	}
	
	@Test
	public void testTrackerExclusiveAnyStageNotExclusive() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ANC:5");
		mTracker.addResultStructureId("2","ID:2","ANC:7");

		Set<String> exclusiveStructures = getExclusiveAnyStageStructures(mTracker);
		Assert.assertEquals(0,exclusiveStructures.size());
	}
	
	/*
	 * Test exclusive all stages structures
	 */
	@Test
	public void testTrackerExclusiveAllStagesEmpty() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		Assert.assertEquals(0,getExclusiveAllStageStructures(mTracker).size());
	}
	@Test
	public void testTrackerExclusiveAllStagesNotExclusive() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ANC:5");
		mTracker.addResultStructureId("2","ID:2","ANC:7");

		Set<String> exclusiveStructures = getExclusiveAllStageStructures(mTracker);
		Assert.assertEquals(0,exclusiveStructures.size());
	}
	@Test
	public void testTrackerExclusiveAllStagesNotAllStageExclusive() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ANC:5");
		mTracker.addResultStructureId("6","ID:2","ANC:7");

		Set<String> exclusiveStructures = getExclusiveAllStageStructures(mTracker);
		Assert.assertEquals(0,exclusiveStructures.size());
	}
	
	@Test
	public void testTrackerExclusiveAllStagesOneResultNoAncestors() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ID:1");

		Set<String> exclusiveStructures = getExclusiveAllStageStructures(mTracker);
		Assert.assertTrue("does not contain annotated structure ID:1",exclusiveStructures.contains("ID:1"));
	}
	
	@Test
	public void testTrackerExclusiveAllStagesOneResultWithAncestors() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ANC:8");

		Set<String> exclusiveStructures = getExclusiveAllStageStructures(mTracker);
		Assert.assertTrue("does not contain ancestor ANC:8",exclusiveStructures.contains("ANC:8"));
	}
	
	@Test
	public void testTrackerExclusiveAllStagesDuplicateResults() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ANC:8");
		mTracker.addResultStructureId("2","ID:1","ANC:8");

		Set<String> exclusiveStructures = getExclusiveAllStageStructures(mTracker);
		Assert.assertTrue("does not contain annotated structure ID:1",exclusiveStructures.contains("ID:1"));
		Assert.assertTrue("does not contain ancestor ANC:8",exclusiveStructures.contains("ANC:8"));
	}
	
	@Test
	public void testTrackerExclusiveAllStagesMultipleStages() 
	{
		GXDDifferentialMarkerTracker mTracker = new GXDDifferentialMarkerTracker();
		mTracker.addResultStructureId("2","ID:1","ANC:8");
		mTracker.addResultStructureId("5","ID:1","ANC:8");

		Set<String> exclusiveStructures = getExclusiveAllStageStructures(mTracker);
		Assert.assertTrue("does not contain annotated structure ID:1",exclusiveStructures.contains("ID:1"));
		Assert.assertTrue("does not contain ancestor ANC:8",exclusiveStructures.contains("ANC:8"));
	}
	
	public Set<String> getExclusiveAnyStageStructures(GXDDifferentialMarkerTracker mTracker)
	{
		mTracker.calculateExclusiveStructures();
		return mTracker.getExclusiveStructuresAnyStage();
	}
	public Set<String> getExclusiveAllStageStructures(GXDDifferentialMarkerTracker mTracker)
	{
		mTracker.calculateExclusiveStructures();
		return mTracker.getExclusiveStructuresAllStages();
	}

}
