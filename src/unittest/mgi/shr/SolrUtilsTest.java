package unittest.mgi.shr;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.jax.mgi.shr.SolrUtils;
import org.junit.Test;

public class SolrUtilsTest {

	@Test
	public void testBoost() {
		List<String> fieldList = Arrays.asList("1","2","3","4","5","6","7");
		double maxBoost = 1000.0;
		Assert.assertEquals((float)1000.0,SolrUtils.boost(fieldList,"1",maxBoost));
		Assert.assertEquals((float)100.0,SolrUtils.boost(fieldList,"2",maxBoost));
		Assert.assertEquals((float)1.0,SolrUtils.boost(fieldList,"4",maxBoost));
		Assert.assertEquals((float)-10.0,SolrUtils.boost(fieldList,"5",maxBoost));
		Assert.assertEquals((float)-100.0,SolrUtils.boost(fieldList,"6",maxBoost));
	}
	@Test
	public void testBoostNotExists() {
		List<String> fieldList = Arrays.asList("1","2","3","4");
		Assert.assertEquals(0,round(SolrUtils.boost(fieldList,"NOT FOUND")));
	}

	/*
	 * private helper functions
	 */
	private long round(float f){
		return Math.round(f);
	}
}
