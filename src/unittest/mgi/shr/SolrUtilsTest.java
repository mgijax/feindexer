package unittest.mgi.shr;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.jax.mgi.shr.SolrUtils;
import org.junit.Test;

public class SolrUtilsTest {

	@Test
	public void testBoost() {
		List<String> fieldList = Arrays.asList("1","2","3","4");
		Assert.assertEquals(1000000,round(SolrUtils.boost(fieldList,"1",1000000.0)));
		Assert.assertEquals(1000,round(SolrUtils.boost(fieldList,"2",1000000.0)));
		Assert.assertEquals(1,round(SolrUtils.boost(fieldList,"3",1000000.0)));
	}
	@Test
	public void testBoostNotExists() {
		List<String> fieldList = Arrays.asList("1","2","3","4");
		Assert.assertEquals(0,round(SolrUtils.boost(fieldList,"NOT FOUND")));
	}

	/*
	 * private helper functions
	 */
	private int round(float f){
		return Math.round(f);
	}
}
