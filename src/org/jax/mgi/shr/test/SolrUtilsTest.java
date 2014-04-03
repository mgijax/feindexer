package org.jax.mgi.shr.test;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.jax.mgi.shr.SolrUtils;
import org.junit.Test;

public class SolrUtilsTest {

	@Test
	public void testBoost() {
		List<String> fieldList = Arrays.asList("1","2","3","4");
		Assert.assertEquals(10000,round(SolrUtils.boost(fieldList,"1",10000.0)));
		Assert.assertEquals(9000,round(SolrUtils.boost(fieldList,"2",10000.0)));
		Assert.assertEquals(8100,round(SolrUtils.boost(fieldList,"3",10000.0)));
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
