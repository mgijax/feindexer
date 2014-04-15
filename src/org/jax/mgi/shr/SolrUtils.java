package org.jax.mgi.shr;

import java.util.List;

/**
 * Utility functions for dealing with Solr
 */
public class SolrUtils {
    
	/*
	 * Applies a boost to a field depending on its priority in the field list
	 * 	default maxBoost is 1,000,000,000,000 for the highest priority item
	 * 	all lower priority items get .1% of the previous priority item's boost
	 * 	E.g. 4th item will get maxBoost * .1% ^ 4 = 1
	 */
    public static float boost(List<String> fieldList, String field)
    {
    	return boost(fieldList,field,(double)Float.MAX_VALUE);
    }
    public static float boost(List<String> fieldList, String field, Double maxBoost)
    {
    	if(fieldList.contains(field)) 
    	{
    		int idx = fieldList.indexOf(field);
    		return (float) (maxBoost * Math.pow(0.00001,idx));
    	}
    	return (float) 0;
    }
}
