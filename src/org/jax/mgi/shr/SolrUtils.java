package org.jax.mgi.shr;

import java.util.List;

/**
 * Utility functions for dealing with Solr
 */
public class SolrUtils {
    
	/*
	 * Applies a boost to a field depending on its priority in the field list
	 * 	default maxBoost is 10,000 for the highest priority item
	 * 	all lower priority items get 90% of the previous priority item's boost
	 * 	E.g. 4th item will get maxBoost * 90% ^ 4 = 6561
	 */
    public static float boost(List<String> fieldList, String field)
    {
    	return boost(fieldList,field,100000.0);
    }
    public static float boost(List<String> fieldList, String field, Double maxBoost)
    {
    	if(fieldList.contains(field)) 
    	{
    		int idx = fieldList.indexOf(field);
    		return (float) (maxBoost * Math.pow(0.8,idx));
    	}
    	return (float) 0;
    }
}
