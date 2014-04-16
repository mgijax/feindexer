package org.jax.mgi.shr;

import java.util.List;

/**
 * Utility functions for dealing with Solr
 */
public class SolrUtils {
    
	/*
	 * Applies a boost to a field depending on its priority in the field list
	 * 	default maxBoost is 100,000,000 for the highest priority item
	 * 	all lower priority items get 1% of the previous priority item's boost
	 * 	E.g. 4th item will get maxBoost * 1% ^ 4 = 1
	 */
    public static float boost(List<String> fieldList, String field)
    {
    	return boost(fieldList,field,100000000.0);
    }
    public static float boost(List<String> fieldList, String field, Double maxBoost)
    {
    	if(fieldList.contains(field)) 
    	{
    		int decreaseFactor = 5;
    		int idx = fieldList.indexOf(field);
    		double factor = maxBoost / (Math.pow(decreaseFactor,idx));
    		if(factor < 1) factor = 1;
    		return (float) factor;
    	}
    	return (float) 0;
    }
}
