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
    		int decreaseFactor = 10;
    		int idx = fieldList.indexOf(field);
    		double factor = maxBoost / (Math.pow(decreaseFactor,idx));
    		if(factor < 1)
    		{
    			/* Solr doesn't do well with fractional boosts, so we start going negative
    			 * 	by means of figuring out how many indexes it took to get from maxBoost to between 1 and 0,
    			 * 	then use the remaining index to generate the factor like above, but negated.
    			 * 
    			 * 	Math explained: decreaseFactor^posIdx = maxBoost
    			 * 		Solve for posIdx and we get: posIdx = log(maxBoost) / log(decreaseFactor)
    			 */
    			int posIdx = (int) Math.round(Math.log(maxBoost) / Math.log(decreaseFactor));
    			idx = idx - posIdx;
    			factor = 0 - Math.pow(decreaseFactor,idx);
    		}
    		return (float) factor;
    	}
    	return (float) 0;
    }
}
