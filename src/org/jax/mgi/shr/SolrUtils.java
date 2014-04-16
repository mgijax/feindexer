package org.jax.mgi.shr;

import java.util.List;

/**
 * Utility functions for dealing with Solr
 */
public class SolrUtils {
    
	/*
	 * Applies a boost to a field depending on its priority in the field list
	 */
    public static float boost(List<String> fieldList, String field)
    {
    	return boost(fieldList,field,1000.0);
    }
    public static float boost(List<String> fieldList, String field, Double maxBoost)
    {
    	if(fieldList.contains(field)) 
    	{
    		double decreaseFactor = 1.5;
    		int idx = fieldList.indexOf(field);
    		double factor = maxBoost / (Math.pow(decreaseFactor,idx));
    		if(factor < 1) factor = 1;
    		return (float) factor;
    	}
    	return (float) 0;
    }
}
