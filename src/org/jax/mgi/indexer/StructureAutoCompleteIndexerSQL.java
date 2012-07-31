package org.jax.mgi.indexer;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

/**
 * StructureAutoCompleteIndexerSQL
 * @author kstone
 * This index is has the primary responsibility of populating the structure autocomplete index.
 * Since this is such a small index we don't do any actual chunking here.
 * 
 * Note: refactored during 5.x development
 */

public class StructureAutoCompleteIndexerSQL extends Indexer 
{   
    public StructureAutoCompleteIndexerSQL () 
    { super("index.url.structureAC"); }

    public void index() 
    {    
    	Set<String> uniqueIds = new HashSet<String>();
    	Map<String,Integer> termSort = new HashMap<String,Integer>();
    	ArrayList<String> termsToSort = new ArrayList<String>();
    	
        try 
        {    
            logger.info("Getting all distinct structures & synonyms");
            String query = "select a1.structure,a1.synonym, "+
            	"case when (exists (select 1 from anatomy_structures_synonyms a2 where a2.structure=a1.synonym)) "+
            		"then 'true' else 'false' end as is_strict_synonym "+
            	"from anatomy_structures_synonyms a1 ";
            ResultSet rs_overall = ex.executeProto(query);
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            logger.info("calculating sorts");
            while(rs_overall.next())
            {
            	String term = rs_overall.getString("structure");
                String synonym = rs_overall.getString("synonym");
                termsToSort.add(term);
                termsToSort.add(synonym);
            }
            //sort the terms
            Collections.sort(termsToSort,new SmartAlphaComparator());
            for(int i=0;i<termsToSort.size();i++)
            {
            	termSort.put(termsToSort.get(i), i);
            }
            
            logger.info("Creating the documents");
            
            rs_overall = ex.executeProto(query);
            
            while (rs_overall.next()) 
            {
            	// add the synonym structure combo
                String structure = rs_overall.getString("structure");
                String synonym = rs_overall.getString("synonym");
                // structure_key is merely a unique id so that Solr is happy, because structures and synonyms can repeat.
                String structure_key = structure+"-"+synonym;
                if (synonym != null && !synonym.equals("") && !uniqueIds.contains(structure_key))
                {
                    // strict synonym means that this term only exists as a synonym
                    Boolean isStrictSynonym = rs_overall.getString("is_strict_synonym").equals("true");
                    
                	uniqueIds.add(structure_key);
	                SolrInputDocument doc = new SolrInputDocument();
	                doc.addField(IndexConstants.STRUCTUREAC_STRUCTURE, structure);
	                doc.addField(IndexConstants.STRUCTUREAC_SYNONYM, synonym);
	                doc.addField(IndexConstants.STRUCTUREAC_BY_SYNONYM, termSort.get(synonym));
	                doc.addField(IndexConstants.STRUCTUREAC_KEY,structure_key);
	                doc.addField(IndexConstants.STRUCTUREAC_IS_STRICT_SYNONYM, isStrictSynonym);
	                docs.add(doc);
                }
                // Also, make sure that the base structure gets included as a "synonym"
                structure_key = structure+"-"+structure;
                if (!uniqueIds.contains(structure_key))
                {
                	uniqueIds.add(structure_key);
	                SolrInputDocument doc = new SolrInputDocument();
	                doc.addField(IndexConstants.STRUCTUREAC_STRUCTURE, structure);
	                doc.addField(IndexConstants.STRUCTUREAC_SYNONYM, structure);
	                doc.addField(IndexConstants.STRUCTUREAC_BY_SYNONYM, termSort.get(structure));
	                doc.addField(IndexConstants.STRUCTUREAC_KEY,structure_key);
	                doc.addField(IndexConstants.STRUCTUREAC_IS_STRICT_SYNONYM, false);

	                docs.add(doc);
                }
            }
            
            logger.info("Adding the documents to the index.");
            
            server.add(docs);
            server.commit();
            
        } 
        catch (Exception e) 
        {
            logger.error("In the exception part.");
            e.printStackTrace();
        }
    }
}
