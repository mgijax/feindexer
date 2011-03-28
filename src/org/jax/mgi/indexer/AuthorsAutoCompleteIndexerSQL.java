package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * AuthorsAutoCompleteIndexerSQL
 * @author mhall
 * This class is responsible for populating the authors auto complete index for 
 * the fewi.
 * 
 * The authors are split on any possible type of whitespace before being placed into 
 * the index. 
 */
public class AuthorsAutoCompleteIndexerSQL extends Indexer {

   
    public AuthorsAutoCompleteIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * The main method of this indexer, when called it invokes its doChunks method which 
     * creates the index.
     * 
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub


        AuthorsAutoCompleteIndexerSQL ri = new AuthorsAutoCompleteIndexerSQL("index.url.authorsAC");
        ri.doChunks();

    }
    
    /**
     * This is the main workhorse of the indexer.  It puts together all of the sub objects
     * that the index needs, and then runs the main query.  
     * 
     * For the author autocomplete index there is only a single query, the main one.
     * We then do special processing of the authors in this index, as detailed inline.
     * 
     * Since there is a very small set of authors we do no actual chunking in this indexer.
     */
    
    private void parseAuthor(SolrInputDocument doc, String author) {
        
        String [] temp = author.split("[\\W&&[^']]");
        
        //if (temp.length > 1) {
            for (int i = 0; i < temp.length; i++) {
                    String tempString = "";
                    for (int j = i; j < temp.length; j++) {
                        if (j == 0) {
                            tempString += temp[j];
                        }
                        else {
                            tempString += " " + temp[j];
                        }
                    }
                doc.addField(IndexConstants.REF_AUTHOR, tempString);
            }
        //}
    }
    
    private void doChunks() {
                
        HashSet<String> authorSet = new HashSet<String>();
        Map<String, String> fabAuthorsForGXD = new HashMap<String, String>(); 
        HashMap<String, String> uniqueAuthors = new HashMap<String, String>();
        
        try {
            
            // Get the distinct author list from the database.
            
            logger.info("Getting all distinct author");
            ResultSet rs_overall = ex.executeProto("select distinct a.author, r.indexed_for_gxd " + 
				"from reference r, reference_individual_authors a " + 
				"where r.reference_key = a.reference_key " + 
				"and r.indexed_for_gxd = 1 " +
				"and a.author is not null " + 
				"union " + 
				"select distinct a.author, r.indexed_for_gxd " + 
				"from reference r, reference_individual_authors a " + 
				"where r.reference_key = a.reference_key " + 
				"and r.indexed_for_gxd = 0 " + 
				"and not exists (select 1 " + 
				"from reference_individual_authors a2, reference r2 " + 
				"where r2.reference_key = a2.reference_key " + 
				"and a.author = a2.author " +
				"and r2.indexed_for_gxd = 1) " +
				"and a.author is not null");
            
            String author;
            String indexedForGxd;
            
            while (rs_overall.next()) {
            	author = rs_overall.getString("author");
            	indexedForGxd = rs_overall.getString("indexed_for_gxd");
            	
            	if (!uniqueAuthors.containsKey(author)) {
            		uniqueAuthors.put(author, indexedForGxd);
            	} else if (indexedForGxd.equals("1")) {
            		uniqueAuthors.put(author, indexedForGxd);
            	}
            }
            logger.info("Build HashMap of " + uniqueAuthors.size() + " authors");
            
            List<String> authors = new ArrayList(uniqueAuthors.keySet());
            Collections.sort(authors);
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse out the solr documents
            
            String authorIndexedForGxd;
            logger.info("Parsing them");
            for (String thisAuthor:authors) {
            	authorIndexedForGxd = uniqueAuthors.get(thisAuthor);
                
                SolrInputDocument doc = new SolrInputDocument();
                
                // Add in the author, with no changes to the author.
                
                doc.addField(IndexConstants.REF_AUTHOR, thisAuthor);
                
                parseAuthor(doc, thisAuthor);
                doc.addField(IndexConstants.REF_AUTHOR_SORT, thisAuthor);
                doc.addField(IndexConstants.AC_FOR_GXD, authorIndexedForGxd);
                doc.addField(IndexConstants.AC_UNIQUE_KEY, thisAuthor);
                doc.addField(IndexConstants.AC_IS_GENERATED, "0");
                docs.add(doc);
                
                // Parse out the first 4 tokens of the author field, and make display tokens for them.
                
                String [] temp = thisAuthor.split("[\\W-&&[^']]");
                
                //doc = new SolrInputDocument();
                
                doc = new SolrInputDocument();
                
                String forGXD = null;
                if (temp.length > 1) {
                    for (int i = temp.length - 1; i>= 0; i--) {
                    		forGXD = "0";
                            String tempString = "";
                            for (int j = 0; j < temp.length - 1 && j < 4; j++) {
                                //System.out.println("getting here");
                                if (j == 0) {
                                    tempString += temp[j];
                                }
                                else {
                                    tempString += " " + temp[j];
                                }
                                
                                if (tempString != "") {
                                    doc.addField(IndexConstants.REF_AUTHOR_SORT, tempString);
                                    if (authorIndexedForGxd.equals("1")) {
                                    	forGXD = "1";
                                    } else if (uniqueAuthors.containsKey(tempString)) {
                                    	forGXD = uniqueAuthors.get(tempString);
                                    }
                                    doc.addField(IndexConstants.AC_FOR_GXD, forGXD);
                                    doc.addField(IndexConstants.AC_UNIQUE_KEY, tempString);
                                    doc.addField(IndexConstants.AC_IS_GENERATED, "1");
                                    parseAuthor(doc, tempString);

                                   	docs.add(doc);

                                    doc = new SolrInputDocument();
                                }
                            }
                    }
                }
                

            }
            
            server.add(docs);
            server.commit();
            
        } catch (Exception e) {
            logger.error("In the exception part.");
            e.printStackTrace();}
    }
}
