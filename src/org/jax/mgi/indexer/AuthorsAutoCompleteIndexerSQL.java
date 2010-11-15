package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

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
        
        try {
            
            // Get the distinct author list from the database.
            
            logger.info("Getting all distinct author");
            ResultSet rs_overall = ex.executeProto("select distinct(author) from reference_individual_authors where author is not null");
            
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            
            // Parse out the solr documents
            
            logger.info("Parsing them");
            while (rs_overall.next()) {
                
                SolrInputDocument doc = new SolrInputDocument();
                
                // Add in the author, with no changes to the author.
                
                doc.addField(IndexConstants.REF_AUTHOR, rs_overall.getString("author"));
                
                // For testing purposes.. add in the authors a second time.
                
                parseAuthor(doc, rs_overall.getString("author"));
                doc.addField(IndexConstants.REF_AUTHOR_SORT, rs_overall.getString("author"));
                doc.addField("testString", rs_overall.getString("author"));
                docs.add(doc);
                
                // Parse out the first 4 tokens of the author field, and make display tokens for them.
                
                String [] temp = rs_overall.getString("author").split("[\\W&&[^']]");
                
                //doc = new SolrInputDocument();
                
                doc = new SolrInputDocument();
                
                if (temp.length > 1) {
                    for (int i = temp.length - 1; i>= 0; i--) {
                            String tempString = "";
                            for (int j = 0; j < temp.length && j < 4; j++) {
                                //System.out.println("getting here");
                                if (j == 0) {
                                    tempString += temp[j];
                                }
                                else {
                                    tempString += " " + temp[j];
                                }
                                
                                if (tempString != "") {
                                    //System.out.println("authorSort: " + tempString);
                                    doc.addField(IndexConstants.REF_AUTHOR_SORT, tempString);
                                    //System.out.println("testString: " + rs_overall.getString("author"));
                                    //doc.addField("testString", rs_overall.getString("author"));
                                    parseAuthor(doc, tempString);
                                    docs.add(doc);
                                    //System.out.println("==================================");
                                    doc = new SolrInputDocument();
                                }
                            }
                    }
                }
                
                //String [] temp = rs_overall.getString("author").split("\\W");
                
                // Add all possible prefix combinations of this author into the index.
                
/*                if (temp.length > 1) {
                    for (int i = 1; i< temp.length; i++) {
                            String tempString = "";
                            for (int j = i; j < temp.length; j++) {
                                if (j == 1) {
                                    tempString += temp[j];
                                }
                                else {
                                    tempString += " " + temp[j];
                                }
                            }
                        doc.addField(IndexConstants.REF_AUTHOR, tempString);
                    }
                }*/
                
                // Since we cannot sort by a multi valued field we add the author back
                // in in an unmodified form into the author_sort column.  This column
                // also doubles as the display field. 
                
                //doc.addField(IndexConstants.REF_AUTHOR_SORT, rs_overall.getString("author"));
                //docs.add(doc);
/*                String [] foo = rs_overall.getString("author").split(" ");
                authorSet.add(foo[0]);*/
            }
            
            // Finally we need to add in a special author that is only the base part of
            // each author string.  For example this means we can now search for
            // all of the eppigs at once.
            
/*            for (String author: authorSet) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField(IndexConstants.REF_AUTHOR, author);
                doc.addField(IndexConstants.REF_AUTHOR_SORT, author);
                docs.add(doc);
            }
            
            // Add all of the documents to the index.
            */
            server.add(docs);
            server.commit();
            
        } catch (Exception e) {
            logger.error("In the exception part.");
            e.printStackTrace();}
    }
}
