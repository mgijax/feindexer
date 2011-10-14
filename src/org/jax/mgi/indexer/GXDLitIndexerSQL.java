package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.searchInfo.GxdLitAgeAssayTypePair;
import org.jax.mgi.searchInfo.MarkerSearchInfo;
import org.jax.mgi.searchInfo.ReferenceSearchInfo;
import org.jax.mgi.shr.fe.IndexConstants;

/**
 * RefIndexerSQL
 * @author mhall
 * This class has the primary responsibility for populating the reference index.
 * It has a fairly large number of sub object relationships, but since its a 
 * fairly small dataset there is no real need to do actual chunking.
 */

public class GXDLitIndexerSQL extends Indexer {

   
   
    public GXDLitIndexerSQL (String httpConnection) {
        super(httpConnection);
    }
    

    /**
     * The main method, called from the command line in order to start the indexing.
     * @param args
     */
    public static void main(String[] args) {

        GXDLitIndexerSQL ri = new GXDLitIndexerSQL("index.url.gxdLitIndex");
        ri.doChunks();
         
    }
    
    /**
     * The main worker of this class, it starts by gathering up all the 1->N 
     * subobject relationships that a reference can have, and then runs the 
     * main query.  We then enter a parsing phase where we place the data into
     * solr documents, and put them into the index.
     */
    
    private void doChunks() {
    	
    	logger.info("Gathering up the marker information.");
    	
    	Map <String, MarkerSearchInfo> markerSearchInfo = getMarkerInfo();
    	
    	logger.info("Past the marker info step");

    	logger.info("Gathering the reference information");
    	
    	Map <String, ReferenceSearchInfo> referenceSearchInfo = getReferenceInfo();
    	
    	logger.info("Gathering the assay type/age pairings.");
    	
    	Map <String, List <GxdLitAgeAssayTypePair>> records = getAgeAssayTypeInfo();
    	
    	logger.info("Past the reference info step.");
    	
    	logger.info("Generating the solr documents.");
    	
    	logger.info("Gathering the expression unique results");
    	
    	ResultSet rs_base = ex.executeProto("select distinct ei.marker_key, ei.reference_key, ei.index_key, " +
    		"msn.by_symbol, rsn.by_author " + 
			"from expression_index as ei " + 
			"join marker_sequence_num as msn on ei.marker_key = msn.marker_key " + 
			"join reference_sequence_num as rsn on ei.reference_key = rsn.reference_key " + 
			"order by ei.marker_key, ei.reference_key, ei.index_key");

    	logger.info("Creating solr documents");
    	
    	Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    	
    	try {
    		rs_base.next();
    		
    		while (!rs_base.isAfterLast()) {
    			SolrInputDocument doc = new SolrInputDocument();

    			doc.addField(IndexConstants.GXD_LIT_SINGLE_KEY, rs_base.getString("index_key"));
    			doc.addField(IndexConstants.MRK_BY_SYMBOL, rs_base.getString("by_symbol"));
    			doc.addField(IndexConstants.REF_AUTHOR_SORT, rs_base.getString("by_author"));
    			
    			
    			for (GxdLitAgeAssayTypePair pair: records.get(rs_base.getString("index_key"))) {

        			doc.addField(IndexConstants.GXD_LIT_AGE, pair.getAge());
        			doc.addField(IndexConstants.GXD_LIT_ASSAY_TYPE, pair.getAssayType());
    				
    			}
    			doc.addField(IndexConstants.REF_KEY, rs_base.getString("reference_key"));
    			doc.addField(IndexConstants.MRK_KEY, rs_base.getString("marker_key"));    	
    			
    			// Add in all the marker information for this tuple
    			
    			MarkerSearchInfo msi = markerSearchInfo.get(rs_base.getString("marker_key"));
    			for (String nomen: msi.getNomen()) {
        			doc.addField(IndexConstants.GXD_MRK_NOMEN,
				    this.translateString(nomen));
    			}
    			
    			// Add in all the reference information for this tuple.
    			
    			ReferenceSearchInfo rsi = referenceSearchInfo.get(rs_base.getString("reference_key"));
    			
    			doc.addField(IndexConstants.REF_JOURNAL, rsi.getJournal());
    			
    			for (String journal: rsi.getJournalFacet()) {
        			doc.addField(IndexConstants.REF_JOURNAL_FACET, journal);	
    			}
    			
    			for (String author: rsi.getAuthorsFacet()) {
    				doc.addField(IndexConstants.REF_AUTHOR_FACET, author);
    			}
    			
    			for (String author: rsi.getAuthors()) {
    				doc.addField(IndexConstants.REF_AUTHOR_FORMATTED, author);
    			}
    			
    			for (String author: rsi.getLastAuthor()) {
    				doc.addField(IndexConstants.REF_LAST_AUTHOR, author);
    			}
    			
    			for (String author: rsi.getFirstAuthor()) {
    				doc.addField(IndexConstants.REF_FIRST_AUTHOR, author);
    			}
    			
    			doc.addField(IndexConstants.REF_ABSTRACT_STEMMED, rsi.getAbstractStemmed());
    			doc.addField(IndexConstants.REF_ABSTRACT_UNSTEMMED, rsi.getAbstractUnstemmed());
    			
    			doc.addField(IndexConstants.REF_TITLE_STEMMED, rsi.getTitleStemmed());
    			doc.addField(IndexConstants.REF_TITLE_UNSTEMMED, rsi.getTitleUnstemmed());
    			
    			doc.addField(IndexConstants.REF_TITLE_ABSTRACT_STEMMED, rsi.getTitleAbstractStemmed());
    			doc.addField(IndexConstants.REF_TITLE_ABSTRACT_UNSTEMMED, rsi.getTitleAbstractUnstemmed());
    			
    			doc.addField(IndexConstants.REF_YEAR, rsi.getYear());
    			
    			docs.add(doc);
                if (docs.size() > 10000) {
                    logger.info("Adding a stack of the documents to Solr");
                    server.add(docs);
                    docs = new ArrayList<SolrInputDocument>();
                    logger.info("Done adding to solr, Moving on");
                }
    			
    			rs_base.next();
    		}
    		
            server.add(docs);
            server.commit();
            
    	}
    	catch (Exception e) {e.printStackTrace();}
    	
    	logger.info("Complete");
    	
    }
    
    /** translate String 's' to have any unacceptable characters converted to
     * be whitespace.  Also ensures no more than one consecutive space in the
     * returned String.
     * @param s input String
     * @return a cleansed version of s
     */
    private String translateString (String s) {
	return s.replaceAll ("[^A-Za-z0-9]", " ").replaceAll("  +", " ");
    }

    private Map<String, List <GxdLitAgeAssayTypePair>> getAgeAssayTypeInfo() {
    	
    	Map <String, List <GxdLitAgeAssayTypePair>> records = new HashMap<String, List <GxdLitAgeAssayTypePair>>();
    	
    	ResultSet rs_assay_age_pair = ex.executeProto("select index_key, assay_type, age_string " +
    			"from expression_index_stages " +
    			"order by index_key");
    	
    	try {
    		rs_assay_age_pair.next();
    		
    		while (!rs_assay_age_pair.isAfterLast()) {
    		
    			GxdLitAgeAssayTypePair pair = new GxdLitAgeAssayTypePair();
    			pair.setAge(rs_assay_age_pair.getString("age_string"));
    			pair.setAssayType(rs_assay_age_pair.getString("assay_type"));

    			if (records.containsKey(rs_assay_age_pair.getString("index_key"))) {
    				records.get(rs_assay_age_pair.getString("index_key")).add(pair);
    			}
    			else {
    				List<GxdLitAgeAssayTypePair> pairList = new ArrayList<GxdLitAgeAssayTypePair> ();
    				pairList.add(pair);
    				records.put(rs_assay_age_pair.getString("index_key"), pairList);	
    			}
    			
    			rs_assay_age_pair.next();
    		}
    	}
    	catch (Exception e) {e.printStackTrace();}
    	
    	return records;
    }
    
    /**
     * Gather up the marker information that this index will require.  We will only be gathering the 
     * markers that are referenced in the gxd lit index.
     * 
     * @return Map<String, MarkerSearchInfo> A mapping of markerKey -> MarkerSearchInfo objects.
     */
    
    private Map<String, MarkerSearchInfo> getMarkerInfo() {
    	
    	ResultSet rs_marker_base = ex.executeProto("select distinct m.marker_key, m.symbol, m.name, msn.by_symbol " +
			"from marker as m " + 
			"join expression_index as ei on m.marker_key = ei.marker_key " +
			"join marker_sequence_num as msn on m.marker_key = msn.marker_key");

    	String markerKeyToSynonymsSQL = "select distinct m.marker_key, m.synonym " +
    		"from marker_synonym as m " + 
			"join expression_index as ei on m.marker_key = ei.marker_key " +
			"order by m.marker_key";
    	
    	logger.info("Creating the marker to synonym hash");
    	
    	HashMap <String, HashSet<String>> markerKeyToSynonyms = makeHash(markerKeyToSynonymsSQL, "marker_key", "synonym");
    	
    	Map <String, MarkerSearchInfo> markerInfo = new HashMap <String, MarkerSearchInfo> ();
    	
    	logger.info("Entering the main marker section");
    	
    	try {
	    	rs_marker_base.next();
	    	
	    	while (!rs_marker_base.isAfterLast()) {
	    		MarkerSearchInfo msi = new MarkerSearchInfo();
	    		msi.addNomen(rs_marker_base.getString("symbol"));
	    		msi.addNomen(rs_marker_base.getString("name"));
	    		msi.setBySymbol(rs_marker_base.getString("by_symbol"));
	    		
                if (markerKeyToSynonyms.containsKey(rs_marker_base.getString("marker_key"))) {
                    for (String synonym: markerKeyToSynonyms.get(rs_marker_base.getString("marker_key"))) {
                        msi.addNomen(synonym);
                    }
                }
                
                markerInfo.put(rs_marker_base.getString("marker_key"), msi);
                rs_marker_base.next();
	    	}
    	}
    	catch (Exception e) {e.printStackTrace();}
    	
    	return markerInfo;
    }
    
    private Map<String, ReferenceSearchInfo> getReferenceInfo() {
    	Map<String, ReferenceSearchInfo> referenceInfo = new HashMap<String, ReferenceSearchInfo> ();
    	
    	logger.info("Getting the base reference information");
    	
    	ResultSet rs_reference_base = ex.executeProto("select distinct r.reference_key, r.year, r.authors, r.journal, r.title, ra.abstract " + 
			"from reference as r " + 
			"join expression_index as ei on r.reference_key = ei.reference_key " + 
			"inner join reference_abstract as ra on r.reference_key = ra.reference_key"); 

    	logger.info("Seleceting all reference -> publisher");
        String pubToRefSQL = "select distinct rb.reference_key, rb.publisher " + 
			"from reference_book as rb " +
			"join expression_index as ei on ei.reference_key = rb.reference_key";
        logger.info(pubToRefSQL);
        HashMap <String, HashSet <String>> pubToRefs = makeHash(pubToRefSQL, "reference_key", "publisher");        
        
        // Author information, for the formatted authors.
        logger.info("Seleceting all reference -> author associations");
        String referenceAuthorSQL = "select distinct r.reference_key, r.author " +  
			"from reference_individual_authors as r " + 
			"join expression_index as ei on r.reference_key = ei.reference_key";
        logger.info(referenceAuthorSQL);
        HashMap <String, HashSet <String>> refAuthors = makeHash(referenceAuthorSQL,"reference_key","author");        
        
        // last Author information, for the formatted authors.
        logger.info("Seleceting all reference -> last author");
        String referenceAuthorLastSQL = "select r.reference_key, r.author " +  
			"from reference_individual_authors as r " + 
			"join expression_index as ei on ei.reference_key = r.reference_key " + 
			"where r.is_last = 1";
        logger.info(referenceAuthorLastSQL);
        HashMap <String, HashSet <String>> refAuthorsLast = makeHash(referenceAuthorLastSQL,"reference_key","author");
                
        // first Author information, for the formatted authors.
        logger.info("Seleceting all reference -> first author");
        String referenceAuthorFirstSQL = "select r.reference_key, r.author " +  
		"from reference_individual_authors as r " + 
		"join expression_index as ei on ei.reference_key = r.reference_key " + 
		"where r.sequence_num = 1";
        
        logger.info(referenceAuthorFirstSQL);
        HashMap <String, HashSet <String>> refAuthorsFirst = makeHash(referenceAuthorFirstSQL,"reference_key","author");        
               
    	try {
    		rs_reference_base.next();
    		
    		while (!rs_reference_base.isAfterLast()) {
    			
    			String reference_key = rs_reference_base.getString("reference_key");
    			ReferenceSearchInfo rsi = new ReferenceSearchInfo();
    			rsi.setYear(rs_reference_base.getString("year"));
    			rsi.setJournal(rs_reference_base.getString("journal"));
    			
    			referenceInfo.put(rs_reference_base.getString("reference_key"), rsi);
    			
                // Add in the 1->1 publisher relationships, this only applies to books.
                
                if (pubToRefs.containsKey(rs_reference_base.getString("reference_key"))) {
                    for (String publisher: pubToRefs.get(rs_reference_base.getString("reference_key"))) {
                        rsi.addJournalFacet(publisher);
                    }
                }   
    			
                // Add in all of the indivudual authors, specifically formatted for 
                // searching.
                
                // In a nutshell we split on whitespace, and then add in each resulting
                // token into the database, as well as the entirety of the author string.
                
                if (refAuthors.containsKey(reference_key)) {
                    for (String author: refAuthors.get(reference_key)) {
                        rsi.addAuthor(author);
                    	//doc.addField(IndexConstants.REF_AUTHOR_FORMATTED, author);
                        
                        // Add in a single untouched version of the formatted authors
                        if (author == null || author.equals(" ")) {
                        	rsi.addAuthorFacet("No author listed");
                            //doc.addField(IndexConstants.REF_AUTHOR_FACET, "No author listed");
                        }
                        else {
                        	rsi.addAuthorFacet(author);
                            //doc.addField(IndexConstants.REF_AUTHOR_FACET, author);
                        }
                        
                        if (author != null) {
                        String [] temp = author.split("[\\W-&&[^']]");
                        
                        // Add all possible permutations of this author into the index.
                        if (temp.length > 1) {
                            String tempString = "";
                            for (int i = 0; i< temp.length && i <= 3; i++) {
                                    if (i == 0) {
                                        tempString = temp[i];
                                    }
                                    else {
                                        tempString = tempString + " " + temp[i];
                                    }
                                    rsi.addAuthor(tempString);
                                    //doc.addField(IndexConstants.REF_AUTHOR_FORMATTED, tempString);
                                }
                            }
                        }
                    }
                }
                
                // Add all possible prefixes for the last author only
                
                if (refAuthorsLast.containsKey(reference_key)) {
                    for (String author: refAuthorsLast.get(reference_key)) {
                        //doc.addField(IndexConstants.REF_LAST_AUTHOR, author);
                        rsi.addLastAuthor(author);
                        
                        if (author != null) {
                            String [] temp = author.split("[\\W-&&[^']]");
                            
                            // Add all possible permutations of this author into the index.
                            if (temp.length > 1) {
                                String tempString = "";
                                for (int i = 0; i< temp.length && i <= 3; i++) {
                                        if (i == 0) {
                                            tempString = temp[i];
                                        }
                                        else {
                                            tempString = tempString + " " + temp[i];
                                        }
                                        //doc.addField(IndexConstants.REF_LAST_AUTHOR, tempString);
                                        rsi.addLastAuthor(tempString);
                                    }
                                }
                            }
                    }
                }

                // Add all possible prefixes for the first author only
                
                if (refAuthorsFirst.containsKey(reference_key)) {
                    for (String author: refAuthorsFirst.get(reference_key)) {
                        rsi.addFirstAuthor(author);
                    	//doc.addField(IndexConstants.REF_FIRST_AUTHOR, author);
                        
                        if (author != null) {
                            String [] temp = author.split("[\\W-&&[^']]");
                            
                            // Add all possible permutations of this author into the index.
                            if (temp.length > 1) {
                                String tempString = "";
                                for (int i = 0; i< temp.length && i <= 3; i++) {
                                        if (i == 0) {
                                            tempString = temp[i];
                                        }
                                        else {
                                            tempString = tempString + " " + temp[i];
                                        }
                                        rsi.addFirstAuthor(tempString);
                                        //doc.addField(IndexConstants.REF_FIRST_AUTHOR, tempString);
                                    }
                                }
                            }
                    }
                }
                
                // This string is used for the overall smushed title and abstract field.
                String titleAndAbstract = "";
                
                if (rs_reference_base.getString("title") != null) {
                    String tempTitle = rs_reference_base.getString("title").replaceAll("\\p{Punct}", " ");
                    
                    //doc.addField(IndexConstants.REF_TITLE_STEMMED, tempTitle);
                    rsi.setTitleStemmed(tempTitle);
                    //doc.addField(IndexConstants.REF_TITLE_UNSTEMMED, tempTitle);
                    rsi.setTitleUnstemmed(tempTitle);
                    titleAndAbstract = tempTitle;
                }
                 
                if (rs_reference_base.getString("abstract") != null) {                
                    String tempAbstract = rs_reference_base.getString("abstract").replaceAll("\\p{Punct}", " ");
    
                    //doc.addField(IndexConstants.REF_ABSTRACT_STEMMED, tempAbstract);
                    rsi.setAbstractStemmed(tempAbstract);
                    //doc.addField(IndexConstants.REF_ABSTRACT_UNSTEMMED, tempAbstract);
                    rsi.setAbstractUnstemmed(tempAbstract);
                    
                    // Put together the second part of the smushed title and abstract
                    
                    if (titleAndAbstract.equals("")) {
                        titleAndAbstract = tempAbstract;
                    } else {
                        titleAndAbstract = titleAndAbstract + " WORDTHATCANTEXIST " + tempAbstract;
                    }
                    
                }
                
                // Add the smushed title and abstract into the document
                
                //doc.addField(IndexConstants.REF_TITLE_ABSTRACT_STEMMED, titleAndAbstract);
                rsi.setTitleAbstractStemmed(titleAndAbstract);
                //doc.addField(IndexConstants.REF_TITLE_ABSTRACT_UNSTEMMED, titleAndAbstract);
                rsi.setTitleAbstractUnstemmed(titleAndAbstract);

                referenceInfo.put(reference_key, rsi);
                
    			rs_reference_base.next();
    		}
    	}
    	catch (Exception e) {e.printStackTrace();}
    	
    	return referenceInfo;
    }
    
}
