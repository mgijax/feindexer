package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.SolrUtils;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.MarkerSummaryFields;

/**
 * MarkerIndexerSQL
 * @author kstone
 * This class populates the marker index, which powers both marker ID lookups
 * 	as well as the marker summary.
 */

public class MarkerIndexerSQL extends Indexer 
{  
	public static String GO_VOCAB="GO";
	public static String GO_FUNCTION="Function";
	public static String GO_PROCESS="Process";
	public static String GO_COMPONENT="Component";
	public static String INTERPRO_VOCAB="InterPro Domains";
	
	public Map<String,Set<String>> ancestorTerms = null; // includes ancestors and synonyms
	public Map<String,Set<String>> ancestorIds = null;
	public Map<String,Set<String>> termSynonyms = null;
	
    public MarkerIndexerSQL () 
    {
        super("index.url.marker");
    }
    
    /**
     * The main worker of this class, it starts by gathering up all the 1->N 
     * subobject relationships that a reference can have, and then runs the 
     * main query.  We then enter a parsing phase where we place the data into
     * solr documents, and put them into the index.
     */
    public void index() throws Exception
    {
    	this.tempTables();
    	
    	logger.info("loading go ancestor terms");
    	String goAncestorQuery = "select t.primary_id term_id,tas.ancestor_term,tas.ancestor_primary_id " +
    			"from term t join term_ancestor_simple tas on tas.term_key=t.term_key " +
    			"where t.vocab_name in ('GO','InterPro Domains') " +
    			"and tas.ancestor_term not in ('cellular_component','biological_process','molecular_function') ";
    	this.ancestorTerms = this.populateLookup(goAncestorQuery,"term_id","ancestor_term","GO term ID -> Ancestor Term");
    	this.ancestorIds = this.populateLookup(goAncestorQuery,"term_id","ancestor_primary_id","GO term ID -> Ancestor Term ID");
    	String synonymQuery = "select t.primary_id term_id,ts.synonym " +
    			"from term t join term_synonym ts on ts.term_key=t.term_key " +
    			"where t.vocab_name in ('GO','InterPro Domains') ";
    	this.termSynonyms = this.populateLookup(synonymQuery,"term_id","synonym","term ID -> synonyms");
    	
        // How many markers are there total?
        ResultSet rs = ex.executeProto("select max(marker_key) as maxMarkerKey from marker");
        rs.next();
        
        Integer start = 0;
        Integer end = rs.getInt("maxMarkerKey");
    	int chunkSize = 20000;
        
        int modValue = end.intValue() / chunkSize;
        
        // Perform the chunking
        logger.info("Loading markers up to marker_key="+end);
        for (int i = 0; i <= modValue; i++) 
        {
            start = i * chunkSize;
            end = start + chunkSize;
            
            processMarkers(start,end);
        }
        
       logger.info("Done loading markers");
       
       // clean up any references
       this.ancestorTerms=null;
       this.ancestorIds=null;
       this.termSynonyms=null;
    }
    
    private void processMarkers(int start, int end) throws Exception
    {
    	logger.info("Processing marker keys "+start+" to "+end);
    	
    	 // Get marker id -> marker relationships
        String markerToIDSQL = "select distinct marker_key, acc_id from marker_id where marker_key > " + start + " and marker_key <= "+ end + " and private = 0";
        Map<String,Set<String>> idToMarkers = this.populateLookup(markerToIDSQL, "marker_key", "acc_id","marker to IDs");

        // Get marker -> reference relationships, by marker key
        String markerToReferenceSQL = "select distinct marker_key, reference_key from marker_to_reference where marker_key > " + start + " and marker_key <= "+ end;
        Map<String,Set<String>> referenceToMarkers = this.populateLookup(markerToReferenceSQL, "marker_key", "reference_key","marker to ref keys");
                    
        Map<String,List<MarkerTerm>> termToMarkers = this.getMarkerTerms(start,end);

        // Get marker terms and their IDs
        String markerToTermIDSQL = "select distinct m.marker_key, a.term_id from marker_to_annotation m, annotation a where m.marker_key > " + start + " and m.marker_key <= "+ end + " and m.annotation_key = a.annotation_key";
        Map<String,Set <String>> termToMarkersID = this.populateLookup(markerToTermIDSQL, "marker_key", "term_id","marker to Terms/IDs");
        
        // Get marker location information
        Map<Integer,MarkerLocation> locationMap = getMarkerLocations(start,end);
        
        // Get marker nomen information
        Map<Integer,List<MarkerNomen>> nomenMap = getMarkerNomen(start,end);
        
        // phenotypes
    	Map<String,Set<String>> alleleNotesMap = getAlleleNotesMap(start,end);
    	Map<String,Set<String>> alleleTermMap = getAlleleTermsMap(start,end);
    	Map<String,Set<String>> alleleTermIdMap = getAlleleTermIdsMap(start,end);
        
        logger.info("Getting all mouse markers");
        String markerSQL = "select m.marker_key, m.primary_id marker_id,m.symbol, " +
        			"m.name, m.marker_type, m.marker_subtype, m.status, m.organism, " +
        			"m.coordinate_display, " +
        			"m.location_display, " +
        			"m_sub_type.term_key marker_subtype_key, " +
        			"msn.by_symbol, " +
        			"msn.by_location " +
        		"from marker m join " +
        		"term m_sub_type on m_sub_type.term=m.marker_subtype join " +
        		"marker_sequence_num msn on msn.marker_key=m.marker_key " +
        		"where m.organism = 'mouse' " +
        		"	and m_sub_type.vocab_name='Marker Category' " +
        		"	and m.marker_key > "+start+" and m.marker_key <= "+end;
        ResultSet rs = ex.executeProto(markerSQL);
        
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
      
        // strictly for mapping the field boosts for nomen queries
        List<String> nomenKeyList = new ArrayList<String>(MarkerSummaryFields.NOMEN_FIELDS.keySet());
        
        // Parse the base query, adding its contents into solr  
        logger.info("Parsing them");
        while (rs.next()) 
        {
        	String mrkKey = rs.getString("marker_key");
        	int mrkKeyInt = rs.getInt("marker_key");
            SolrInputDocument doc = new SolrInputDocument();
            
            doc.addField(IndexConstants.MRK_KEY, rs.getString("marker_key"));
            doc.addField(IndexConstants.MRK_SYMBOL, rs.getString("symbol"));
            doc.addField(IndexConstants.MRK_NAME, rs.getString("name"));
            doc.addField(IndexConstants.MRK_TYPE, rs.getString("marker_type"));
            doc.addField(MarkerSummaryFields.FEATURE_TYPE, rs.getString("marker_subtype"));
            doc.addField(MarkerSummaryFields.FEATURE_TYPE_KEY, rs.getString("marker_subtype_key"));
            doc.addField(MarkerSummaryFields.COORDINATE_DISPLAY, rs.getString("coordinate_display"));
            doc.addField(MarkerSummaryFields.LOCATION_DISPLAY, rs.getString("location_display"));

            doc.addField(IndexConstants.MRK_STATUS, rs.getString("status"));
            doc.addField(IndexConstants.MRK_ORGANISM, rs.getString("organism"));
            String markerID = rs.getString("marker_id");
            if (markerID==null) markerID = "";
            doc.addField(IndexConstants.MRK_PRIMARY_ID,markerID);
            
            // sorts
            doc.addField(MarkerSummaryFields.BY_SYMBOL,rs.getInt("by_symbol"));
            doc.addField(MarkerSummaryFields.BY_LOCATION,rs.getInt("by_location"));
            
            // Parse the 1->N marker relationships here
            this.addAllFromLookup(doc,IndexConstants.REF_KEY,mrkKey,referenceToMarkers);
            this.addAllFromLookup(doc,IndexConstants.MRK_ID,mrkKey,idToMarkers);
            this.addAllFromLookup(doc,IndexConstants.MRK_TERM_ID,mrkKey,termToMarkersID);
            

            //logger.info("marker="+mrkKey);
            if(termToMarkers.containsKey(mrkKey))
            {
            	//logger.info("has terms");
            	for(MarkerTerm mt : termToMarkers.get(mrkKey))
            	{
            		String termField = "goTerm";
            		if(GO_PROCESS.equals(mt.vocab)) termField = MarkerSummaryFields.GO_PROCESS_TERM;
            		else if(GO_FUNCTION.equals(mt.vocab)) termField = MarkerSummaryFields.GO_FUNCTION_TERM;
            		else if(GO_COMPONENT.equals(mt.vocab)) termField = MarkerSummaryFields.GO_COMPONENT_TERM;
            		else if(INTERPRO_VOCAB.equals(mt.vocab)) termField = MarkerSummaryFields.INTERPRO_TERM;
            		
            		//logger.info("field="+field+",mtvocab="+mt.vocab+",term="+mt.term);
            		this.addFieldNoDup(doc,termField,mt.term);
            		
            		// add ancestors and synonyms for this term
            		this.addAllFromLookupNoDups(doc,termField,mt.termId,this.ancestorTerms);
            		this.addAllFromLookupNoDups(doc,termField,mt.termId,this.termSynonyms);
            		
            		// if we have ancestors, add their ids, and ancestor synonyms
            		if(this.ancestorIds.containsKey(mt.termId))
            		{
            			for(String ancestorId : this.ancestorIds.get(mt.termId))
            			{
            				doc.addField(IndexConstants.MRK_TERM_ID,ancestorId);
                    		this.addAllFromLookupNoDups(doc,termField,ancestorId,this.termSynonyms);
            			}
            			
            		}
            	}
            	this.resetDupTracking();
            }
            
            /*
             * Marker Location data
             */
            if(locationMap.containsKey(mrkKeyInt))
            {
            	MarkerLocation ml = locationMap.get(mrkKeyInt);
            	// add any location data for this marker
            	if(ml.chromosome!=null) doc.addField(IndexConstants.CHROMOSOME, ml.chromosome);
            	if(ml.startCoordinate>0) doc.addField(IndexConstants.START_COORD, ml.startCoordinate);
            	if(ml.endCoordinate>0) doc.addField(IndexConstants.END_COORD, ml.endCoordinate);
            	if(ml.cmOffset>0.0) doc.addField(IndexConstants.CM_OFFSET, ml.cmOffset);
            	if(ml.strand!=null) doc.addField(IndexConstants.STRAND,ml.strand);
            }
            
            /*
             * Marker nomen
             */
            if(nomenMap.containsKey(mrkKeyInt))
            {
            	for(MarkerNomen mn : nomenMap.get(mrkKeyInt))
            	{
            		float boost = SolrUtils.boost(nomenKeyList,mn.termType);
            		doc.addField(mapNomenField(mn.termType),mn.term,boost);
            	}
            }
            
            /*
             * Allele phenotypes
             */
            // add the phenotype notes
            this.addAllFromLookup(doc,MarkerSummaryFields.PHENO_TEXT,mrkKey,alleleNotesMap);
            this.addAllFromLookup(doc,MarkerSummaryFields.PHENO_ID,mrkKey,alleleTermIdMap);
            this.addAllFromLookup(doc,MarkerSummaryFields.PHENO_TEXT,mrkKey,alleleTermMap);
            
            docs.add(doc);
            
            if (docs.size() > 1000) 
            {
                writeDocs(docs);
                docs = new ArrayList<SolrInputDocument>();
            }
        }
        if(docs.size()>0) server.add(docs);
        server.commit();
    }
    
    private Map<Integer,MarkerLocation> getMarkerLocations(int start,int end) throws Exception
    {
    	logger.info("building map of marker_keys -> marker locations");
    	String locationQuery="select ml.* " +
    			"from marker_location ml " +
    			"where ml.marker_key > "+start+" and ml.marker_key <= "+end+" ";
    	Map<Integer,MarkerLocation> locationMap = new HashMap<Integer,MarkerLocation>();
    	ResultSet rs = ex.executeProto(locationQuery);
    	while(rs.next())
    	{
    		Integer mrkKey = rs.getInt("marker_key");
    		String chromosome = rs.getString("chromosome");
    		Integer startCoord = rs.getInt("start_coordinate");
    		Integer endCoord = rs.getInt("end_coordinate");
    		Double cmOffset = rs.getDouble("cm_offset");
    		String strand = rs.getString("strand");
    		
    		if(!locationMap.containsKey(mrkKey)) locationMap.put(mrkKey,new MarkerLocation());
    		MarkerLocation ml = locationMap.get(mrkKey);
    		
    		// set any non-null fields from this location row
    		if(chromosome!=null) ml.chromosome=chromosome;
    		if(startCoord>0) ml.startCoordinate=startCoord;
    		if(endCoord>0) ml.endCoordinate=endCoord;
    		if(cmOffset>0.0) ml.cmOffset=cmOffset;
    		if(strand!=null) ml.strand=strand;
    	}
    	logger.info("done building map of marker_keys -> marker locations");
    	return locationMap;
    }
    
    private Map<Integer,List<MarkerNomen>> getMarkerNomen(int start,int end) throws Exception
    {
    	logger.info("building map of marker_keys -> marker nomen");
    	String mrkNomenQuery = "select marker_key, nomen, term_type " +
    			"from tmp_marker_nomen mn " +
    			"where mn.marker_key > "+start+" and mn.marker_key <= "+end;

    	Map<Integer,List<MarkerNomen>> nomenMap = new HashMap<Integer,List<MarkerNomen>>();
    	ResultSet rs = ex.executeProto(mrkNomenQuery);
    	while(rs.next())
    	{
    		Integer mrkKey = rs.getInt("marker_key");
    		
    		MarkerNomen mn = new MarkerNomen();
    		mn.term = rs.getString("nomen");
    		mn.termType = rs.getString("term_type");
    		
    		if(!nomenMap.containsKey(mrkKey)) nomenMap.put(mrkKey,new ArrayList<MarkerNomen>(Arrays.asList(mn)));
    		else nomenMap.get(mrkKey).add(mn);
    	}
    	logger.info("done building map of marker_keys -> marker nomen");
    	return nomenMap;
    }
    
    public Map<String,Set<String>> getAlleleNotesMap(int start,int end) throws Exception
    {
    	// get all phenotype notes for alleles
    	String phenoNotesSQL="select marker_key, note\r\n" + 
    			"from tmp_allele_note "+
    			"where marker_key > "+start+" and marker_key <= "+end+" ";
    	return this.populateLookup(phenoNotesSQL,"marker_key","note","marker_key->annotation notes");
    }
    public Map<String,Set<String>> getAlleleTermIdsMap(int start,int end) throws Exception
    {
    	// get the direct MP ID associations
	    String mpIdsSQL="select mpt.marker_key, mpt.term_id " + 
	    		"from tmp_allele_mp_term mpt "+
				"where mpt.marker_key > "+start+" and mpt.marker_key <= "+end+" ";
		Map<String,Set<String>> allelePhenoIdMap = this.populateLookup(mpIdsSQL,"marker_key","term_id","allele_key->MP Ids");
		
		// add the parent IDs
		String mpAncIdsSQL="select mpt.marker_key, tas.ancestor_primary_id " + 
				"from tmp_allele_mp_term mpt join " +
				"	term t on t.primary_id=mpt.term_id join " + 
				"	term_ancestor_simple tas on tas.term_key=t.term_key "+
				"where mpt.marker_key > "+start+" and mpt.marker_key <= "+end+" ";
		allelePhenoIdMap = this.populateLookup(mpAncIdsSQL,"marker_key","ancestor_primary_id","marker_key->ancestor IDs",allelePhenoIdMap);
		

		// add OMIM IDs
		String omimIdSql="select aot.marker_key,aot.term_id " + 
				"from tmp_allele_omim_term aot " + 
				"where aot.marker_key > "+start+" and aot.marker_key <= "+end+" ";
		allelePhenoIdMap = this.populateLookup(omimIdSql,"marker_key","term_id","marker_key->OMIM IDs",allelePhenoIdMap);
		
		// add the alt IDs for all parents and MP and OMIM terms
		String altIdSQL="select at.marker_key, ti.acc_id " + 
				"from tmp_allele_term at join " + 
				"	term_ancestor_simple tas on tas.term_key=at.term_key join " + 
				"	term anc_t on anc_t.primary_id=tas.ancestor_primary_id join " + 
				"	term_id ti on ti.term_key=anc_t.term_key " + 
				"where at.marker_key > "+start+" and at.marker_key <= "+end+" "+
				"UNION " + 
				"select at.marker_key,ti.acc_id " + 
				"from tmp_allele_term at join " + 
				"	term_id ti on ti.term_key=at.term_key "+
				"where at.marker_key > "+start+" and at.marker_key <= "+end+" ";
		allelePhenoIdMap = this.populateLookup(altIdSQL,"marker_key","acc_id","marker_key->alt IDs",allelePhenoIdMap);
		
		
		return allelePhenoIdMap;
    }
    public Map<String,Set<String>> getAlleleTermsMap(int start,int end) throws Exception
    {
    	// get the direct MP Term associations
	    String mpTermsSQL="select mpt.marker_key, mpt.term\r\n" + 
	    		"from tmp_allele_mp_term mpt "+
				"where mpt.marker_key > "+start+" and mpt.marker_key <= "+end+" ";
		Map<String,Set<String>> allelePhenoTermMap = this.populateLookup(mpTermsSQL,"marker_key","term","marker_key->MP terms");
		
		// add OMIM IDs
		String omimTermSql="select aot.marker_key,aot.term " + 
				"from tmp_allele_omim_term aot "+
				"where aot.marker_key > "+start+" and aot.marker_key <= "+end+" ";
		allelePhenoTermMap = this.populateLookup(omimTermSql,"marker_key","term","marker_key->OMIM IDs",allelePhenoTermMap);
				
		// add the parent terms
		String mpAncTermsSQL="select mpt.marker_key, tas.ancestor_term\r\n" + 
				"from tmp_allele_mp_term mpt join "+
				"	term t on t.primary_id=mpt.term_id join " + 
				"	term_ancestor_simple tas on tas.term_key=t.term_key "+
				"where mpt.marker_key > "+start+" and mpt.marker_key <= "+end+" ";
		allelePhenoTermMap = this.populateLookup(mpAncTermsSQL,"marker_key","ancestor_term","marker_key->ancestor IDs",allelePhenoTermMap);
		
		// add the synonyms for all parents and MP and OMIM terms
		String mpSynonymSQL="select at.marker_key, ti.synonym " + 
				"from tmp_allele_term at join " + 
				"	term_ancestor_simple tas on tas.term_key=at.term_key join " + 
				"	term anc_t on anc_t.primary_id=tas.ancestor_primary_id join " + 
				"	term_synonym ti on ti.term_key=anc_t.term_key " + 
				"where at.marker_key > "+start+" and at.marker_key <= "+end+" "+
				"UNION " + 
				"select at.marker_key,ti.synonym " + 
				"from tmp_allele_term at join " + 
				"	term_synonym ti on ti.term_key=at.term_key "+
				"where at.marker_key > "+start+" and at.marker_key <= "+end+" ";
		allelePhenoTermMap = this.populateLookup(mpSynonymSQL,"marker_key","synonym","marker_key->alt IDs",allelePhenoTermMap);
		
		return allelePhenoTermMap;
    }
    
    
    private Map<String,List<MarkerTerm>> getMarkerTerms(int start,int end) throws Exception
    {
        // Get marker -> vocab relationships, by marker key
        String markerToTermSQL = "select distinct m.marker_key, a.term, a.annotation_type, a.term_id,t.display_vocab_name " +
        		"from marker_to_annotation m, " +
        		"	annotation a join " +
        		"	term t on t.primary_id=a.term_id " +
        		"where m.marker_key > " + start + " and m.marker_key <= "+ end +" "+
        			"and m.annotation_key = a.annotation_key " +
        			"and a.annotation_type in ('GO/Marker','InterPro/Marker') " +
        			"and a.term not in ('cellular_component','biological_process','molecular_function') ";
        Map <String,List<MarkerTerm>> tempMap = new HashMap<String,List<MarkerTerm>>();
        
        ResultSet rs = ex.executeProto(markerToTermSQL); 
        while (rs.next()) 
        {
            String key = rs.getString("marker_key");
            String term = rs.getString("term");
            String type = rs.getString("annotation_type");
            String vocab = rs.getString("display_vocab_name");
            String termId = rs.getString("term_id");
            MarkerTerm mt = translateVocab(term,termId,type,vocab);
            if(mt!=null)
            {
	            if (tempMap.containsKey(key)) 
	            {
	                tempMap.get(key).add(mt);
	            }
	            else 
	            {
	                tempMap.put(key, new ArrayList<MarkerTerm>(Arrays.asList(mt)));
	            }
            }
        }
        return tempMap;
    }
    
    /*
     * creates: 
     * 	tmp_allele_mp_term,
     * 	tmp_allele_omim_term,
     * 	tmp_allele_term,
     * 	tmp_allele_note
     *  tmp_marker_nomen
     */
    private void tempTables() throws Exception
    {
    	// create marker to phenotype/disease terms/IDs
    	logger.info("creating temp table of marker to 'simple' genotypes");
    	String simpleGenotypeQuery="select distinct marker_key,genotype_key " +
    			"into temp tmp_marker_genotype " +
    			"from hdp_annotation ha " +
    			"where ha.genotype_type!='complex' ";
    	this.ex.executeVoid(simpleGenotypeQuery);
    	createTempIndex("tmp_marker_genotype","marker_key");
    	createTempIndex("tmp_marker_genotype","genotype_key");
    	
    	logger.info("creating temp table of marker_key to mp abnormal term");
    	String mpAbnormalQuery="select tmg.marker_key, mpt.term,mpt.term_id,mpt.mp_term_key " + 
    			"into temp tmp_allele_mp_term "+
    			"from tmp_marker_genotype tmg join "+
	    			"mp_system ms on ms.genotype_key=tmg.genotype_key join " + 
	    			"mp_term mpt on mpt.mp_system_key=ms.mp_system_key join " +
	    			"mp_annot mpa on mpa.mp_term_key=mpt.mp_term_key " + 
    			"where mpa.call=1 ";
    	this.ex.executeVoid(mpAbnormalQuery);
    	createTempIndex("tmp_allele_mp_term","marker_key");
    	createTempIndex("tmp_allele_mp_term","term_id");
    	createTempIndex("tmp_allele_mp_term","mp_term_key");
    	logger.info("done creating temp table of marker_key to mp abnormal term");
    	
    	logger.info("creating temp table of marker_key to OMIM abnormal term");
    	String omimAbnormalQuery="select tmg.marker_key,gd.term,gd.term_id " + 
    			"into temp tmp_allele_omim_term "+
				"from tmp_marker_genotype tmg join " +
					"genotype_disease gd on gd.genotype_key=tmg.genotype_key "+
				"where gd.is_not=0 ";
    	this.ex.executeVoid(omimAbnormalQuery);
    	createTempIndex("tmp_allele_omim_term","marker_key");
    	createTempIndex("tmp_allele_omim_term","term_id");
    	logger.info("done creating temp table of marker_key to OMIM abnormal term");
    	    	
    	// create allele to MP+OMIM term keys
    	logger.info("creating temp table of marker_key to term_key");
    	String alleleTermsQuery="select mpt.marker_key,t.term_key " +
    			"into temp tmp_allele_term " + 
				"from tmp_allele_mp_term mpt join " + 
				"	term t on t.primary_id=mpt.term_id "+
					"UNION " +
				"select aot.marker_key,t.term_key " + 
				"from tmp_allele_omim_term aot join " + 
				"	term t on t.primary_id=aot.term_id ";
    	this.ex.executeVoid(alleleTermsQuery);
    	createTempIndex("tmp_allele_term","marker_key");
    	logger.info("done creating temp table of marker_key to term_key");

    	// create allele to mp notes
    	logger.info("creating temp table of marker_key to mp annotation note");
    	String alleleNotesQuery = "select distinct mpt.marker_key, replace(mpan.note,'Background Sensitivity: ','') note " + 
    			"into temp tmp_allele_note "+
    			"from tmp_allele_mp_term mpt join " + 
    			"	mp_reference mpr on mpr.mp_term_key=mpt.mp_term_key join " + 
    			"	mp_annotation_note  mpan on mpan.mp_reference_key=mpr.mp_reference_key ";
    	this.ex.executeVoid(alleleNotesQuery);
    	logger.info("adding General Allele notes to allele notes temp table");
    	String generalNotesQuery = "insert into tmp_allele_note (marker_key,note) " +
    			"select mta.marker_key, mn.note\r\n" + 
    			"from marker_to_allele mta join " +
    			"allele_note mn on mn.allele_key=mta.allele_key " +
    			"where mn.note_type in ('General') ";
    	this.ex.executeVoid(generalNotesQuery);
    	logger.info("adding QTL text notes to allele notes temp table");
    	String qtlNotesQuery = "insert into tmp_allele_note (marker_key,note) " +
    			"select mqtl.marker_key, mqtl.note\r\n" + 
    			"from marker_qtl_experiments mqtl " + 
    			"where mqtl.note_type='TEXT-QTL' ";
    	this.ex.executeVoid(qtlNotesQuery);
    	createTempIndex("tmp_allele_note","marker_key");
    	logger.info("done creating temp table of marker_key to mp annotation note");
    	
    	// create marker to nomenclature
    	logger.info("creating temp table of marker_key to nomenclature");
    	String mrkNomenQuery = "select msn.marker_key, msn.term nomen, msn.term_type " +
    			"into temp tmp_marker_nomen " +
    			"from marker_searchable_nomenclature msn " +
    			"where msn.term_type in ('human name','human synonym','human symbol'," +
        				"'current symbol','current name','old symbol','synonym','related synonym','old name'," +
        				"'rat symbol','rat synonym','cattle symbol','chicken symbol','dog symbol'," +
        				"'rhesus macaque symbol','zebrafish symbol' ) ";
    	this.ex.executeVoid(mrkNomenQuery);
    	mrkNomenQuery = "insert into tmp_marker_nomen (marker_key,nomen,term_type) " +
    			"select mta.marker_key, a.symbol nomen, 'alleleSymbol' term_type " +
    			"from marker_to_allele mta join " +
    			"allele a on a.allele_key=mta.allele_key ";
    	this.ex.executeVoid(mrkNomenQuery);
    	mrkNomenQuery = "insert into tmp_marker_nomen (marker_key,nomen,term_type) " +
    			"select mta.marker_key, a.name nomen, 'alleleName' term_type " +
    			"from marker_to_allele mta join " +
    			"allele a on a.allele_key=mta.allele_key ";
    	this.ex.executeVoid(mrkNomenQuery);
//    	mrkNomenQuery = "insert into tmp_marker_nomen (marker_key,nomen,term_type) " +
//    			"select mta.marker_key, syn.synonym nomen, 'alleleSynoynm' term_type " +
//    			"from marker_to_allele mta join " +
//    			"allele_synonym syn on syn.allele_key=mta.allele_key ";
//    	this.ex.executeVoid(mrkNomenQuery);
    	createTempIndex("tmp_marker_nomen","marker_key");
    	logger.info("done creating temp table of tmp_marker_nomen marker_key to nomenclature");
    }
    
    protected MarkerTerm translateVocab(String term, String termId,String type,String vocab) 
    {
    	MarkerTerm mt = new MarkerTerm();
    	mt.term = term;
    	mt.termId = termId;
        if (type.equals("GO/Marker"))
        {
        	if(vocab.equals(GO_PROCESS)) mt.vocab = GO_PROCESS;
        	else if(vocab.equals(GO_FUNCTION)) mt.vocab = GO_FUNCTION;
        	else if(vocab.equals(GO_COMPONENT)) mt.vocab = GO_COMPONENT;
        	else return null;
        }
        else if (type.equals("InterPro/Marker"))   mt.vocab = INTERPRO_VOCAB;
        //else if (vocab.equals("PIRSF/Marker"))  return "Protein Family: " + value;
        //else if (vocab.equals("OMIM/Human Marker"))  return "Disease Model: " + value;
        else
        {
        	return null;
        }
        
        return mt;
    }
    
    private String mapNomenField(String termType)
    {
    	if(MarkerSummaryFields.NOMEN_FIELDS.containsKey(termType)) return MarkerSummaryFields.NOMEN_FIELDS.get(termType);

    	return termType;
    }
    
    // helper class for storing marker location info
    public class MarkerLocation
    {
    	String chromosome=null;
    	Integer startCoordinate=0;
    	Integer endCoordinate=0;
    	Double cmOffset=0.0;
    	String strand=null;
    }
    
    // helper class for marker terms
    public class MarkerTerm
    {
    	String term;
    	String termId;
    	String vocab;
    }
    
    // helper class for storing marker nomen info
    public class MarkerNomen
    {
    	String term;
    	String termType;
    }
}
