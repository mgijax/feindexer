package org.jax.mgi.indexer;

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
 * AlleleIndexerSQL
 * @author kstone
 * 
 * This class is responsible for populating the allele index. 
 * 
 * Note: Refactored during 5.x development
 */

public class AlleleIndexerSQL extends Indexer 
{
	// Constants
	private static final String CELL_LINE = "Cell Line";
	
	private Map<String,Integer> diseaseSorts = new HashMap<String,Integer>();
   
    public AlleleIndexerSQL () {
        super("index.url.allele");
    }

    /**
     * This is the method that is responsible for populating the index for cre.  It has one sub 
     * object relationship, and an extremely large main query.
     */
    
    public void index() throws Exception
    {
    	this.tempTables();
    	
    	this.initSorts();
    	
        // Parse the results, again this is a very large query, but fairly
        // flat and straightforward. 
        ResultSet rs = ex.executeProto("select max(allele_key) as max_allele_key from allele");
    	rs.next();
    	
    	Integer start = 0;
        Integer end = rs.getInt("max_allele_key");
    	int chunkSize = 50000;
        
        int modValue = end.intValue() / chunkSize;
        // Perform the chunking

        logger.info("Getting all disease and MP annotations");
        
        for (int i = 0; i <= modValue; i++) 
        {
            start = i * chunkSize;
            end = start + chunkSize;
            
            processAlleles(start,end);
        }
        
        // clean up class variables
        this.diseaseSorts=null;
    }
    
    private void initSorts() throws Exception
    {
    	ResultSet rs = ex.executeProto("select distinct disease from disease ");
    	Set<String> unorderedDiseases = new HashSet<String>();
    	while(rs.next())
    	{
    		unorderedDiseases.add(rs.getString("disease"));
    	}
    	ArrayList<String> orderedDiseases = new ArrayList<String>(unorderedDiseases);
    	unorderedDiseases=null;
        Collections.sort(orderedDiseases, new SmartAlphaComparator());
        
        int count=0;
        for(String disease : orderedDiseases)
        {
        	count+=1;
        	this.diseaseSorts.put(disease,count);
        }
    }
    
    /*
     * All queries in here should filter by start and end allele_key
     */
    private void processAlleles(int startKey,int endKey) throws Exception
    {
    	/*
    	 *  gather lookups
    	 */
        logger.info("Getting all alleles and other related info from keys "+startKey+" to "+endKey);
    	// phenotypes
    	Map<String,Set<String>> alleleNotesMap = getAlleleNotesMap(startKey,endKey);
    	Map<String,Set<String>> alleleTermMap = getAlleleTermsMap(startKey,endKey);
    	Map<String,Set<String>> alleleTermIdMap = getAlleleTermIdsMap(startKey,endKey);
    	
	// mutation involves markers
	Map<String,Set<String>> mutationInvolvesMap = getMutationInvolvesMap(
		startKey, endKey);

    	// nomenclature
    	Map<String,Set<String>> mrkNomenMap = getMarkerNomenMap(startKey, endKey);
    	Map<String,Set<String>> allNomenMap = getAlleleNomenMap(startKey, endKey);
    	
    	// references
    	Map<String,Set<String>> refKeysMap = getRefKeys(startKey, endKey);
    	Map<String,Set<String>> jnumMap = getJnumIds(startKey, endKey);
    	Set<Integer> alleleKeysWithOMIM = getAllelesWithOMIM(startKey, endKey);
    	
    	// locations
    	Map<Integer,AlleleLocation> locationMap = getAlleleLocations(startKey,endKey);
    	
    	// accession IDs
        Map<String,Set<String>> allIdMap = this.getAlleleIdsMap(startKey,endKey);
        
        // sorts
        Map<Integer,Integer> diseaseSortMap = this.getAlleleDiseaseSortMap(startKey,endKey);
        
    	// The main sql for allele
        ResultSet rs = ex.executeProto("select m.marker_key,m.primary_id marker_id, " +
        		"a.allele_key, " +
        		"a.symbol, a.name, " +
        		"a.allele_type,a.allele_subtype, " +
        		"a.collection, is_wild_type, " +
        		"a.transmission_type, " +
        		"asn.by_symbol, " +
        		"asn.by_chromosome, " +
        		"asn.by_allele_type " +
        		"from allele a join " +
        		"marker_to_allele mta on a.allele_key=mta.allele_key join " +
        		"marker m on m.marker_key=mta.marker_key join " +
        		"allele_sequence_num asn on asn.allele_key=a.allele_key "+
        		"where mta.allele_key > "+startKey+" and mta.allele_key <= "+endKey+" ");
        
        logger.info("Parsing them");
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        while (rs.next()) 
        {
            SolrInputDocument doc = new SolrInputDocument();
            Integer allKey=rs.getInt("allele_key");
            String allKeyString=allKey.toString();
            String mrkKeyString = rs.getString("marker_key");
            int byTransmission = CELL_LINE.equalsIgnoreCase(rs.getString("transmission_type")) ? 1 : 0;
            int byDisease = diseaseSortMap.containsKey(allKey) ? diseaseSortMap.get(allKey) : 9999999;
            
            /*
             * Add marker fields
             */
            doc.addField(IndexConstants.MRK_KEY,mrkKeyString);
            doc.addField(IndexConstants.MRK_ID,rs.getString("marker_id"));

	    /*
	     * Add mutation involves markers:
	     * 1. to standard MRK_ID field so the allele will be returned
	     *    for either its traditional marker ID or for any markers
	     *    with a mutation involves relationship.
	     * 2. to the new ALL_MI_MARKER_IDS field so we can return just the
	     *    alleles related to a marker by a mutation involves
	     *    relationship.
	     */
            this.addAllFromLookup(doc,IndexConstants.MRK_ID, allKeyString,
		mutationInvolvesMap);
            this.addAllFromLookup(doc,IndexConstants.ALL_MI_MARKER_IDS,
		allKeyString, mutationInvolvesMap);

            /*
             * Add allele fields
             */
            doc.addField(IndexConstants.ALL_KEY, allKeyString);
            doc.addField(IndexConstants.ALL_SYMBOL, rs.getString("symbol"));
            doc.addField(IndexConstants.ALL_NAME, rs.getString("name"));
            doc.addField(IndexConstants.ALL_TYPE, rs.getString("allele_type"));
            doc.addField(IndexConstants.ALL_IS_WILD_TYPE, rs.getString("is_wild_type"));
            doc.addField(IndexConstants.ALL_COLLECTION, rs.getString("collection"));
            doc.addField(IndexConstants.ALL_IS_CELLLINE, byTransmission);
            
            String subTypes = rs.getString("allele_subtype");
            if(notEmpty(subTypes))
            {
            	for(String subType : subTypes.split(", "))
            	{
            		doc.addField(IndexConstants.ALL_SUBTYPE,subType);
            	}
            }
            
            /*
             * Allele sorts
             */
            doc.addField(IndexConstants.ALL_TRANSMISSION_SORT,byTransmission);
            doc.addField(IndexConstants.ALL_SYMBOL_SORT,rs.getInt("by_symbol"));
            doc.addField(IndexConstants.ALL_TYPE_SORT,rs.getInt("by_allele_type"));
            doc.addField(IndexConstants.ALL_CHR_SORT,rs.getInt("by_chromosome"));
            doc.addField(IndexConstants.ALL_DISEASE_SORT,byDisease);
            
            /*
             * Phenotype data
             */
            // add the phenotype notes
            this.addAllFromLookup(doc,IndexConstants.ALL_PHENO_TEXT,allKeyString,alleleNotesMap);
            this.addAllFromLookup(doc,IndexConstants.ALL_PHENO_ID,allKeyString,alleleTermIdMap);
            this.addAllFromLookup(doc,IndexConstants.ALL_PHENO_TEXT,allKeyString,alleleTermMap);
            
            /*
             * nomenclature data
             */
            this.addAllFromLookup(doc,IndexConstants.ALL_NOMEN,mrkKeyString,mrkNomenMap);
            this.addAllFromLookup(doc,IndexConstants.ALL_NOMEN,allKeyString,allNomenMap);
            
            /*
             * References data
             */
            this.addAllFromLookup(doc,IndexConstants.REF_KEY,allKeyString,refKeysMap);
            this.addAllFromLookup(doc,IndexConstants.JNUM_ID,allKeyString,jnumMap);
            int hasOMIM = alleleKeysWithOMIM.contains(allKey) ? 1 : 0;
            doc.addField(IndexConstants.ALL_HAS_OMIM,hasOMIM);
            
            /*
             * Allele Location data
             */
            if(locationMap.containsKey(allKey))
            {
            	AlleleLocation al = locationMap.get(allKey);
            	// add any location data for this allele
            	if(al.chromosome!=null) doc.addField(IndexConstants.CHROMOSOME, al.chromosome);
            	if(al.startCoordinate>0) doc.addField(IndexConstants.START_COORD, al.startCoordinate);
            	if(al.endCoordinate>0) doc.addField(IndexConstants.END_COORD, al.endCoordinate);
            	if(al.cmOffset>0.0) doc.addField(IndexConstants.CM_OFFSET, al.cmOffset);
            	if(al.cytogeneticOffset!=null) doc.addField(IndexConstants.CYTOGENETIC_OFFSET, al.cytogeneticOffset);
            }

            
            // add allele accession IDs
            this.addAllFromLookup(doc,IndexConstants.ALL_ID,allKeyString,allIdMap);
         
            docs.add(doc);   
            if (docs.size() > 1000) {
                writeDocs(docs);
                docs = new ArrayList<SolrInputDocument>();
            }
        }
        logger.info("Adding final stack of the documents to Solr");
        server.add(docs);
        server.commit();    
    }
    
    
    /*
     * Lookup access functions
     */
    public Map<String,Set<String>> getMutationInvolvesMap(int start, int end)
	    throws Exception
    {
	String mutationInvolvesSQL = "select allele_key, related_marker_id "
		+ "from allele_related_marker "
		+ "where relationship_category = 'mutation_involves' "
		+ "  and allele_key > " + start
		+ "  and allele_key <= " + end;

	return this.populateLookup(mutationInvolvesSQL, "allele_key",
		"related_marker_id",
		"allele_key->mutation involves markers");
    }

    public Map<String,Set<String>> getAlleleNotesMap(int start,int end) throws Exception
    {
    	// get all phenotype notes for alleles
    	String phenoNotesSQL="select allele_key, note\r\n" + 
    			"from tmp_allele_note "+
    			"where allele_key > "+start+" and allele_key <= "+end+" ";
    	return this.populateLookup(phenoNotesSQL,"allele_key","note","allele_key->annotation notes");
    }
    public Map<String,Set<String>> getAlleleTermIdsMap(int start,int end) throws Exception
    {
    	// get the direct MP ID associations
	    String mpIdsSQL="select mpt.allele_key, mpt.term_id " + 
	    		"from tmp_allele_mp_term mpt "+
				"where mpt.allele_key > "+start+" and mpt.allele_key <= "+end+" ";
		Map<String,Set<String>> allelePhenoIdMap = this.populateLookup(mpIdsSQL,"allele_key","term_id","allele_key->MP Ids");
		
		// add the parent IDs
		String mpAncIdsSQL="select mpt.allele_key, tas.ancestor_primary_id " + 
				"from tmp_allele_mp_term mpt join " +
				"	term t on t.primary_id=mpt.term_id join " + 
				"	term_ancestor_simple tas on tas.term_key=t.term_key "+
				"where mpt.allele_key > "+start+" and mpt.allele_key <= "+end+" ";
		allelePhenoIdMap = this.populateLookup(mpAncIdsSQL,"allele_key","ancestor_primary_id","allele_key->ancestor IDs",allelePhenoIdMap);
		

		// add OMIM IDs
		String omimIdSql="select aot.allele_key,aot.term_id " + 
				"from tmp_allele_omim_term aot " + 
				"where aot.allele_key > "+start+" and aot.allele_key <= "+end+" ";
		allelePhenoIdMap = this.populateLookup(omimIdSql,"allele_key","term_id","allele_key->OMIM IDs",allelePhenoIdMap);
		
		// add the alt IDs for all parents and MP and OMIM terms
		String altIdSQL="select at.allele_key, ti.acc_id " + 
				"from tmp_allele_term at join " + 
				"	term_ancestor_simple tas on tas.term_key=at.term_key join " + 
				"	term anc_t on anc_t.primary_id=tas.ancestor_primary_id join " + 
				"	term_id ti on ti.term_key=anc_t.term_key " + 
				"where at.allele_key > "+start+" and at.allele_key <= "+end+" "+
				"UNION " + 
				"select at.allele_key,ti.acc_id " + 
				"from tmp_allele_term at join " + 
				"	term_id ti on ti.term_key=at.term_key "+
				"where at.allele_key > "+start+" and at.allele_key <= "+end+" ";
		allelePhenoIdMap = this.populateLookup(altIdSQL,"allele_key","acc_id","allele_key->alt IDs",allelePhenoIdMap);
		
		
		return allelePhenoIdMap;
    }
    public Map<String,Set<String>> getAlleleTermsMap(int start,int end) throws Exception
    {
    	// get the direct MP Term associations
	    String mpTermsSQL="select mpt.allele_key, mpt.term\r\n" + 
	    		"from tmp_allele_mp_term mpt "+
				"where mpt.allele_key > "+start+" and mpt.allele_key <= "+end+" ";
		Map<String,Set<String>> allelePhenoTermMap = this.populateLookup(mpTermsSQL,"allele_key","term","allele_key->MP terms");
		
		// add OMIM IDs
		String omimTermSql="select aot.allele_key,aot.term " + 
				"from tmp_allele_omim_term aot "+
				"where aot.allele_key > "+start+" and aot.allele_key <= "+end+" ";
		allelePhenoTermMap = this.populateLookup(omimTermSql,"allele_key","term","allele_key->OMIM IDs",allelePhenoTermMap);
				
		// add the parent terms
		String mpAncTermsSQL="select mpt.allele_key, tas.ancestor_term\r\n" + 
				"from tmp_allele_mp_term mpt join "+
				"	term t on t.primary_id=mpt.term_id join " + 
				"	term_ancestor_simple tas on tas.term_key=t.term_key "+
				"where mpt.allele_key > "+start+" and mpt.allele_key <= "+end+" ";
		allelePhenoTermMap = this.populateLookup(mpAncTermsSQL,"allele_key","ancestor_term","allele_key->ancestor IDs",allelePhenoTermMap);
		
		// add the synonyms for all parents and MP and OMIM terms
		String mpSynonymSQL="select at.allele_key, ti.synonym " + 
				"from tmp_allele_term at join " + 
				"	term_ancestor_simple tas on tas.term_key=at.term_key join " + 
				"	term anc_t on anc_t.primary_id=tas.ancestor_primary_id join " + 
				"	term_synonym ti on ti.term_key=anc_t.term_key " + 
				"where at.allele_key > "+start+" and at.allele_key <= "+end+" "+
				"UNION " + 
				"select at.allele_key,ti.synonym " + 
				"from tmp_allele_term at join " + 
				"	term_synonym ti on ti.term_key=at.term_key "+
				"where at.allele_key > "+start+" and at.allele_key <= "+end+" ";
		allelePhenoTermMap = this.populateLookup(mpSynonymSQL,"allele_key","synonym","allele_key->alt IDs",allelePhenoTermMap);
		
		return allelePhenoTermMap;
    }
    public Map<String,Set<String>> getAlleleIdsMap(int start,int end) throws Exception
    {
        String allToIDSQL = "select allele_key, acc_id from allele_id "+
        		 "where allele_key > "+start+" and allele_key <= "+end+" ";
        Map<String,Set<String>> allIdMap = this.populateLookup(allToIDSQL, "allele_key", "acc_id","allele_keys -> allele accession IDs");
        return allIdMap;
    }
    
    public Map<String,Set<String>> getMarkerNomenMap(int start, int end) throws Exception
    {
    	String mrkNomenQuery = "WITH " +
    			"allele_markers AS (select distinct marker_key " +
    				"from marker_to_allele mta " +
    				"where allele_key > "+start+" and allele_key <= "+end+" ) "+
    			"select msn.marker_key, msn.term nomen " +
    			"from marker_searchable_nomenclature msn join " +
    			"allele_markers am on am.marker_key=msn.marker_key " +
    			"where msn.term_type in ('human name','human synonym','human symbol'," +
        				"'current symbol','current name','old symbol','synonym','related synonym','old name' ) ";
    	Map<String,Set<String>> mrkNomenMap = this.populateLookup(mrkNomenQuery,"marker_key","nomen","marker_keys -> marker nomenclature");
    	return mrkNomenMap;
    }
    public Map<String,Set<String>> getAlleleNomenMap(int start, int end) throws Exception
    {
    	String nomenQuery = "select allele_key, nomen " +
		"from tmp_allele_nomen " +
		 "where allele_key > "+start+" and allele_key <= "+end+" ";
    	Map<String,Set<String>> nomenMap = this.populateLookup(nomenQuery,"allele_key","nomen","allele_keys -> allele nomenclature");
    	return nomenMap;
    }
    
    public Map<Integer,AlleleLocation> getAlleleLocations(int start,int end) throws Exception
    {
    	logger.info("building map of allele_keys -> marker locations");
    	String locationQuery="select allele_key,ml.* " +
    			"from marker_to_allele mta join " +
    			"marker_location ml on ml.marker_key=mta.marker_key " +
    			"where mta.allele_key > "+start+" and mta.allele_key <= "+end+" ";
    	Map<Integer,AlleleLocation> locationMap = new HashMap<Integer,AlleleLocation>();
    	ResultSet rs = ex.executeProto(locationQuery);
    	while(rs.next())
    	{
    		Integer allKey = rs.getInt("allele_key");
    		String chromosome = rs.getString("chromosome");
    		Integer startCoord = rs.getInt("start_coordinate");
    		Integer endCoord = rs.getInt("end_coordinate");
    		Double cmOffset = rs.getDouble("cm_offset");
    		String cytogeneticOffset = rs.getString("cytogenetic_offset");
    		
    		if(!locationMap.containsKey(allKey)) locationMap.put(allKey,new AlleleLocation());
    		AlleleLocation al = locationMap.get(allKey);
    		
    		// set any non-null fields from this location row
    		if(chromosome!=null) al.chromosome=chromosome;
    		if(startCoord>0) al.startCoordinate=startCoord;
    		if(endCoord>0) al.endCoordinate=endCoord;
    		if(cmOffset>0.0) al.cmOffset=cmOffset;
    		if(cytogeneticOffset!=null) al.cytogeneticOffset=cytogeneticOffset;
    	}
    	logger.info("done building map of allele_keys -> marker locations");
    	return locationMap;
    }
    
    public Map<String,Set<String>> getRefKeys(int start, int end) throws Exception
    {
    	String refQuery="select allele_key,reference_key " +
    			"from allele_to_reference "+
    			"where allele_key > "+start+" and allele_key <= "+end+" ";
    	Map<String,Set<String>> refKeyMap = this.populateLookup(refQuery,"allele_key","reference_key","allele_keys -> reference keys");
    	return refKeyMap;
    }
    
    public Map<String,Set<String>> getJnumIds(int start, int end) throws Exception
    {
    	String refQuery="select allele_key,jnum_id " +
    			"from allele_to_reference atr join " +
    			"reference r on r.reference_key=atr.reference_key "+
    			"where allele_key > "+start+" and allele_key <= "+end+" ";
    	Map<String,Set<String>> jnumMap = this.populateLookup(refQuery,"allele_key","jnum_id","allele_keys -> jnum IDs");
    	return jnumMap;
    }
    
    public Set<Integer> getAllelesWithOMIM(int start, int end) throws Exception
    {
    	String omimAllelesQuery="select allele_key " +
    			"from diseasetable_disease dtd " +
    			"where exists (select 1 from diseasetable_disease_cell dtdc " +
    					"where dtdc.diseasetable_disease_key=dtd.diseasetable_disease_key " +
    					"	and dtdc.call='Y') "+
    			"and allele_key > "+start+" and allele_key <= "+end+" ";
    	Set<Integer> alleleKeys = new HashSet<Integer>();
    	ResultSet rs = ex.executeProto(omimAllelesQuery);
    	while(rs.next())
    	{
    		alleleKeys.add(rs.getInt("allele_key"));
    	}
    	return alleleKeys;
    }
    
    public Map<Integer,Integer> getAlleleDiseaseSortMap(int start, int end) throws Exception
    {
    	String alleleDiseaseQuery="select asd.allele_key,asd.disease " +
    			"from allele_summary_disease asd " +
    			"where allele_key > "+start+" and allele_key <= "+end+" ";
    	ResultSet rs = ex.executeProto(alleleDiseaseQuery);
    	Map<Integer,Integer> alleleDiseaseSortMap = new HashMap<Integer,Integer>();
    	while(rs.next())
    	{
    		int allKey = rs.getInt("allele_key");
    		Integer dSort = this.diseaseSorts.get(rs.getString("disease"));
    		if(!alleleDiseaseSortMap.containsKey(allKey) ||
    				alleleDiseaseSortMap.get(allKey) > dSort)
    		{ 
    			alleleDiseaseSortMap.put(allKey,dSort); 
    		}
    	}
    	
    	return alleleDiseaseSortMap;
    }
    
    /*
     * creates: 
     * 	tmp_allele_mp_term,
     * 	tmp_allele_omim_term,
     * 	tmp_allele_term,
     * 	tmp_allele_note
     */
    public void tempTables() throws Exception
    {
    	// create abnormal MP terms
    	logger.info("creating temp table of allele_key to mp abnormal term");
    	String mpAbnormalQuery="select atg.allele_key, mpt.term,mpt.term_id,mpt.mp_term_key " + 
    			"into temp tmp_allele_mp_term "+
    			"from allele_to_genotype atg join \r\n" + 
    			"	mp_system ms on ms.genotype_key=atg.genotype_key join\r\n" + 
    			"	mp_term mpt on mpt.mp_system_key=ms.mp_system_key join " +
    			"	mp_annot mpa on mpa.mp_term_key=mpt.mp_term_key " + 
    			"where mpa.call=1 ";
    	this.ex.executeVoid(mpAbnormalQuery);
    	createTempIndex("tmp_allele_mp_term","allele_key");
    	createTempIndex("tmp_allele_mp_term","term_id");
    	createTempIndex("tmp_allele_mp_term","mp_term_key");
    	logger.info("done creating temp table of allele_key to mp abnormal term");
    	
    	logger.info("creating temp table of allele_key to OMIM abnormal term");
    	String omimAbnormalQuery="select asg.allele_key,gd.term,gd.term_id " + 
    			"into temp tmp_allele_omim_term "+
				"from allele_to_genotype asg join " + 
				"	genotype_disease gd on gd.genotype_key=asg.genotype_key "+
				"where gd.is_not=0 ";
    	this.ex.executeVoid(omimAbnormalQuery);
    	createTempIndex("tmp_allele_omim_term","allele_key");
    	createTempIndex("tmp_allele_omim_term","term_id");
    	logger.info("done creating temp table of allele_key to OMIM abnormal term");
    	    	
    	// create allele to MP+OMIM term keys
    	logger.info("creating temp table of allele_key to term_key");
    	String alleleTermsQuery="select mpt.allele_key,t.term_key " +
    			"into temp tmp_allele_term " + 
				"from tmp_allele_mp_term mpt join " + 
				"	term t on t.primary_id=mpt.term_id "+
					"UNION " +
				"select aot.allele_key,t.term_key " + 
				"from tmp_allele_omim_term aot join " + 
				"	term t on t.primary_id=aot.term_id ";
    	this.ex.executeVoid(alleleTermsQuery);
    	createTempIndex("tmp_allele_term","allele_key");
    	logger.info("done creating temp table of allele_key to term_key");

    	// create allele to mp notes (skip Normal notes and notes for
	// annotations with "normal" qualifiers)
    	logger.info("creating temp table of allele_key to mp annotation note");
    	String alleleNotesQuery = "select distinct mpt.allele_key, replace(mpan.note,'Background Sensitivity: ','') note " + 
    			"into temp tmp_allele_note "+
    			"from tmp_allele_mp_term mpt join " + 
    			"	mp_reference mpr on mpr.mp_term_key=mpt.mp_term_key join " + 
    			"	mp_annotation_note  mpan on (mpan.mp_reference_key=mpr.mp_reference_key " +
			"	and mpan.note_type != 'Normal'" +
			"	and mpan.has_normal_qualifier = 0)" +
			" join mp_annot ma on (mpr.mp_annotation_key = ma.mp_annotation_key and ma.call = 1)";
    	this.ex.executeVoid(alleleNotesQuery);
    	logger.info("adding General Allele notes to allele notes temp table");
    	String generalNotesQuery = "insert into tmp_allele_note (allele_key,note) " +
    			"select mn.allele_key, mn.note\r\n" + 
    			"from allele_note mn " +
    			"where mn.note_type in ('General') ";
    	this.ex.executeVoid(generalNotesQuery);
    	logger.info("adding QTL text notes to allele notes temp table");
    	String qtlNotesQuery = "insert into tmp_allele_note (allele_key,note) " +
    			"select mta.allele_key, mqtl.note " + 
    			"from allele a join " +
    			"marker_to_allele mta on mta.allele_key=a.allele_key join " +
    			"marker_qtl_experiments mqtl on mqtl.marker_key=mta.marker_key " + 
    			"where mqtl.note_type='TEXT-QTL' " +
    			"and a.allele_type='QTL' ";
    	this.ex.executeVoid(qtlNotesQuery);
    	createTempIndex("tmp_allele_note","allele_key");
    	logger.info("done creating temp table of allele_key to mp annotation note");
    	
    	// create allele to nomenclature
    	logger.info("creating temp table of allele_key to nomenclature");
    	String alleleNomenQuery = "select allele_key, synonym nomen " + 
    			"into temp tmp_allele_nomen " +
    			"from allele_synonym ";
    	this.ex.executeVoid(alleleNomenQuery);
    	alleleNomenQuery = "insert into tmp_allele_nomen (allele_key,nomen) " +
    			"select allele_key, name nomen " + 
    			"from allele ";
    	this.ex.executeVoid(alleleNomenQuery);
    	alleleNomenQuery = "insert into tmp_allele_nomen (allele_key,nomen) " +
    			"select allele_key, symbol nomen " +
    			"from allele ";
    	this.ex.executeVoid(alleleNomenQuery);
    	createTempIndex("tmp_allele_nomen","allele_key");
    	logger.info("done creating temp table of allele_key to nomenclature");
    }
    
    // helper class for storing allele location info
    public class AlleleLocation
    {
    	String chromosome=null;
    	Integer startCoordinate=0;
    	Integer endCoordinate=0;
    	Double cmOffset=0.0;
    	String cytogeneticOffset=null;
    }
    private boolean notEmpty(String s)
    {
    	return s!=null && !s.equals("");
    }
}
