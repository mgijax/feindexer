package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.DiseasePortalFields;
import org.jax.mgi.shr.fe.query.SolrLocationTranslator;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;

/**
 * DiseasePortalIndexerSQL
 * @author kstone
 * This index has the primary responsibility of populating the diseasePortal solr index.
 * 
 */

public class DiseasePortalIndexerSQL extends Indexer 
{   
    public DiseasePortalIndexerSQL () 
    { super("index.url.diseasePortal"); }    
    
    /*
     * Constants
     */
    private final String NORMAL_PHENOTYPE = "normal phenotype";
    
    public void index() throws Exception
    {    
    	/*
    	 *  populate numerous simple lookups for use in filling out fields with multiple values
    	 */
    	
    	// ------------- TERM QUERY RELATED LOOKUPS ------------
    	
    	this.setMaxThreads(4); // setting threads lower, because of the high volume of indexed fields
    	
    	logger.info("creating any necessary temp tables");
    	createTempTables();
    	logger.info("done creating temp tables");
    	
    	logger.info("building counts of disease relevant refs to disease ID");
		String diseaseRefCountQuery = "select ha.term_id disease_id, count(distinct trt.reference_key) ref_count " +
				"from hdp_term_to_reference trt, " +
				"hdp_annotation ha " +
				"where ha.term_key=trt.term_key " +
				"and ha.vocab_name='OMIM' "+
				"group by disease_id ";
		ResultSet rs = ex.executeProto(diseaseRefCountQuery);
		Map<String,Integer> diseaseRefCountMap = new HashMap<String,Integer>();
		while(rs.next())
		{
			diseaseRefCountMap.put(rs.getString("disease_id"),rs.getInt("ref_count"));
		}
		logger.info("done building counts of disease relevant refs to disease ID");
		
    	// create map to sort MP Headers
    	logger.info("calculating sorts for MP Header Systems");
    	Map<String,Integer> mpHeaderSortMap = new HashMap<String,Integer>();
    	String query = "select distinct header from hdp_annotation  order by header ";
	    rs = ex.executeProto(query);
	    int headerCount = 0;
		while(rs.next())
		{
			mpHeaderSortMap.put(rs.getString("header"),++headerCount);
		}
		// put normal phenotype header last
		if(mpHeaderSortMap.containsKey(NORMAL_PHENOTYPE)) mpHeaderSortMap.put(NORMAL_PHENOTYPE,++headerCount); 
    	
    	logger.info("finshed calculating sorts for MP Header Systems");
    	
    	// Sort terms by name using smart alpha
    	Map<String,Integer> termSortMap = new HashMap<String,Integer>();
    	ArrayList<String> termsToSort = new ArrayList<String>();
    	logger.info("calculating sorts for OMIM disease names");
    	query = "select distinct term " +
    			"from term t " +
    			"where t.vocab_name in ('OMIM','Mammalian Phenotype') ";
	    rs = ex.executeProto(query);
		while(rs.next())
		{
		      termsToSort.add(rs.getString("term"));
		}
		//sort the terms using smart alpha
		Collections.sort(termsToSort,new SmartAlphaComparator());
		for(int i=0;i<termsToSort.size();i++)
		{
			  termSortMap.put(termsToSort.get(i), i);
		}
		int maxTermSort = termSortMap.keySet().size()+1;
		termsToSort = null; // not used anymore
	    logger.info("finished calculating sorts for OMIM disease names");
    	
	    // load disease and MP synonyms
    	String termSynonymQuery="select t.primary_id term_id,ts.synonym "+
    			"from term t,term_synonym ts "+
    			"where t.term_key=ts.term_key " +
    				"and t.vocab_name in ('OMIM','Mammalian Phenotype') ";
    	Map<String,Set<String>> termSynonymMap = populateLookup(termSynonymQuery,"term_id","synonym","disease + MP synonyms to term IDs");
        
    	// load alternate MP IDs
        String termIdQuery="select t.primary_id term_id,ti.acc_id alt_id "+
    			"from term t,term_id ti "+
    			"where t.term_key=ti.term_key " +
    				"and t.vocab_name in ('Mammalian Phenotype') ";
        Map<String,Set<String>> altTermIdMap = populateLookup(termIdQuery,"term_id","alt_id","alternate MP IDs to term IDs");
       
        // load term ancestors and ancestor IDs
        String termAncestorQuery="select t.primary_id term_id, " +
        		"tas.ancestor_term, " +
        		"tas.ancestor_primary_id ancestor_id "+
    			"from term t,term_ancestor_simple tas "+
    			"where t.term_key=tas.term_key " +
    				"and t.vocab_name in ('Mammalian Phenotype') ";
        Map<String,Set<String>> termAncestorMap = populateLookup(termAncestorQuery,"term_id","ancestor_term","MP ancestor terms to term IDs");
        Map<String,Set<String>> termAncestorIdMap = populateLookup(termAncestorQuery,"term_id","ancestor_id","MP ancestor IDs to term IDs");
        
        // Now we want to load all altIds, and synonyms for every ancestor up the tree (DAG)...
        // For now we will store every alt ID and synonym together with the primary terms and IDs for ancestors.
        // We can change this if we ever need to separate them.
        for(String termId : termAncestorIdMap.keySet())
        {
        	Set<String> ancestorSynonyms = new HashSet<String>();
        	Set<String> ancestorAltIds = new HashSet<String>();
		
			for(String ancestorId : termAncestorIdMap.get(termId))
	        {
	
				if(termSynonymMap.containsKey(ancestorId))
				{
					ancestorSynonyms.addAll(termSynonymMap.get(ancestorId));
				}
				if(altTermIdMap.containsKey(ancestorId))
				{
					ancestorAltIds.addAll(altTermIdMap.get(ancestorId));
				}
	        }
			termAncestorIdMap.get(termId).addAll(ancestorAltIds);
        	if(termAncestorMap.containsKey(termId)) termAncestorMap.get(termId).addAll(ancestorSynonyms);
        }
 
        // load mouse symbols associated with a disease via hdp_annotation  (no complex genotypes)
        String diseaseMouseSymbolQuery = "select distinct h.term_id, m.symbol "+
        		"from tmp_hdp_annotation_nn  h, marker m "+
        		"where h.vocab_name='OMIM' " +
			"and h.organism_key = 1 " +
			"and h.marker_key = m.marker_key " +
			"and (h.genotype_type!='complex' or h.genotype_type is null) " +
        		"order by term_id ";
        Map<String,Set<String>> diseaseMouseSymbolMap = populateLookupOrdered(diseaseMouseSymbolQuery,"term_id","symbol", "diseaes (OMIM) ids to mouse symbol");
        
        // load human symbols associated with a disease via tmp_hdp_annotation_nn  (no complex genotypes)
        String diseaseHumanSymbolQuery = "select distinct h.term_id, m.symbol "+
        		"from tmp_hdp_annotation_nn  h, marker m "+
        		"where h.vocab_name='OMIM' " +
			"and h.organism_key = 2 " +
			"and h.marker_key = m.marker_key " +
			"and (h.genotype_type!='complex' or h.genotype_type is null) " +
        		"order by term_id ";
        Map<String,Set<String>> diseaseHumanSymbolMap = populateLookupOrdered(diseaseHumanSymbolQuery,"term_id","symbol", "diseaes (OMIM) ids to human symbol");
        
        // load the disease model counts for each disease
        // using a distrinct set of OMIM term to marker annotations, return the counts by term_id
        String diseaseModelQuery="select dm.disease_id, count(dm.disease_model_key) diseaseModelCount " +
        		"from disease_model dm " +
        		"where is_not_model=0 " +
        		"group by disease_id ";

		Map<String, Integer> diseaseModelMap = new HashMap<String,Integer>();
		logger.info("builing map of disease model count -> OMIM ID");
	
		rs = ex.executeProto(diseaseModelQuery);
		while(rs.next())
		{
			String termId = rs.getString("disease_id");
			Integer diseaseModelCount = rs.getInt("diseaseModelCount");
			diseaseModelMap.put(termId,diseaseModelCount);
		}
		logger.info("done builing map of disease model count -> OMIM ID");

        // ------------- GRID CLUSTER RELATED LOOKUPS ------------
		
		// load homologene IDs
        String homologeneIdQuery="select hcotm.marker_key, hc.primary_id homology_id "+
					"from homology_cluster hc, "+
						"homology_cluster_organism hco, "+
						"homology_cluster_organism_to_marker hcotm "+
					"where hc.cluster_key=hco.cluster_key "+
						"and hco.cluster_organism_key=hcotm.cluster_organism_key ";

		Map<Integer, String> homologeneIdMap = new HashMap<Integer,String>();
		logger.info("builing map of marker key -> homologeneId");
	
		rs = ex.executeProto(homologeneIdQuery);
		while(rs.next())
		{
			Integer markerKey = rs.getInt("marker_key");
			String homologeneId = rs.getString("homology_id");
			homologeneIdMap.put(markerKey,homologeneId);
		}
		logger.info("done builing map of marker key -> homologeneId");
		
		String gridMouseSymbolsQuery = "select gcm.hdp_gridcluster_key, " +
				"gcm.symbol||'||'||gcm.marker_key symbol " +
				"from hdp_gridcluster_marker gcm " +
				"where gcm.organism_key=1 ";
        Map<String,Set<String>> gridMouseSymbolsMap = populateLookupOrdered(gridMouseSymbolsQuery,"hdp_gridcluster_key","symbol", "gridclusterKeys to mouse symbols");
		
        String gridHumanSymbolsQuery =  "select gcm.hdp_gridcluster_key, " +
				"gcm.symbol " +
				"from hdp_gridcluster_marker gcm " +
				"where gcm.organism_key=2 ";
        Map<String,Set<String>> gridHumanSymbolsMap = populateLookupOrdered(gridHumanSymbolsQuery,"hdp_gridcluster_key","symbol", "gridclusterKeys to human symbols");
        
        // load grid cluster sorts
        String gridByLocationQuery="select hdp_gridcluster_key,organism_key,min_location\n" + 
        		"from (select gcm.hdp_gridcluster_key,gcm.organism_key,min(msn.by_location) min_location\n" + 
        		"	from hdp_gridcluster_marker gcm,\n" + 
        		"		marker_sequence_num msn\n" + 
        		"	where gcm.marker_key=msn.marker_key\n" + 
        		"	group by gcm.hdp_gridcluster_key,gcm.organism_key) q1 ";

		Map<Integer, Integer> gridByHumanLocationMap = new HashMap<Integer,Integer>();
		Map<Integer, Integer> gridByMouseLocationMap = new HashMap<Integer,Integer>();
		Integer maxSort = 99999999;
		
		logger.info("builing map of grid cluster key -> human/mouse location sorts");
		rs = ex.executeProto(gridByLocationQuery);
		while(rs.next())
		{
			int organismKey = rs.getInt("organism_key");
			if(organismKey==1) gridByMouseLocationMap.put(rs.getInt("hdp_gridcluster_key"),rs.getInt("min_location"));
			else gridByHumanLocationMap.put(rs.getInt("hdp_gridcluster_key"),rs.getInt("min_location"));

		}
		logger.info("done builing map of grid cluster key -> human/mouse location sorts");
        
        // ------------- MARKER RELATED LOOKUPS ------------
        
        // get IMSR count for each marker that has an allele
		logger.info("building counts of IMSR to marker key");
		String markerIMSRQuery = "select distinct on (m.marker_key) m.marker_key, aic.count_for_marker imsr_count \n" + 
				"from marker m, allele_imsr_counts aic, marker_to_allele mta \n" + 
				"where mta.marker_key=m.marker_key\n" + 
				"and mta.allele_key=aic.allele_key";
		rs = ex.executeProto(markerIMSRQuery);
		Map<Integer,Integer> markerIMSRMap = new HashMap<Integer,Integer>();
		while(rs.next())
		{
			markerIMSRMap.put(rs.getInt("marker_key"),rs.getInt("imsr_count"));
		}
		logger.info("done building counts of IMSR to marker key");
        
		// get count of disease relevant references
		logger.info("building counts of disease relevant refs to marker key");
		String diseaseRelevantMarkerQuery = "select marker_key, count(reference_key) ref_count " +
				"from hdp_marker_to_reference " +
				"group by marker_key ";
		rs = ex.executeProto(diseaseRelevantMarkerQuery);
		Map<Integer,Integer> markerDiseaseRefCountMap = new HashMap<Integer,Integer>();
		while(rs.next())
		{
			markerDiseaseRefCountMap.put(rs.getInt("marker_key"),rs.getInt("ref_count"));
		}
		logger.info("done building counts of disease relevant refs to marker key");
		
        // load MP headers(systems) associated with markers via hdp_annotation  (no complex genotypes)
        String markerSystemQuery = "WITH "+
			"term_to_system as (select distinct mpt.term_id,mps.system "+
				"from mp_term mpt, "+
				"mp_system mps "+
				"where mpt.mp_system_key=mps.mp_system_key " +
				"and mps.system!='normal phenotype') "+
			"select distinct ha.marker_key,tts.system "+
			"from tmp_hdp_annotation_nn  ha, "+
				"term_to_system tts "+
			"where ha.term_id=tts.term_id "+
				"and ha.vocab_name='Mammalian Phenotype' " +
				"and (genotype_type!='complex' or genotype_type is null) " +
			"order by system ";
        Map<String,Set<String>> markerSystemMap = populateLookupOrdered(markerSystemQuery,"marker_key","system","MP systems to marker_keys");
        
        // load diseases associated with markers via tmp_hdp_annotation_nn  (no complex genotypes)
        String markerDiseaseQuery = "select distinct marker_key,term disease "+
        		"from tmp_hdp_annotation_nn  "+
        		"where vocab_name='OMIM' " +
        		"and (genotype_type!='complex' or genotype_type is null) " +
        		"order by disease ";
        Map<String,Set<String>> markerDiseaseMap = populateLookupOrdered(markerDiseaseQuery,"marker_key","disease","marker_keys (simple/null genotypes) to diseases");
        
        // load all marker synonyms (including synonyms for every organism)
        String markerSynonymQuery="select distinct msn.marker_key,msn.term synonym " +
        		"from marker_searchable_nomenclature msn " +
        		"where msn.term_type in ('old symbol','old name','synonym') " +
        		"UNION " +
        		"select distinct ms.marker_key, ms.synonym " +
        		"from marker_synonym ms ";
        Map<String,Set<String>> markerSynonymMap = populateLookup(markerSynonymQuery,"marker_key","synonym","marker keys to marker synonyms");
        
        // load all marker IDs (for every organism)
        String markerIdQuery="select marker_key,acc_id from marker_id " +
        		"where logical_db not in ('ABA','Download data from the QTL Archive','FuncBase','GENSAT','GEO','HomoloGene','RIKEN Cluster','UniGene') ";
        Map<String,Set<String>> markerIdMap = populateLookup(markerIdQuery,"marker_key","acc_id","marker keys to marker Ids");
        
        // load all mouse and human marker locations to be encoded for Solr spatial queries
    	String markerLocationQuery="select distinct m.marker_key, " +
    			"ml.chromosome, " +
    			"ml.strand, " +
    			"ml.start_coordinate, " +
    			"ml.end_coordinate " +
    			"from marker_location ml, " +
    			"marker m " +
    			"where m.marker_key=ml.marker_key " +
    			"and ml.sequence_num=1 " +
    			"and ml.start_coordinate is not null " +
    			"and ml.chromosome != 'UN' ";
    			//"and strand is not null ";
    	Map<Integer,Set<String>> markerLocationMap = new HashMap<Integer,Set<String>>();
    	logger.info("building map of marker locations to marker keys");
    	
        rs = ex.executeProto(markerLocationQuery);

        while (rs.next())
        {
        	Integer markerKey = rs.getInt("marker_key");
        	String chromosome = rs.getString("chromosome");
        	Long start = rs.getLong("start_coordinate");
        	Long end = rs.getLong("end_coordinate");
        	if(!markerLocationMap.containsKey(markerKey))
        	{
        		markerLocationMap.put(markerKey, new HashSet<String>());
        	}
        	// NOTE: we are ignoring strand at this time. We can support searching both (in theory), so we will just treat everything as positive for now.
        	// AS OF 2013/08/06 -kstone
        	String spatialString = SolrLocationTranslator.getIndexValue(chromosome,start,end,true);
        	// only add locations for chromosomes we can map. The algorithm will ignore any weird values
        	if(spatialString != null && !spatialString.equals(""))
        	{
        		markerLocationMap.get(markerKey).add(spatialString);
        	}
        }
        logger.info("done building map of marker locations to marker keys");
        
        // load marker homology information
        String markerOrthologQuery="select distinct otm.marker_key marker_key, "+
        			"other_otm.marker_key other_marker_key "+
				"from homology_cluster_organism o, "+
					"homology_cluster_organism_to_marker otm, "+
					"homology_cluster_organism other_o, "+
					"homology_cluster_organism_to_marker other_otm "+
				"where other_o.cluster_key=o.cluster_key "+
					"and o.cluster_organism_key=otm.cluster_organism_key "+
					"and other_o.cluster_organism_key=other_otm.cluster_organism_key "+
					"and otm.marker_key!=other_otm.marker_key ";
        Map<String,Set<String>> markerOrthologMap = populateLookup(markerOrthologQuery,"marker_key","other_marker_key","marker keys to marker ortholog keys");
        
        // load symbol and name for orthologs
        String orthologNomenQuery="select marker_key,symbol,name from marker";
        Map<String,Set<String>> orthologNomenMap = populateLookup(orthologNomenQuery,"marker_key","symbol","marker keys to gene symbol for orthologs");
        orthologNomenMap = populateLookup(orthologNomenQuery,"marker_key","name","marker keys to gene name for orthologs",orthologNomenMap);
        
        // map the locations generated above to their orthologs in the homology cluster
        String orthologHumanMouseQuery = "select distinct mouse_otm.marker_key mouse_marker_key, "+
        		"human_otm.marker_key human_marker_key "+
				"from homology_cluster_organism mouse_o, "+
				"homology_cluster_organism_to_marker mouse_otm, "+
				"homology_cluster_organism human_o, "+
				"homology_cluster_organism_to_marker human_otm "+
				"where human_o.cluster_key=mouse_o.cluster_key "+
					"and mouse_o.cluster_organism_key=mouse_otm.cluster_organism_key "+
					"and human_o.cluster_organism_key=human_otm.cluster_organism_key "+
					"and mouse_o.organism='mouse' "+
					"and human_o.organism='human' ";
        Map<Integer,Set<String>> orthologLocationMap = new HashMap<Integer,Set<String>>();
    	logger.info("building map of marker locations to ortholog marker keys");
    	
        rs = ex.executeProto(orthologHumanMouseQuery);

        while (rs.next())
        {
        	Integer mouseMarkerKey = rs.getInt("mouse_marker_key");
        	Integer humanMarkerKey = rs.getInt("human_marker_key");
        	// locations
        	// do mouse first
        	if(!orthologLocationMap.containsKey(mouseMarkerKey))
        	{
        		orthologLocationMap.put(mouseMarkerKey, new HashSet<String>());
        	}
        	if(markerLocationMap.containsKey(humanMarkerKey))
        	{
        		// add all locations for every ortholog
        		orthologLocationMap.get(mouseMarkerKey).addAll(markerLocationMap.get(humanMarkerKey));
        	}
        	// do human
        	if(!orthologLocationMap.containsKey(humanMarkerKey))
        	{
        		orthologLocationMap.put(humanMarkerKey, new HashSet<String>());
        	}
        	if(markerLocationMap.containsKey(mouseMarkerKey))
        	{
        		// add all locations for every ortholog
        		orthologLocationMap.get(humanMarkerKey).addAll(markerLocationMap.get(mouseMarkerKey));
        	}
        }
        logger.info("done building map of marker locations to ortholog marker keys");

        
        // ---------------- LOAD DISEASES WITHOUT HDP_ANNOTATIONS INTO SOLR ---------------
        Integer uniqueKey = 0;
        logger.info("loading disease terms without annotations");
        String termsNoAnnotQuery = "select t.term, " +
        		"t.primary_id term_id, " +
        		"'OMIM' vocab_name " +
        		"from term t " +
        		"where not exists (select 1 from hdp_annotation ha " +
        			"where ha.term_id=t.primary_id) " +
        		"and t.vocab_name='OMIM' " +
        		"and t.is_obsolete=0 ";
        rs = ex.executeProto(termsNoAnnotQuery);
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        while (rs.next()) 
        {  
        	uniqueKey += 1;
          	String term = rs.getString("term");
        	String termId = rs.getString("term_id");
        	String vocabName = rs.getString("vocab_name");
        	
        	SolrInputDocument doc = new SolrInputDocument();
        	doc.addField(DiseasePortalFields.UNIQUE_KEY,uniqueKey);
        	doc.addField(DiseasePortalFields.TERM,term);
        	doc.addField(DiseasePortalFields.TERM_ID,termId);
        	doc.addField(DiseasePortalFields.TERM_ID_GROUP,termId);
        	doc.addField(DiseasePortalFields.TERM_TYPE,vocabName);
        	// find sort for the term name
        	Integer termSort = termSortMap.containsKey(term) ? termSortMap.get(term) : maxTermSort;
        	doc.addField(DiseasePortalFields.BY_TERM_NAME,termSort);
        	
        	// add any synonyms for this OMIM term
        	this.addAllFromLookup(doc,DiseasePortalFields.TERM_SYNONYM,termId,termSynonymMap);
        	
        	docs.add(doc);
        	if (docs.size() > 1000) 
        	{
                writeDocs(docs);
                docs = new ArrayList<SolrInputDocument>();
        	}
        }
        if (!docs.isEmpty())  server.add(docs);
        server.commit();
        
        logger.info("done loading disease terms without annotations");

        ResultSet rs_tmp = ex.executeProto("select max(CAST(nullif(primary_id,'') AS integer)) max_homologene_id from homology_cluster;");
        rs_tmp.next();
        Integer maxHomologeneId = rs_tmp.getInt("max_homologene_id") + 1;
        
        // ---------------- LOAD MARKERS WITHOUT 'simple' HDP_ANNOTATIONS INTO SOLR ---------------
        logger.info("loading markers without annotations");
        String markersNoAnnotQuery = "select m.marker_key, " +
        		"m.organism, " +
        		"m.symbol marker_symbol," +
        		"m.name marker_name, " +
        		"m.primary_id marker_id, " +
        		"m.marker_subtype feature_type, " +
        		"m.location_display, " +
        		"m.coordinate_display, " +
        		"m.build_identifier, " +
        		"msqn.by_organism, " +
        		"msqn.by_symbol, " +
        		"msqn.by_marker_subtype, " +
        		"msqn.by_location, " +
        		"mc.reference_count "+
        		"from marker m, "+
        		"marker_sequence_num msqn, " +
        		"marker_counts mc " +
        		"where not exists (select 1 from tmp_hdp_annotation_nn ha " +
        			"where ha.marker_key=m.marker_key " +
        			"and (ha.genotype_type is null or genotype_type!='complex')) " +
        		"and m.organism in ('mouse','human') " +
        		"and m.marker_key=msqn.marker_key " +
        		"and m.status='official' " +
        		"and m.marker_type NOT IN ('BAC/YAC end','DNA Segment') " +
        		"and mc.marker_key=m.marker_key ";
        rs = ex.executeProto(markersNoAnnotQuery);
        docs = new ArrayList<SolrInputDocument>();
        while (rs.next()) 
        {  
        	uniqueKey += 1;
          	Integer markerKey = rs.getInt("marker_key");
        	String symbol = rs.getString("marker_symbol");
        	String organism = rs.getString("organism");
        	String homologyId = null;
        	if(homologeneIdMap.containsKey(markerKey)) homologyId = homologeneIdMap.get(markerKey);
        	Integer imsrCount = markerIMSRMap.containsKey(markerKey) ? markerIMSRMap.get(markerKey) : 0;
        	
        	
        	SolrInputDocument doc = new SolrInputDocument();
        	// -------- Marker centric fields -------------
        	doc.addField(DiseasePortalFields.UNIQUE_KEY,uniqueKey.toString());
        	doc.addField(DiseasePortalFields.MARKER_KEY,markerKey);
        	doc.addField(DiseasePortalFields.MARKER_SYMBOL,symbol);
        	doc.addField(DiseasePortalFields.MARKER_MGI_ID,rs.getString("marker_id"));
        	doc.addField(DiseasePortalFields.ORGANISM,organism);
        	doc.addField(DiseasePortalFields.MARKER_NAME,rs.getString("marker_name"));
        	doc.addField(DiseasePortalFields.MARKER_FEATURE_TYPE,rs.getString("feature_type"));
        	doc.addField(DiseasePortalFields.HOMOLOGENE_ID,homologyId);
        	doc.addField(DiseasePortalFields.LOCATION_DISPLAY,rs.getString("location_display"));
        	doc.addField(DiseasePortalFields.COORDINATE_DISPLAY,rs.getString("coordinate_display"));
        	doc.addField(DiseasePortalFields.BUILD_IDENTIFIER,rs.getString("build_identifier"));
        	doc.addField(DiseasePortalFields.MARKER_ALL_REF_COUNT,rs.getString("reference_count"));
        	doc.addField(DiseasePortalFields.MARKER_IMSR_COUNT,imsrCount);
        	
        	// ----------- marker sorts -------------
        	doc.addField(DiseasePortalFields.BY_MARKER_ORGANISM,rs.getString("by_organism"));
        	doc.addField(DiseasePortalFields.BY_MARKER_SYMBOL,rs.getString("by_symbol"));
        	doc.addField(DiseasePortalFields.BY_MARKER_TYPE,rs.getString("by_marker_subtype"));
        	doc.addField(DiseasePortalFields.BY_MARKER_LOCATION,rs.getString("by_location"));
//        	// use max homology id to sort null homology ids last
        	Integer byHomologyId = homologyId == null ? maxHomologeneId : Integer.parseInt(homologyId);
        	doc.addField(DiseasePortalFields.BY_HOMOLOGENE_ID,byHomologyId);
        	
        	// check for marker locations
        	if(markerLocationMap.containsKey(markerKey))
        	{
        		String coordField = DiseasePortalFields.MOUSE_COORDINATE;
        		if("human".equalsIgnoreCase(organism)) coordField = DiseasePortalFields.HUMAN_COORDINATE;
        		for(String spatialString : markerLocationMap.get(markerKey))
        		{
        			doc.addField(coordField,spatialString);
        		}
        		//doc.addField(coordField+"Count",markerLocationMap.get(markerKey).size());
        	}
        	
        	if(orthologLocationMap.containsKey(markerKey))
        	{
        		// now we are loading coordinates for the orthologs (opposite from above)
        		String coordField = DiseasePortalFields.HUMAN_COORDINATE;
        		if("human".equalsIgnoreCase(organism)) coordField = DiseasePortalFields.MOUSE_COORDINATE;
        		for(String spatialString : orthologLocationMap.get(markerKey))
        		{
        			doc.addField(coordField,spatialString);
        		}
        		// for debugging
        		//doc.addField(coordField+"Count",orthologLocationMap.get(markerKey).size());
        	}
        	
        	// load information for gene query
        	addAllFromLookup(doc,DiseasePortalFields.MARKER_SYNONYM,markerKey.toString(),markerSynonymMap);
        	addAllFromLookup(doc,DiseasePortalFields.MARKER_ID,markerKey.toString(),markerIdMap);
        	// load ortholog information for gene query
        	if(markerOrthologMap.containsKey(markerKey.toString()))
        	{
        		for(String orthologMarkerKey : markerOrthologMap.get(markerKey.toString()))
        		{
        			addAllFromLookup(doc,DiseasePortalFields.ORTHOLOG_NOMEN,orthologMarkerKey,orthologNomenMap);
        			addAllFromLookup(doc,DiseasePortalFields.ORTHOLOG_NOMEN,orthologMarkerKey,markerSynonymMap);
                	addAllFromLookup(doc,DiseasePortalFields.ORTHOLOG_ID,orthologMarkerKey,markerIdMap);
        		}
        	}
        	docs.add(doc);
        	if (docs.size() > 1000) 
        	{
                writeDocs(docs);
                docs = new ArrayList<SolrInputDocument>();
        	}
        }
        if (!docs.isEmpty())  server.add(docs);
        server.commit();
        
        logger.info("done loading markers without annotations");
       

        // ---------------- LOAD HDP_ANNOTATIONS [clusters,markers(except 'complex'),genotypes,terms(disease+MP)] INTO SOLR ---------------
    	rs_tmp = ex.executeProto("select max(hdp_annotation_key) as max_hdp_key from hdp_annotation");
    	rs_tmp.next();
    	
    	Integer start = 0;
        Integer end = rs_tmp.getInt("max_hdp_key");
    	int chunkSize = 30000;
        
        int modValue = end.intValue() / chunkSize;
        
        // Perform the chunking, this might become a configurable value later on

        logger.info("Getting all disease and MP annotations");
        
        for (int i = 0; i <= modValue; i++) {
        
            start = i * chunkSize;
            end = start + chunkSize;

            logger.info ("Processing hdp_annotation key > " + start + " and <= " + end);
            
            // populate the lookups for this chunk of data
            // set which data we query for any given mp term
            @SuppressWarnings("unchecked")
			List<Map<String,Set<String>>> mpLookups = Arrays.asList(termSynonymMap,altTermIdMap,termAncestorMap,termAncestorIdMap);
            // make a lookup to map MP queries to diseases for related genotype clusters
            Map<String,Set<String>> mpTermForSSDiseaseMap = this.populateMpForSSDiseaseLookup(start,end,mpLookups);
            // make a lookup to map MP queries to diseases for related genotypes
            Map<String,Set<String>> mpTermForDiseaseMap = this.populateMpForDiseaseLookup(start,end,mpLookups);
            // make a lookup to map MP queries to other MP terms in related genotype clusters
            Map<String,Set<String>> mpTermForMPMap = this.populateMpForMpGridLookup(start,end,mpLookups);
            // make a lookup to map OMIM queries to other MP terms in related genotype clusters
            @SuppressWarnings("unchecked")
			List<Map<String,Set<String>>> omimLookups = Arrays.asList(termSynonymMap);
            Map<String,Set<String>> omimTermForMPMap = populateOMIMForMpGridLookup(start,end,omimLookups);
            Map<String,Set<String>> omimTermForDiseaseMap = populateOMIMForOMIMGridLookup(start,end,omimLookups);
            
            String annotationQuery="select ha.hdp_annotation_key, " +
        		"ha.marker_key, " +
        		"ha.genotype_key, " +
        		"ha.term, " +
        		"ha.term_id, " +
        		"ha.vocab_name, " +
        		"ha.genotype_type, " +
        		"ha.header term_header, " +
        		"ha.qualifier_type, " +
        		"ha.term_seq, " +
        		"ha.term_depth, " +
        		"m.organism, " +
        		"m.symbol marker_symbol, " +
        		"m.primary_id marker_id, " +
        		"m.name marker_name, " +
        		"m.marker_subtype feature_type, " +
        		"m.location_display, " +
        		"m.coordinate_display, " +
        		"m.build_identifier, " +
        		"msqn.by_organism, " +
        		"msqn.by_symbol, " +
        		"msqn.by_marker_subtype, " +
        		"msqn.by_location, " +
        		"mc.reference_count, " +
        		"gcm.hdp_gridcluster_key, " +
        		"gcg.hdp_genocluster_key, " +
        		"gsn.by_hdp_rules by_genocluster "+
        		"from hdp_annotation ha left outer join " +
        		"hdp_gridcluster_marker gcm on gcm.marker_key=ha.marker_key left outer join " +
        		"hdp_genocluster_genotype gcg on gcg.genotype_key=ha.genotype_key left outer join " +
        		"genotype_sequence_num gsn on gsn.genotype_key=ha.genotype_key, " +
        		"marker m, " +
        		"marker_sequence_num msqn, " +
        		"marker_counts mc " +
        		"where m.marker_key=ha.marker_key " +
        		"and m.marker_key=msqn.marker_key " +
        		"and m.marker_key=mc.marker_key " +
        		"and ha.hdp_annotation_key > "+start+" and ha.hdp_annotation_key <= "+end+" ";
	        
            rs = ex.executeProto(annotationQuery);
            
            docs = new ArrayList<SolrInputDocument>();
            
            logger.info("Parsing them into Solr docs");
            while (rs.next()) 
            {           
            	uniqueKey += 1;
            	String hdpAnnotationKey = rs.getString("hdp_annotation_key");
            	Integer markerKey = rs.getInt("marker_key");
            	String organism = rs.getString("organism");
            	String markerSymbol = rs.getString("marker_symbol");
            	String homologyId = null;
            	if(homologeneIdMap.containsKey(markerKey)) homologyId = homologeneIdMap.get(markerKey);
            	Integer gridClusterKey = rs.getInt("hdp_gridcluster_key");
            	Integer genoClusterKey = rs.getInt("hdp_genocluster_key");
            	
            	Integer genotypeKey = rs.getInt("genotype_key");
            	String genotypeType = rs.getString("genotype_type");
            	
            	String termQualifier = rs.getString("qualifier_type");
            	if(termQualifier==null) termQualifier = "";
            	
            	String term = rs.getString("term");
            	String termId = rs.getString("term_id");
            	String vocabName = rs.getString("vocab_name");
            	
            	SolrInputDocument doc = new SolrInputDocument();

            	// unique document key
            	doc.addField(DiseasePortalFields.UNIQUE_KEY,uniqueKey.toString());
            	
            	// --------- Grid cluster fields ---------------
            	// For grid clusters, we only include human annotations and super-simple mouse annotations (no allele->OMIM)
            	boolean isOnGrid = "human".equalsIgnoreCase(organism) 
            			|| (!"complex".equalsIgnoreCase(genotypeType) && genoClusterKey!=null && genoClusterKey>0);
            	if(isOnGrid)
            	{
            		if(gridClusterKey!=null && gridClusterKey>0)
            		{
            			doc.addField(DiseasePortalFields.GRID_CLUSTER_KEY,gridClusterKey);
            			addAllFromLookup(doc,DiseasePortalFields.GRID_MOUSE_SYMBOLS,gridClusterKey.toString(),gridMouseSymbolsMap);
            			addAllFromLookup(doc,DiseasePortalFields.GRID_HUMAN_SYMBOLS,gridClusterKey.toString(),gridHumanSymbolsMap);
            			
            			// add special grid sorts
            			int gridByMouseLocation = gridByMouseLocationMap.containsKey(gridClusterKey) ? gridByMouseLocationMap.get(gridClusterKey) : maxSort;
            			int gridByHumanLocation = gridByHumanLocationMap.containsKey(gridClusterKey) ? gridByHumanLocationMap.get(gridClusterKey) : maxSort;
            			doc.addField(DiseasePortalFields.GRID_BY_MOUSE_LOCATION,gridByMouseLocation);
            			doc.addField(DiseasePortalFields.GRID_BY_HUMAN_LOCATION,gridByHumanLocation);
            		}
            		if(genoClusterKey!=null && genoClusterKey>0)
            		{
            			doc.addField(DiseasePortalFields.GENO_CLUSTER_KEY,genoClusterKey);   
            			doc.addField(DiseasePortalFields.BY_GENOCLUSTER,rs.getInt("by_genocluster"));
            		}
            		
            	}
            	
            	// -------- Marker centric fields -------------
            	
            	// we don't want to add the marker information when we have a complex genotypeType.
            	if(!"complex".equals(genotypeType))
            	{	
	            	doc.addField(DiseasePortalFields.MARKER_KEY,markerKey);
	            	doc.addField(DiseasePortalFields.MARKER_SYMBOL,markerSymbol);
	            	doc.addField(DiseasePortalFields.MARKER_MGI_ID,rs.getString("marker_id"));
	            	doc.addField(DiseasePortalFields.ORGANISM,organism);
	            	doc.addField(DiseasePortalFields.MARKER_NAME,rs.getString("marker_name"));
	            	doc.addField(DiseasePortalFields.MARKER_FEATURE_TYPE,rs.getString("feature_type"));
	            	doc.addField(DiseasePortalFields.HOMOLOGENE_ID,homologyId);
	            	doc.addField(DiseasePortalFields.LOCATION_DISPLAY,rs.getString("location_display"));
	            	doc.addField(DiseasePortalFields.COORDINATE_DISPLAY,rs.getString("coordinate_display"));
	            	doc.addField(DiseasePortalFields.BUILD_IDENTIFIER,rs.getString("build_identifier"));
	            	doc.addField(DiseasePortalFields.MARKER_ALL_REF_COUNT,rs.getString("reference_count"));
	            	int markerDiseaseRefCount = markerDiseaseRefCountMap.containsKey(markerKey) ? markerDiseaseRefCountMap.get(markerKey) : 0;
	            	doc.addField(DiseasePortalFields.MARKER_DISEASE_REF_COUNT,markerDiseaseRefCount);

                	Integer imsrCount = markerIMSRMap.containsKey(markerKey) ? markerIMSRMap.get(markerKey) : 0;
	            	doc.addField(DiseasePortalFields.MARKER_IMSR_COUNT,imsrCount);
	            	
	            	// add all distinct disease names for the marker
	            	addAllFromLookup(doc,DiseasePortalFields.MARKER_DISEASE,rs.getString("marker_key"),markerDiseaseMap);
	            	addAllFromLookup(doc,DiseasePortalFields.MARKER_SYSTEM,rs.getString("marker_key"),markerSystemMap);
	            	
	            	// ----------- marker sorts -------------
	            	doc.addField(DiseasePortalFields.BY_MARKER_ORGANISM,rs.getString("by_organism"));
	            	doc.addField(DiseasePortalFields.BY_MARKER_SYMBOL,rs.getString("by_symbol"));
	            	doc.addField(DiseasePortalFields.BY_MARKER_TYPE,rs.getString("by_marker_subtype"));
	            	doc.addField(DiseasePortalFields.BY_MARKER_LOCATION,rs.getString("by_location"));
	            	// use max homology id to sort null homology ids last
	            	Integer byHomologyId = homologyId == null ? maxHomologeneId : Integer.parseInt(homologyId);
	            	doc.addField(DiseasePortalFields.BY_HOMOLOGENE_ID,byHomologyId);
	            	
	            	
	            	// check for marker locations
	            	if(markerLocationMap.containsKey(markerKey))
	            	{
	            		String coordField = DiseasePortalFields.MOUSE_COORDINATE;
	            		if("human".equalsIgnoreCase(organism)) coordField = DiseasePortalFields.HUMAN_COORDINATE;
	            		for(String spatialString : markerLocationMap.get(markerKey))
	            		{
	            			doc.addField(coordField,spatialString);
	            		}
	            		//doc.addField(coordField+"Count",markerLocationMap.get(markerKey).size());
	            	}
	            	
	            	if(orthologLocationMap.containsKey(markerKey))
	            	{
	            		// now we are loading coordinates for the orthologs (opposite from above)
	            		String coordField = DiseasePortalFields.HUMAN_COORDINATE;
	            		if("human".equalsIgnoreCase(organism)) coordField = DiseasePortalFields.MOUSE_COORDINATE;
	            		for(String spatialString : orthologLocationMap.get(markerKey))
	            		{
	            			doc.addField(coordField,spatialString);
	            		}
	            		// for debugging
	            		//doc.addField(coordField+"Count",orthologLocationMap.get(markerKey).size());
	            	}
	            	
	            	// load information for gene query
	            	addAllFromLookup(doc,DiseasePortalFields.MARKER_SYNONYM,markerKey.toString(),markerSynonymMap);
	            	addAllFromLookup(doc,DiseasePortalFields.MARKER_ID,markerKey.toString(),markerIdMap);
	            	// load ortholog information for gene query
	            	if(markerOrthologMap.containsKey(markerKey.toString()))
	            	{
	            		for(String orthologMarkerKey : markerOrthologMap.get(markerKey.toString()))
	            		{
	            			addAllFromLookup(doc,DiseasePortalFields.ORTHOLOG_NOMEN,orthologMarkerKey,orthologNomenMap);
	            			addAllFromLookup(doc,DiseasePortalFields.ORTHOLOG_NOMEN,orthologMarkerKey,markerSynonymMap);
	                    	addAllFromLookup(doc,DiseasePortalFields.ORTHOLOG_ID,orthologMarkerKey,markerIdMap);
	            		}
	            	}
            	
	            	
	            	// add the join key for human markers
	            	if("OMIM".equalsIgnoreCase(vocabName)) 
	            	{
	            		doc.addField(DiseasePortalFields.HUMAN_DISEASE_JOIN_KEY,makeHumanDiseaseKey(markerKey,termId));
	            	}
            	}
            	// ----------- genotype centric fields ----------------
            	doc.addField(DiseasePortalFields.GENOTYPE_KEY,genotypeKey);
            	doc.addField(DiseasePortalFields.GENOTYPE_TYPE,genotypeType);
            	
            	// ------------- term centric fields -------------------
            	doc.addField(DiseasePortalFields.TERM,term);
            	doc.addField(DiseasePortalFields.TERM_ID,termId);
            	doc.addField(DiseasePortalFields.TERM_ID_GROUP,termId);
            	doc.addField(DiseasePortalFields.TERM_TYPE,vocabName);
            	String termHeader = rs.getString("term_header");
            	//HACK: edit for diseases
            	if ("OMIM".equalsIgnoreCase(vocabName) && (termHeader == null || termHeader.equals(""))) termHeader = term;
            	doc.addField(DiseasePortalFields.TERM_HEADER,termHeader);
            	doc.addField(DiseasePortalFields.TERM_QUALIFIER,termQualifier);

            	
            	// find sort for the term name
            	Integer termSort = termSortMap.containsKey(term) ? termSortMap.get(term) : maxTermSort;
            	doc.addField(DiseasePortalFields.BY_TERM_NAME,termSort);
            	
            	Integer termDagSort = rs.getInt("term_seq");
            	if(termDagSort == null) termDagSort = maxTermSort;
            	doc.addField(DiseasePortalFields.BY_TERM_DAG,termDagSort);
            	//doc.addField("termDepth",rs.getInt("term_depth"));
            	
            	
            	// find sort for the term header
            	int termHeaderSort = mpHeaderSortMap.containsKey(termHeader) ? mpHeaderSortMap.get(termHeader) : headerCount+1;
            	doc.addField(DiseasePortalFields.BY_MP_HEADER,termHeaderSort);
            	
            	// add any synonyms for this term
            	addAllFromLookup(doc,DiseasePortalFields.TERM_SYNONYM,termId,termSynonymMap);
            	
            	// add all alternate IDs for this term
            	addAllFromLookup(doc,DiseasePortalFields.TERM_ALT_ID,termId,altTermIdMap);
            	
            	// add all ancestor terms + synonyms
            	addAllFromLookup(doc,DiseasePortalFields.TERM_ANCESTOR,termId,termAncestorMap);
            	
            	// add all ancestor terms IDs and alt IDs
            	addAllFromLookup(doc,DiseasePortalFields.TERM_ANCESTOR,termId,termAncestorIdMap);
            	
            	// add all disease terms and mouse symbols
            	addAllFromLookup(doc,DiseasePortalFields.TERM_MOUSESYMBOL,termId,diseaseMouseSymbolMap);

            	// add all disease terms and human symbols
            	addAllFromLookup(doc,DiseasePortalFields.TERM_HUMANSYMBOL,termId,diseaseHumanSymbolMap);

            	if(vocabName.equals("OMIM"))
            	{
            		int diseaseRefCount = diseaseRefCountMap.containsKey(termId) ? diseaseRefCountMap.get(termId) : 0;
            		doc.addField(DiseasePortalFields.DISEASE_REF_COUNT,diseaseRefCount);
            		
            		addAllFromLookup(doc,DiseasePortalFields.MP_TERM_FOR_DISEASE,hdpAnnotationKey,mpTermForDiseaseMap);
            		addAllFromLookup(doc,DiseasePortalFields.MP_TERM_FOR_SS_DISEASE,hdpAnnotationKey,mpTermForSSDiseaseMap);
            		addAllFromLookup(doc,DiseasePortalFields.OMIM_TERM_FOR_DISEASE,hdpAnnotationKey,omimTermForDiseaseMap);
            	}
            	else if(vocabName.equals("Mammalian Phenotype"))
            	{
            		addAllFromLookup(doc,DiseasePortalFields.MP_TERM_FOR_PHENOTYPE,hdpAnnotationKey,mpTermForMPMap);
            		addAllFromLookup(doc,DiseasePortalFields.OMIM_TERM_FOR_PHENOTYPE,hdpAnnotationKey,omimTermForMPMap);
            	}
            	
            	// check for diseaes model counts
            	if(diseaseModelMap.containsKey(termId))
            	{
            		doc.addField(DiseasePortalFields.DISEASE_MODEL_COUNTS,diseaseModelMap.get(termId));
            	}

                docs.add(doc);
                if (docs.size() > 1000) {
                    //logger.info("Adding a stack of the documents to Solr");
                	startTime();
                    writeDocs(docs);
                    long endTime = stopTime();
                    if(endTime > 500)
                    {
                    	logger.info("time to call writeDocs() "+stopTime());
                    }
                    docs = new ArrayList<SolrInputDocument>();
                }
                
            }
            if (! docs.isEmpty()) {
                server.add(docs);
            }
            
            server.commit();
        }
    }
    private Map<String,Set<String>> populateMpForDiseaseLookup(int start,int end,List<Map<String,Set<String>>> mpLookups) throws Exception
    {
        // load a map of MP term IDs to OMIM disease IDs via hdp_annotation
        logger.info("building map of MP query terms/altIDs/synonyms/ancestors to OMIM hdp_annotation_keys");
        // first we map mp ids = omim ids, we can use existing lookups to get the rest
        String mpToDiseaseQuery = "select mp_id,mp_term,hdp_annotation_key " +
        		"from tmp_mp_to_disease " +
        		"where hdp_annotation_key > "+start+" and hdp_annotation_key <= "+end+" ";
        ResultSet rs = ex.executeProto(mpToDiseaseQuery);
        Map<String,Set<String>> mpTermForDiseaseMap = new HashMap<String,Set<String>>();
        while(rs.next())
        {
        	String hdpAnnotationKey = rs.getString("hdp_annotation_key");
        	String mpId = rs.getString("mp_id");
        	String mpTerm = rs.getString("mp_term");
        	if(!mpTermForDiseaseMap.containsKey(hdpAnnotationKey))
        	{
        		mpTermForDiseaseMap.put(hdpAnnotationKey,new HashSet<String>());
        	}
        	mpTermForDiseaseMap.get(hdpAnnotationKey).add(mpId);
        	mpTermForDiseaseMap.get(hdpAnnotationKey).add(mpTerm);
        	Set<String> mpSearch = new HashSet<String>();
    		for(Map<String,Set<String>> lookup : mpLookups)
    		{
    			if(lookup.containsKey(mpId))
    			{
    				mpSearch.addAll(lookup.get(mpId));
    			}
    		}
    		mpTermForDiseaseMap.get(hdpAnnotationKey).addAll(mpSearch);
        }
        logger.info("finished building map of MP query terms/altIDs/synonyms/ancestors to OMIM hdp_annotation_keys");
        return mpTermForDiseaseMap;
    }
    
    private Map<String,Set<String>> populateMpForSSDiseaseLookup(int start,int end,List<Map<String,Set<String>>> mpLookups) throws Exception
    {
        // load a map of MP term IDs to OMIM disease IDs via hdp_annotation
        logger.info("building map of MP query terms/altIDs/synonyms/ancestors to SS OMIM hdp_annotation_keys");
        // first we map mp ids = omim ids, we can use existing lookups to get the rest
        String mpToDiseaseQuery = "select mp_id,mp_term,hdp_annotation_key " +
        		"from tmp_mp_to_ss_disease " +
        		"where hdp_annotation_key > "+start+" and hdp_annotation_key <= "+end+" ";
        ResultSet rs = ex.executeProto(mpToDiseaseQuery);
        Map<String,Set<String>> mpTermForDiseaseMap = new HashMap<String,Set<String>>();
        while(rs.next())
        {
        	String hdpAnnotationKey = rs.getString("hdp_annotation_key");
        	String mpId = rs.getString("mp_id");
        	String mpTerm = rs.getString("mp_term");
        	if(!mpTermForDiseaseMap.containsKey(hdpAnnotationKey))
        	{
        		mpTermForDiseaseMap.put(hdpAnnotationKey,new HashSet<String>());
        	}
        	mpTermForDiseaseMap.get(hdpAnnotationKey).add(mpId);
        	mpTermForDiseaseMap.get(hdpAnnotationKey).add(mpTerm);
        	Set<String> mpSearch = new HashSet<String>();
    		for(Map<String,Set<String>> lookup : mpLookups)
    		{
    			if(lookup.containsKey(mpId))
    			{
    				mpSearch.addAll(lookup.get(mpId));
    			}
    		}
    		mpTermForDiseaseMap.get(hdpAnnotationKey).addAll(mpSearch);
        }
        
        logger.info("finished building map of MP query terms/altIDs/synonyms/ancestors to SS OMIM hdp_annotation_keys");
        return mpTermForDiseaseMap;
    }
    
    private Map<String,Set<String>> populateMpForMpGridLookup(int start,int end,List<Map<String,Set<String>>> mpLookups) throws Exception
    {
        // load a map of MP term IDs to equivalent MP IDs via hdp_annotation
        logger.info("building map of MP query terms/altIDs/synonyms/ancestors to equivalent MP hdp_annotation_keys");
        // first we map mp ids = MP ids, we can use existing lookups to get the rest
        String mpToMpQuery = "select mp_id,mp_term,hdp_annotation_key " +
        		"from tmp_mp_to_mp " +
        		"where hdp_annotation_key > "+start+" and hdp_annotation_key <= "+end+" ";
        ResultSet rs = ex.executeProto(mpToMpQuery);
        Map<String,Set<String>> mpTermForMPMap = new HashMap<String,Set<String>>();
        while(rs.next())
        {
        	String hdpAnnotationKey = rs.getString("hdp_annotation_key");
        	String mpId = rs.getString("mp_id");
        	String mpTerm = rs.getString("mp_term");
        	if(!mpTermForMPMap.containsKey(hdpAnnotationKey))
        	{
        		mpTermForMPMap.put(hdpAnnotationKey,new HashSet<String>());
        	}
        	mpTermForMPMap.get(hdpAnnotationKey).add(mpId);
        	mpTermForMPMap.get(hdpAnnotationKey).add(mpTerm);
        	Set<String> mpSearch = new HashSet<String>();
    		for(Map<String,Set<String>> lookup : mpLookups)
    		{
    			if(lookup.containsKey(mpId))
    			{
    				mpSearch.addAll(lookup.get(mpId));
    			}
    		}
    		mpTermForMPMap.get(hdpAnnotationKey).addAll(mpSearch);
        }
        
        logger.info("finished building map of MP query terms/altIDs/synonyms/ancestors to equivalent MP hdp_annotation_keys");
        return mpTermForMPMap;
    }
    private Map<String,Set<String>> populateOMIMForMpGridLookup(int start,int end,List<Map<String,Set<String>>> omimLookups) throws Exception
    {
        // load a map of OMIM term IDs to equivalent MP IDs via hdp_annotation
        logger.info("building map of OMIM query terms/synonyms to equivalent MP hdp_annotation_keys");
        // first we map mp ids = MP ids, we can use existing lookups to get the rest
        String omimToMpQuery = "select omim_id,omim_term,hdp_annotation_key " +
        		"from tmp_disease_to_mp " +
        		"where hdp_annotation_key > "+start+" and hdp_annotation_key <= "+end+" ";
        ResultSet rs = ex.executeProto(omimToMpQuery);
        Map<String,Set<String>> omimTermForMPMap = new HashMap<String,Set<String>>();
        while(rs.next())
        {
        	String hdpAnnotationKey = rs.getString("hdp_annotation_key");
        	String omimId = rs.getString("omim_id");
        	String omimTerm = rs.getString("omim_term");
        	if(!omimTermForMPMap.containsKey(hdpAnnotationKey))
        	{
        		omimTermForMPMap.put(hdpAnnotationKey,new HashSet<String>());
        	}
        	omimTermForMPMap.get(hdpAnnotationKey).add(omimId);
        	omimTermForMPMap.get(hdpAnnotationKey).add(omimTerm);
        	Set<String> omimSearch = new HashSet<String>();
    		for(Map<String,Set<String>> lookup : omimLookups)
    		{
    			if(lookup.containsKey(omimId))
    			{
    				omimSearch.addAll(lookup.get(omimId));
    			}
    		}
    		omimTermForMPMap.get(hdpAnnotationKey).addAll(omimSearch);
        }
        
        logger.info("finished building map of OMIM query terms/synonyms to equivalent MP hdp_annotation_keys");
        return omimTermForMPMap;
    }
    
    private Map<String,Set<String>> populateOMIMForOMIMGridLookup(int start,int end,List<Map<String,Set<String>>> omimLookups) throws Exception
    {
        // load a map of OMIM term IDs to equivalent OMIM IDs via hdp_annotation
        logger.info("building map of OMIM query terms/synonyms to equivalent OMIM hdp_annotation_keys");
        // first we map OMIM ids = OMIM ids, we can use existing lookups to get the rest
        String omimToOmimQuery = "select omim_id,omim_term,hdp_annotation_key " +
        		"from tmp_disease_to_disease " +
        		"where hdp_annotation_key > "+start+" and hdp_annotation_key <= "+end+" ";
        
        ResultSet rs = ex.executeProto(omimToOmimQuery);
        Map<String,Set<String>> omimTermForOMIMMap = new HashMap<String,Set<String>>();
        while(rs.next())
        {
        	String hdpAnnotationKey = rs.getString("hdp_annotation_key");
        	String omimId = rs.getString("omim_id");
        	String omimTerm = rs.getString("omim_term");
        	if(!omimTermForOMIMMap.containsKey(hdpAnnotationKey))
        	{
        		omimTermForOMIMMap.put(hdpAnnotationKey,new HashSet<String>());
        	}
        	omimTermForOMIMMap.get(hdpAnnotationKey).add(omimId);
        	omimTermForOMIMMap.get(hdpAnnotationKey).add(omimTerm);
        	Set<String> omimSearch = new HashSet<String>();
    		for(Map<String,Set<String>> lookup : omimLookups)
    		{
    			if(lookup.containsKey(omimId))
    			{
    				omimSearch.addAll(lookup.get(omimId));
    			}
    		}
        	omimTermForOMIMMap.get(hdpAnnotationKey).addAll(omimSearch);
        }
        
        logger.info("finished building map of OMIM query terms/synonyms to equivalent OMIM hdp_annotation_keys");
        return omimTermForOMIMMap;
    }
    
    // create some temp tables needed to efficiently process results
    private void createTempTables()
    {
    	logger.info("creating temp table of hdp_annotation to hdp_genocluster_key");
    	String genoClusterKeyQuery = "select distinct on(hdp_annotation_key) hdp_annotation_key,hdp_genocluster_key\n" + 
    			"INTO TEMP tmp_ha_genocluster \n" + 
    			"from hdp_annotation ha,\n" + 
    			"hdp_genocluster_genotype gcg\n" + 
    			"where ha.genotype_key=gcg.genotype_key";
    	this.ex.executeVoid(genoClusterKeyQuery);
    	
    	createTempIndex("tmp_ha_genocluster","hdp_annotation_key");
    	createTempIndex("tmp_ha_genocluster","hdp_genocluster_key");
    	logger.info("done creating temp table of hdp_annotation to hdp_genocluster_key");

    	// no normals temp table for hdp
    	logger.info("creating temp table of hdp_annotation minus normals");
		String noNormalsHdpQuery="select * " +
				"INTO TEMP tmp_hdp_annotation_nn " +
				"from hdp_annotation " +
				"where qualifier_type is null ";
		this.ex.executeVoid(noNormalsHdpQuery);
    	createTempIndex("tmp_hdp_annotation_nn","hdp_annotation_key");
    	createTempIndex("tmp_hdp_annotation_nn","genotype_key");
    	createTempIndex("tmp_hdp_annotation_nn","marker_key");
    	createTempIndex("tmp_hdp_annotation_nn","term_key");
    	createTempIndex("tmp_hdp_annotation_nn","term_id");
    	
    	logger.info("creating temp table of hdp_annotation cross hdp_annotation via genocluster");
    	String hdpAnnotationCrossQuery ="select ha1.hdp_annotation_key ha_key1,\n" + 
    			"ha1.term_id term_id1,\n" + 
    			"ha1.term term1,\n" + 
    			"ha1.vocab_name vocab1,\n" + 
    			"ha2.hdp_annotation_key ha_key2,\n" + 
    			"ha2.term_id term_id2,\n" + 
    			"ha2.term term2,\n" + 
    			"ha2.vocab_name vocab2\n" + 
    			"into temp tmp_ha_cross\n" + 
    			"from hdp_annotation ha1,\n" + 
    			"tmp_ha_genocluster gc1,\n" + 
    			"hdp_annotation ha2,\n" + 
    			"tmp_ha_genocluster gc2\n" + 
    			"where ha1.hdp_annotation_key=gc1.hdp_annotation_key\n" + 
    			"and ha2.hdp_annotation_key=gc2.hdp_annotation_key\n" +  
    			"and ha1.term_id != ha2.term_id "+
    			"and gc1.hdp_genocluster_key=gc2.hdp_genocluster_key";
    	this.ex.executeVoid(hdpAnnotationCrossQuery);
    	//createTempIndex("tmp_ha_genocluster","vocab1");
    	//createTempIndex("tmp_ha_genocluster","vocab2");
    	logger.info("done creating temp table of hdp_annotation cross hdp_annotation via genocluster");
    	
    	// disease to disease ID mappings via genotype clusters
    	logger.info("creating temp table mapping disease terms to diseases via genocluster");
    	String diseaseToDiseaseQuery = "select hc.term_id1 omim_id, \n" + 
    			"        	hc.term1 omim_term, \n" + 
    			"			hc.ha_key2 hdp_annotation_key \n" + 
    			"		INTO TEMP tmp_disease_to_disease\n" + 
    			"			from tmp_ha_cross hc \n" + 
    			"			where hc.vocab1='OMIM' \n" + 
    			"				and hc.vocab2='OMIM' ";
    	this.ex.executeVoid(diseaseToDiseaseQuery);
    	createTempIndex("tmp_disease_to_disease","hdp_annotation_key");
    	
    	// disease to mp ID mappings
    	logger.info("creating temp table mapping disease terms to mp IDs via genocluster");
    	String diseaseToMpQuery = "select hc.term_id2 omim_id, \n" + 
    			"        	hc.term2 omim_term, \n" + 
    			"			hc.ha_key1 hdp_annotation_key\n" + 
    			"		INTO TEMP tmp_disease_to_mp\n" + 
    			"			from tmp_ha_cross hc \n" + 
    			"			where hc.vocab1='Mammalian Phenotype' \n" + 
    			"				and hc.vocab2='OMIM'";
    	this.ex.executeVoid(diseaseToMpQuery);
    	createTempIndex("tmp_disease_to_mp","hdp_annotation_key");
    	
    	// mp to disease ID mappings via genotype clusters
    	logger.info("creating temp table mapping mp terms to SS diseases via genocluster");
    	String mpToSSDiseaseQuery = "select hc.term_id1 mp_id, \n" + 
    			"        	hc.term1 mp_term, \n" + 
    			"			hc.ha_key2 hdp_annotation_key\n" + 
    			"		INTO TEMP tmp_mp_to_ss_disease\n" + 
    			"			from tmp_ha_cross hc \n" + 
    			"			where hc.vocab1='Mammalian Phenotype' \n" + 
    			"				and hc.vocab2='OMIM'";
    	this.ex.executeVoid(mpToSSDiseaseQuery);
    	createTempIndex("tmp_mp_to_ss_disease","hdp_annotation_key");
    	
    	logger.info("creating temp table mapping mp terms to diseases via genocluster");
    	String mpToDiseaseQuery = "select distinct ha_mp.term_id mp_id, \n" + 
    			"        	ha_mp.term mp_term, \n" + 
    			"		ha_omim.hdp_annotation_key \n" + 
    			"		INTO TEMP tmp_mp_to_disease\n" + 
    			"			from hdp_annotation ha_mp, \n" + 
    			"				hdp_annotation ha_omim\n" + 
    			"			where ha_mp.genotype_key=ha_omim.genotype_key \n" + 
    			"				and ha_mp.vocab_name='Mammalian Phenotype' \n" + 
    			"				and ha_omim.vocab_name='OMIM'";
    	this.ex.executeVoid(mpToDiseaseQuery);
    	createTempIndex("tmp_mp_to_disease","hdp_annotation_key");
    	
    	// mp to mp ID mappings
    	logger.info("creating temp table mapping mp terms to mp IDs via genocluster");
    	String mpToMpQuery = "select hc.term_id1 mp_id, \n" + 
    			"        	hc.term1 mp_term, \n" + 
    			"			hc.ha_key2 hdp_annotation_key\n" + 
    			"		INTO TEMP tmp_mp_to_mp\n" + 
    			"			from tmp_ha_cross hc \n" + 
    			"			where hc.vocab1='Mammalian Phenotype' \n" + 
    			"				and hc.vocab2='Mammalian Phenotype'";
    	this.ex.executeVoid(mpToMpQuery);
    	createTempIndex("tmp_mp_to_mp","hdp_annotation_key");
    }
    
    /*
     * generates a unique key for a human marker + disease term combo
     * 	This will be used to link human disease annotations in this index to 
     * 	grid data in the diseasePortalAnnotation index.
     * 
     * 	It is intended to be used in a Solr join, not for anything else.
     */
    public static String makeHumanDiseaseKey(Integer markerKey,String omimId)
    {
    	return markerKey + "000000" + omimId;
    }
    
    /*
     * For debugging purposes only
     */
    private long startTime = 0;
    public void startTime()
    {
    	startTime = System.nanoTime();
    }
    public long stopTime()
    {
    	long endTime = System.nanoTime();
    	return (endTime - startTime)/1000000;
    	
    }
}
