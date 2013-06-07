package org.jax.mgi.indexer;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;
import org.jax.org.mgi.shr.fe.util.TextFormat;

/**
 * GXDResultIndexerSQL
 * @author kstone
 * This index is has the primary responsibility of populating the GXD Result solr index.
 * Each document in this index represents an assay result. This index can/will have fields to group by
 * assayKey and markerKey
 * 
 * Note: refactored during 5.x development
 */

public class GXDResultIndexerSQL extends Indexer 
{   
    public GXDResultIndexerSQL () 
    { super("index.url.gxdResult"); }
    
    public void index() throws Exception
    {    
        	Map<String,List<String>> markerNomenMap = new HashMap<String,List<String>>();
        	logger.info("building map of marker searchable nomenclature");
        	String nomenQuery = "select distinct marker_key,term from marker_searchable_nomenclature msn "+
        			"where term_type in ('synonym','related synonym') "+
	        		"and exists(select 1 from expression_result_summary ers where msn.marker_key=ers.marker_key) ";
            ResultSet rs = ex.executeProto(nomenQuery);

	        while (rs.next())
	        {
	        	String mkey = rs.getString("marker_key");
	        	String term = rs.getString("term");
	        	if(!markerNomenMap.containsKey(mkey))
	        	{
	        		markerNomenMap.put(mkey, new ArrayList<String>());
	        	}
	        	markerNomenMap.get(mkey).add(term);
	        }
	        logger.info("done gathering marker nomenclature");
	        
	        Map<String,String> centimorganMap = new HashMap<String,String>();
	        logger.info("building map of marker centimorgans");
        	String centimorganQuery = "select distinct marker_key,cm_offset from marker_location ml "+
        			"where location_type='centimorgans' ";
            rs = ex.executeProto(centimorganQuery);

	        while (rs.next())
	        {
	        	String mkey = rs.getString("marker_key");
	        	String cm_offset = rs.getString("cm_offset");
	        	centimorganMap.put(mkey, cm_offset);
	        }
	        logger.info("done gathering marker centimorgans");
	        
	       
	        Map<String,Map<String,Map<String,String>>> mutatedInMap = new HashMap<String,Map<String,Map<String,String>>>();
        	logger.info("building map of specimen mutated in genes");
        	String mutatedInQuery = "select m.marker_key,m.symbol,m.name,ag.genotype_key from marker m, marker_to_allele ma, allele_to_genotype ag "+
 	        		"where ag.allele_key=ma.allele_key and ma.marker_key=m.marker_key";
            rs = ex.executeProto(mutatedInQuery);

	        while (rs.next())
	        {
	        	String gkey = rs.getString("genotype_key");
	        	String mkey = rs.getString("marker_key");
	        	String symbol = rs.getString("symbol");
	        	String name = rs.getString("name");
	        	
	        	if(!mutatedInMap.containsKey(gkey))
	        	{
	        		Map<String,Map<String,String>> genotype = new HashMap<String,Map<String,String>>();
	        		genotype.put(mkey, new HashMap<String,String>());
	        		mutatedInMap.put(gkey, genotype);
	        	}
	        	if(!mutatedInMap.get(gkey).containsKey(mkey))
	        	{
	        		mutatedInMap.get(gkey).put(mkey, new HashMap<String,String>());
	        	}
	        	mutatedInMap.get(gkey).get(mkey).put("symbol",symbol);
	        	mutatedInMap.get(gkey).get(mkey).put("name",name);
	        }
	        logger.info("done gathering specimen mutated in genes");
	        
	        Map<String,List<String>> mutatedInAlleleMap = new HashMap<String,List<String>>();
        	logger.info("building map of specimen mutated in allele IDs");
        	String mutatedInAlleleQuery = "select a.primary_id allele_id, ag.genotype_key from allele a, allele_to_genotype ag "+
 	        		"where ag.allele_key=a.allele_key";
            rs = ex.executeProto(mutatedInAlleleQuery);

	        while (rs.next())
	        {
	        	String gkey = rs.getString("genotype_key");
	        	String alleleId = rs.getString("allele_id");
	        	
	        	if(!mutatedInAlleleMap.containsKey(gkey))
	        	{
	        		mutatedInAlleleMap.put(gkey, new ArrayList<String>());
	        	}
	        	mutatedInAlleleMap.get(gkey).add(alleleId);
	        }
	        logger.info("done gathering specimen mutated in genes");
	        
	        Map<String,List<String>> markerVocabMap = new HashMap<String,List<String>>();
	        logger.info("building map of vocabulary annotations");
	        String vocabQuery = SharedQueries.GXD_VOCAB_EXPRESSION_QUERY;
	        rs = ex.executeProto(vocabQuery);

	        while (rs.next())
	        {
	        	String mkey = rs.getString("marker_key");
	        	String termId = rs.getString("term_id");
	        	if(!markerVocabMap.containsKey(mkey))
	        	{
	        		markerVocabMap.put(mkey, new ArrayList<String>());
	        	}
	        	markerVocabMap.get(mkey).add(termId);
	        }
	        logger.info("done gathering vocab annotations");
	        
	        Map<String,List<String>> vocabAncestorMap = new HashMap<String,List<String>>();
	        logger.info("building map of vocabulary term ancestors");
	        String vocabAncestorQuery = SharedQueries.GXD_VOCAB_ANCESTOR_QUERY;
	        rs = ex.executeProto(vocabAncestorQuery);

	        while (rs.next())
	        {
	        	String termId = rs.getString("primary_id");
	        	String ancestorId = rs.getString("ancestor_primary_id");

	        	if(!vocabAncestorMap.containsKey(termId))
	        	{
	        		vocabAncestorMap.put(termId, new ArrayList<String>());
	        	}
	        	vocabAncestorMap.get(termId).add(ancestorId);
	        }
	        logger.info("done gathering vocab term ancestors");
	        
	        Map<String,List<String>> imageMap = new HashMap<String,List<String>>();
	        logger.info("building map of expression images");
	        // label could be either specimen label or if null use the figure label
	        String imageQuery = "select eri.result_key,  " +
	        		"case when ei.pane_label is null then i.figure_label else (i.figure_label || ei.pane_label) end as label "+
	        		"from expression_result_summary ers, " +
	        		"expression_result_to_imagepane eri, "+
	        		"expression_imagepane ei, "+
	        		"image i "+
	        		"where eri.imagepane_key=ei.imagepane_key "+
						"and ei.image_key=i.image_key "+
						"and eri.result_key=ers.result_key "+
						"and ers.specimen_key is null "+
					"UNION "+
					"select ers.result_key, sp.specimen_label as label "+
						"from expression_result_summary ers,assay_specimen sp "+
						"where ers.specimen_key=sp.specimen_key ";
	        rs = ex.executeProto(imageQuery);

	        while (rs.next())
	        {
	        	String rkey = rs.getString("result_key");
	        	String label = rs.getString("label");
	        	if(label != null && !label.equals(""))
	        	{
	        		if(!imageMap.containsKey(rkey))
	        		{
	        			imageMap.put(rkey, new ArrayList<String>());
	        		}
	        		imageMap.get(rkey).add(label);
	        	}
	        }
	        logger.info("done gathering expression images");
	        
	        Map<String,List<String>> structureAncestorIdMap = new HashMap<String,List<String>>();
	        Map<String,List<String>> structureAncestorKeyMap = new HashMap<String,List<String>>();
	        logger.info("building map of structure ancestors");
	        String structureAncestorQuery =SharedQueries.GXD_ANATOMY_ANCESTOR_QUERY;
	        rs = ex.executeProto(structureAncestorQuery);

	        while (rs.next())
	        {
	        	String skey = rs.getString("structure_term_key");
	        	String ancestorId = rs.getString("ancestor_id");
	        	String structureId = rs.getString("structure_id");
	        	String mgdKey = rs.getString("ancestor_mgd_structure_key");
	        	if(!structureAncestorIdMap.containsKey(skey))
	        	{
	        		structureAncestorIdMap.put(skey, new ArrayList<String>());
	        		// Include original term
	        		structureAncestorIdMap.get(skey).add(structureId);
	        		structureAncestorKeyMap.put(skey,new ArrayList<String>());
	        	}
	        	structureAncestorIdMap.get(skey).add(ancestorId);
        		structureAncestorKeyMap.get(skey).add(mgdKey);
	        }
	        logger.info("done gathering structure ancestors");
	        
	        Map<String,List<String>> structureSynonymMap = new HashMap<String,List<String>>();
	        logger.info("building map of structure synonyms");
	        String structureSynonymQuery =SharedQueries.GXD_ANATOMY_SYNONYMS_QUERY;
	        rs = ex.executeProto(structureSynonymQuery);

	        while (rs.next())
	        {
	        	String sId = rs.getString("structure_id");
	        	String synonym = rs.getString("synonym");
	        	String structure = rs.getString("structure");
	        	if(!structureSynonymMap.containsKey(sId))
	        	{
	        		structureSynonymMap.put(sId, new ArrayList<String>());
	        		// Include original term
	        		structureSynonymMap.get(sId).add(structure);
	        	}
	        	if(synonym!=null && !synonym.equals("null"))
	        	{
	        		structureSynonymMap.get(sId).add(synonym);
	        	}
	        }
	        logger.info("done gathering structure synonyms");
	        
        	ResultSet rs_tmp = ex.executeProto("select max(result_key) as max_result_key from expression_result_summary");
        	rs_tmp.next();
        	
        	Integer start = 0;
            Integer end = rs_tmp.getInt("max_result_key");
        	int chunkSize = 150000;
            
            int modValue = end.intValue() / chunkSize;
            
            // Perform the chunking, this might become a configurable value later on
            // TODO Figure out whether or not to make this configurable.

            logger.info("Getting all assay results and related search criteria");
            
            long commit_every=50000;
            long record_count=0;
            for (int i = 0; i <= modValue; i++) {
            
	            start = i * chunkSize;
	            end = start + chunkSize;
	            
	            logger.info ("Processing result key > " + start + " and <= " + end);
	            String query = "select distinct ers.result_key,ers.marker_key, ers.assay_key,ers.assay_type, tae.mgd_structure_key,ers.structure_key, "+
	            		"ers.theiler_stage, ers.is_expressed, ers.has_image, "+
		            	"ers.structure_printname, "+
						"ers.age_abbreviation, ers.anatomical_system, ers.jnum_id, "+
						"ers.detection_level,ers.marker_symbol,ers.assay_id, ers.age_min,ers.age_max,ers.pattern, "+
						"ers.is_wild_type, ers.genotype_key, "+
						"m.marker_subtype, m.name marker_name,m.primary_id marker_id, "+
						"ml.chromosome,ml.cytogenetic_offset,ml.start_coordinate,ml.end_coordinate,ml.strand, "+
						"r.mini_citation, r.pubmed_id, "+
						"g.combination_1 genotype, "+
						"structure.primary_id structure_id, "+
	                    "msqn.by_symbol, "+
	                    "msqn.by_location, "+
	            		"ersn.by_assay_type r_by_assay_type, "+
	            		"ersn.by_gene_symbol r_by_gene_symbol, "+
	            		"ersn.by_anatomical_system r_by_anatomical_system, "+
	            		"ersn.by_age r_by_age, "+
	            		"ersn.by_expressed r_by_expressed, "+
	            		"ersn.by_structure r_by_structure, "+
	            		"ersn.by_mutant_alleles r_by_mutant_alleles, "+
	            		"ersn.by_reference r_by_reference, "+
	            		"easn.by_symbol a_by_symbol, "+
	            		"easn.by_assay_type a_by_assay_type, "+
	            		"exa.has_image a_has_image, "+
	            		"exa.probe_key a_probe_key, "+
	            		"exa.antibody_key a_antibody_key "+
	            		"from expression_result_summary ers, "+
	            		"marker_sequence_num msqn, "+
	                    "marker_counts mc, "+
	            		"marker m, "+
	                    "marker_location ml, "+
	                    "reference r, "+
	            		"genotype g, "+
	                    "term structure,  "+
	            		"term_anatomy_extras tae, "+
	                    "expression_assay exa, "+
	                    "expression_assay_sequence_num easn, "+
	            		"expression_result_sequence_num ersn "+
	            		"where ers.marker_key=msqn.marker_key "+ 
	            		"and ers.marker_key = ml.marker_key "+
	            		"and ml.sequence_num=1 "+
	            		"and ers.reference_key=r.reference_key "+
	            		"and ers.genotype_key=g.genotype_key "+
	                    "and ers.marker_key=mc.marker_key "+
	            		"and ers.marker_key=m.marker_key "+
	            		"and ersn.result_key=ers.result_key "+
	            		"and exa.assay_key=ers.assay_key "+
	            		"and easn.assay_key=ers.assay_key "+
	                    "and ers.assay_type != 'Recombinase reporter'"+
	                    "and ers.assay_type != 'In situ reporter (transgenic)'"+
	                    "and mc.gxd_literature_count>0 "+
	                    "and ers.structure_key=structure.term_key "+
	                    "and ers.structure_key=tae.term_key "+
	                    "and ers.result_key > "+start+" and ers.result_key <= "+end+" ";
	            rs = ex.executeProto(query);
	            
	            //Map<String,SolrInputDocument> docs = new HashMap<String,SolrInputDocument>();
	            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	            
	            logger.info("Parsing them");
	            while (rs.next()) 
	            {           
	            	record_count++;
	            	String marker_key = rs.getString("marker_key");
	                // the marker symbol sort
	            	String by_symbol = rs.getString("by_symbol");	            	
	            	String by_location = rs.getString("by_location");
	                String result_key = rs.getString("result_key");
	                String assay_key = rs.getString("assay_key");
	                
	                String assay_type = rs.getString("assay_type");
	                //result fields
	                String theiler_stage = rs.getString("theiler_stage");
	                String is_expressed = rs.getString("is_expressed");
	                String detection_level = rs.getString("detection_level");
	                String printname = rs.getString("structure_printname");
	                String structureTermKey = rs.getString("structure_key");
	                String mgd_structure_key = rs.getString("mgd_structure_key");
	                String age = rs.getString("age_abbreviation");
	                String assay_id = rs.getString("assay_id");
	                String anatomical_system = rs.getString("anatomical_system");
	                String jnum = rs.getString("jnum_id");
	                String mini_citation = rs.getString("mini_citation");
	                String genotype = rs.getString("genotype");
	                if(genotype != null) genotype = genotype.replace("<sup>","<").replace("</sup>",">");
	                String has_image = rs.getString("has_image");
	                
	                String markerSymbol = rs.getString("marker_symbol");
	                String markerName = rs.getString("marker_name");
	                String markerType = rs.getString("marker_subtype");
	                String markerMgiid = rs.getString("marker_id");
	                
	                String chr = rs.getString("chromosome");
	                String cm_offset = "";
	                if(centimorganMap.containsKey(marker_key))
	                {
	                	cm_offset = centimorganMap.get(marker_key);
	                }
	                String cytogenetic_offset = rs.getString("cytogenetic_offset");
	                String start_coord = rs.getString("start_coordinate");
	                String end_coord = rs.getString("end_coordinate");
	                String strand = rs.getString("strand");


	               // String printname = rs_overall.getString("printname");
	                
	                //result sorts
	                String r_by_assay_type = rs.getString("r_by_assay_type");
	                String r_by_gene_symbol = rs.getString("r_by_gene_symbol");
	                String r_by_anatomical_system = rs.getString("r_by_anatomical_system");
	                String r_by_age = rs.getString("r_by_age");
	                String r_by_structure = rs.getString("r_by_structure");
	                String r_by_expressed = rs.getString("r_by_expressed");
	                String r_by_mutant_alleles = rs.getString("r_by_mutant_alleles");
	                String r_by_reference = rs.getString("r_by_reference");
	                
	                //assay summary
	                String a_has_image = rs.getString("a_has_image");
	                String a_probe_key = rs.getString("a_probe_key");
	                String a_antibody_key = rs.getString("a_antibody_key");
	                
	                // assay sorts
	                String a_by_symbol = rs.getString("a_by_symbol");
	                String a_by_assay_type = rs.getString("a_by_assay_type");

	                String unique_key = assay_type+"-"+result_key;
	                if(unique_key==null || unique_key.equals("-"))
	                { continue; }
	                
	                SolrInputDocument doc = new SolrInputDocument();
//	                if(docs.containsKey(unique_key))
//	                {
//	                	doc = docs.get(unique_key);
//	                }
//	                else
//	                {
	                	// Add the single value fields
		                doc.addField(GxdResultFields.KEY, unique_key);
		                doc.addField(GxdResultFields.MARKER_KEY, marker_key);
	                    doc.addField(IndexConstants.MRK_BY_SYMBOL,by_symbol); 
	                    doc.addField(GxdResultFields.M_BY_LOCATION,by_location); 
		                doc.addField(GxdResultFields.ASSAY_KEY, assay_key);
		                doc.addField(GxdResultFields.RESULT_KEY,result_key);
		                doc.addField(GxdResultFields.RESULT_TYPE, assay_type);
		                doc.addField(GxdResultFields.ASSAY_TYPE, assay_type);
		                doc.addField(GxdResultFields.THEILER_STAGE, theiler_stage);
		                doc.addField(GxdResultFields.IS_EXPRESSED, is_expressed);
		                doc.addField(GxdResultFields.AGE_MIN, roundAge(rs.getString("age_min")));
		                doc.addField(GxdResultFields.AGE_MAX, roundAge(rs.getString("age_max")));
		                
		                boolean isWildType = rs.getString("is_wild_type").equals("1") || rs.getString("genotype_key").equals("-1");
		                doc.addField(GxdResultFields.IS_WILD_TYPE, isWildType);

		                // marker summary
		                doc.addField(GxdResultFields.MARKER_MGIID,markerMgiid);
		                doc.addField(GxdResultFields.MARKER_SYMBOL,markerSymbol);
		                doc.addField(GxdResultFields.MARKER_NAME,markerName);
		                // also add symbol and current name to searchable nomenclature
		                doc.addField(GxdResultFields.NOMENCLATURE, markerSymbol);
		                doc.addField(GxdResultFields.NOMENCLATURE, markerName);
		                doc.addField(GxdResultFields.MARKER_TYPE,markerType);
		                
		                //location stuff
		                doc.addField(GxdResultFields.CHROMOSOME,chr);
		                doc.addField(GxdResultFields.START_COORD,start_coord);
		                doc.addField(GxdResultFields.END_COORD,end_coord);
		                doc.addField(GxdResultFields.CYTOBAND, cytogenetic_offset);
		                doc.addField(GxdResultFields.STRAND,strand);
		                
		                if(cm_offset == null || cm_offset.equals("-1")) cm_offset = "";
		                doc.addField(GxdResultFields.CENTIMORGAN,cm_offset);
		                
		                //assay summary
		                doc.addField(GxdResultFields.ASSAY_HAS_IMAGE, a_has_image.equals("1"));
		                doc.addField(GxdResultFields.PROBE_KEY, a_probe_key);
		                doc.addField(GxdResultFields.ANTIBODY_KEY, a_antibody_key);
		                
		                // assay sorts
		                doc.addField(GxdResultFields.A_BY_SYMBOL,a_by_symbol);
		                doc.addField(GxdResultFields.A_BY_ASSAY_TYPE,a_by_assay_type);
		                
		                // result summary
		                doc.addField(GxdResultFields.DETECTION_LEVEL,mapDetectionLevel(detection_level));
		                doc.addField(GxdResultFields.STRUCTURE_PRINTNAME,printname);
		                doc.addField(GxdResultFields.AGE,age);
		                doc.addField(GxdResultFields.ASSAY_MGIID,assay_id);
		                doc.addField(GxdResultFields.JNUM,jnum);
		                doc.addField(GxdResultFields.PUBMED_ID,rs.getString("pubmed_id"));
		                doc.addField(GxdResultFields.ANATOMICAL_SYSTEM,anatomical_system);
		                doc.addField(GxdResultFields.SHORT_CITATION,mini_citation);
		                doc.addField(GxdResultFields.GENOTYPE,genotype);
		                doc.addField(GxdResultFields.PATTERN,rs.getString("pattern"));
		                
		                // multi values
		                if(markerNomenMap.containsKey(marker_key))
		                {
		                	for(String nomen : markerNomenMap.get(marker_key))
		                	{

				                doc.addField(GxdResultFields.NOMENCLATURE, nomen);
		                	}
		                }
		                String genotype_key = rs.getString("genotype_key");
		                if(mutatedInMap.containsKey(genotype_key))
		                {
		                	Map<String,Map<String,String>> gMap = mutatedInMap.get(genotype_key);
		                	for(String genotype_marker_key : gMap.keySet())
		                	{
			                	doc.addField(GxdResultFields.MUTATED_IN, gMap.get(genotype_marker_key).get("symbol"));
			                	doc.addField(GxdResultFields.MUTATED_IN, gMap.get(genotype_marker_key).get("name"));
			                	// get any synonyms
			                	if (markerNomenMap.containsKey(genotype_marker_key))
			                	{
			                		for(String synonym : markerNomenMap.get(genotype_marker_key))
			                		{
			                			doc.addField(GxdResultFields.MUTATED_IN, synonym);
			                		}
			                	}
		                	}
		                	
		                }
		                if(mutatedInAlleleMap.containsKey(genotype_key))
		                {
		                	List<String> alleleIds = mutatedInAlleleMap.get(genotype_key);
		                	for(String alleleId : alleleIds)
		                	{
			                	doc.addField(GxdResultFields.ALLELE_ID, alleleId);
		                	}
		                	
		                }
		                if(markerVocabMap.containsKey(marker_key))
		                {
		                	Set<String> uniqueAnnotationIDs = new HashSet<String>();
		                	for(String termId : markerVocabMap.get(marker_key))
		                	{
				                uniqueAnnotationIDs.add(termId);
				                if(vocabAncestorMap.containsKey(termId))
				                {
				                	for(String ancestorId : vocabAncestorMap.get(termId))
				                	{
				                		uniqueAnnotationIDs.add(ancestorId);
				                	}
				                }
		                	}
		                	for(String annotationID : uniqueAnnotationIDs)
		                	{
		                		doc.addField(GxdResultFields.ANNOTATION, annotationID);
		                	}
		                }
		                if(imageMap.containsKey(result_key))
		                {
		                	List<String> figures = imageMap.get(result_key);
		                	for(String figure : figures)
		                	{
		                		if(has_image.equals("1"))
		                		{
		                			doc.addField(GxdResultFields.FIGURE,figure);
			                		doc.addField(GxdResultFields.FIGURE_PLAIN,figure);
		                		}
		                	}
		                }
		                
		                String structureID = rs.getString("structure_id");

		                Set<String> ancestorIDs = new HashSet<String>();
		                ancestorIDs.add(structureID);
		                Set<String> ancestorStructures = new HashSet<String>();
		                ancestorStructures.add(printname);
		                
		                if(structureAncestorIdMap.containsKey(structureTermKey))
		                {
		                	// get ancestors
		                	List<String> structure_ancestor_ids = structureAncestorIdMap.get(structureTermKey);
		                	for (String structure_ancestor_id : structure_ancestor_ids)
		                	{
		                		// get synonyms for each ancestor/term
		                		if(structureSynonymMap.containsKey(structure_ancestor_id))
		                		{
		                			//also add structure MGI ID
		                			ancestorIDs.add(structure_ancestor_id);
			                		for (String structureSynonym : structureSynonymMap.get(structure_ancestor_id))
			                		{
			                			ancestorStructures.add(structureSynonym);
			                		}
		                		}
		                	}
		                	// only add unique structures (for best solr indexing performance)
		                	for(String ancestorId : ancestorIDs)
		                	{
		                		doc.addField(GxdResultFields.STRUCTURE_ID, ancestorId);
		                	}
		                	for(String ancestorStructure : ancestorStructures)
		                	{
		                		doc.addField(GxdResultFields.STRUCTURE_ANCESTORS, ancestorStructure);
		                	}
		                }
		                
		                // add the synonyms for this exact annotated structure
		                Set<String> exactStructures = new HashSet<String>();
		                exactStructures.add(printname);
		                if(structureSynonymMap.containsKey(structureID))
		                {
		                	for (String structureSynonym : structureSynonymMap.get(structureID))
	                		{
		                		exactStructures.add(structureSynonym);
	                		}
		               	}
		                for(String exactStructure : exactStructures)
		                {
		                	doc.addField(GxdResultFields.STRUCTURE_EXACT, exactStructure);
		                }
		                
		                Set<String> structureKeys = new HashSet<String>();
		                structureKeys.add(mgd_structure_key);
		                doc.addField(GxdResultFields.ANNOTATED_STRUCTURE_KEY, mgd_structure_key);
		                if(structureAncestorKeyMap.containsKey(structureTermKey))
		                {
		                	// get ancestors by key as well (for links from AD browser)
		                	for (String structureAncestorKey : structureAncestorKeyMap.get(structureTermKey))
		                	{
		                		structureKeys.add(structureAncestorKey);
		                	}
		                }
		                for(String structKey : structureKeys)
		                {
		                	doc.addField(GxdResultFields.STRUCTURE_KEY, structKey);
		                }
		                
		                //result sorts
		                doc.addField(GxdResultFields.R_BY_ASSAY_TYPE,r_by_assay_type);
		                doc.addField(GxdResultFields.R_BY_MRK_SYMBOL,r_by_gene_symbol);
		                doc.addField(GxdResultFields.R_BY_ANATOMICAL_SYSTEM,r_by_anatomical_system);
		                doc.addField(GxdResultFields.R_BY_AGE,r_by_age);
		                doc.addField(GxdResultFields.R_BY_STRUCTURE,r_by_structure);
		                doc.addField(GxdResultFields.R_BY_EXPRESSED,r_by_expressed);
		                doc.addField(GxdResultFields.R_BY_MUTANT_ALLELES,r_by_mutant_alleles);
		                doc.addField(GxdResultFields.R_BY_REFERENCE,r_by_reference);
		                
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
		                   // docs = new HashMap<String,SolrInputDocument>();
		                    docs = new ArrayList<SolrInputDocument>();
		                    //logger.info("Done adding to solr, Moving on");
//		                    if(record_count >= commit_every)
//		                    {
//		                    	record_count = 0;
//			                    startTime();
//			                    server.commit();
//			                    logger.info("time to run commit on "+commit_every+" records= "+stopTime());
//		                    }
		                }
	            }
	            if (! docs.isEmpty()) {
	                server.add(docs);
	            }
	            
	            server.commit();
            }
    }
    
    // maps detection level to currently approved display text.
    public String mapDetectionLevel(String level)
    {
    	List<String> detectedYesLevels = Arrays.asList("Present","Trace","Weak","Moderate","Strong","Very strong");
    	if(level.equals("Absent")) return "No";
    	else if (detectedYesLevels.contains(level)) return "Yes";
    	
    	return level;
    }
    
    public Double roundAge(String ageStr)
    {
    	if(ageStr != null)
    	{
	    	Double age = Double.parseDouble(ageStr);
	    	Double ageInt = Math.floor(age);
	    	Double ageDecimal = age - ageInt;
	    	// try the rounding to nearest 0.5
	    	if(ageDecimal < 0.25) ageDecimal = 0.0;
	    	else if (ageDecimal < 0.75) ageDecimal = 0.5;
	    	else ageDecimal = 1.0;
	    	return ageInt + ageDecimal;
    	}
    	// not sure what to do here... age should never be null.
    	return -1.0;
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
