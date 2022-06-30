package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.DistinctSolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.GxdHtFields;

/** Is: the indexer for samples of high-throughput expression experiments
 *  Does: populates the gxdHtSample solr index
 *  Notes: added during the GXD HT project (TR12370)
 */
public class GXDHtSampleIndexerSQL extends Indexer {

	//--- instance variables ---//

	HashMap<String, HashSet<String>> refIDs = null;			// maps experiment keys to reference IDs
	HashMap<String, HashSet<String>> notes = null;			// maps sample keys to notes
	HashMap<String, HashSet<String>> experimentIDs = null;	// maps experiment keys to exp. IDs
	HashMap<String,String> strains = null;					// maps genotype key to strain
	HashMap<String,String> alleles = null;					// maps genotype key to allele combination
	HashMap<String, HashSet<String>> markerData = null;		// maps sample key to marker symbols, IDs, and synonyms
	HashMap<String, HashSet<String>> variables = null;		// maps sample key to experimental variables
	HashMap<String,String> terms = null;					// maps term key to term (EMAPA)
	HashMap<String,String> termIDs = null;					// maps term key to term ID (EMAPA)
	HashMap<String,String> clTerms = null;					// maps term key to term (Cell type)
	HashMap<String,String> clTermIDs = null;					// maps term key to term ID (Cell type)
	HashMap<String, HashSet<String>> termStrings = null;	// maps term key to terms, IDs, and synonyms
	HashSet<String> conditionalGenotypes = null;		// set of genotype keys for conditional genotypes

	// maps EMAPA term key to stage key to ancestor term keys (including self), so we ensure that EMAPA
	// ancestry is stage-aware
	HashMap<String, HashMap<String, HashSet<String>>> ancestors = null;
	
	//--- methods ---//
	
	public GXDHtSampleIndexerSQL() {
		super("gxdHtSample");
	}

	private void cacheReferenceIDs() throws Exception {
		// look up PubMed IDs and J: numbers for each experiment
		String cmd0 = "select experiment_key, value as ref_id "
			+ "from expression_ht_experiment_property "
			+ "where name = 'PubMed ID' "
			+ "union "
			+ "select p.experiment_key, r.jnum_id "
			+ "from expression_ht_experiment_property p, reference r "
			+ "where p.name = 'PubMed ID' "
			+ "and p.value = r.pubmed_id";
		this.refIDs = makeHash(cmd0, "experiment_key", "ref_id");
		logger.info("Retrieved references for " + this.refIDs.size() + " experiments");
	}
	
	private void cacheNotes() throws Exception {
		// look up notes for each sample
		String cmd1 = "select sample_key, note from expression_ht_sample_note";
		this.notes = makeHash(cmd1, "sample_key", "note");
		logger.info("Retrieved sample notes for " + this.notes.size() + " samples");
	}
	
	// add an "Is Conditional" note to each genotype where applicable
	private void addConditionalGenotypeNotes(String sampleKey) {
		// prepend to existing notes
		if (this.notes.containsKey(sampleKey)) {
			HashSet<String> myNotes = this.notes.get(sampleKey);
			// It's a set, but there's only one member.
			for (String note : myNotes) {
				myNotes.remove(note);
				myNotes.add("Conditional mutant. " + note);
			}
		} else {
			// no other note, so add this one
			HashSet<String> myNotes = new HashSet<String>();
			myNotes.add("Conditional mutant.");
			this.notes.put(sampleKey, myNotes);
		}
	}

	private void cacheExperimentData() throws Exception {
		// look up IDs and variables for each experiment
		String cmd2 = "select experiment_key, acc_id from expression_ht_experiment_id";
		this.experimentIDs = makeHash(cmd2, "experiment_key", "acc_id");
		logger.info("Retrieved IDs for " + this.experimentIDs.size() + " experiments");

		String cmd2a = "select experiment_key, variable from expression_ht_experiment_variable";
		this.variables = makeHash(cmd2a, "experiment_key", "variable");
		logger.info("Retrieved variables for " + this.variables.size() + " experiments");
	}
	
	private void cacheGenotypeData() throws Exception {
		// look up genotype info (strain + alleles) for genotypes tied to samples
		this.strains = new HashMap<String,String>();
		this.alleles = new HashMap<String,String>();
		this.conditionalGenotypes = new HashSet<String>();

		String cmd3 = "select s.genotype_key, g.background_strain, g.combination_1, g.is_conditional "
			+ "from expression_ht_sample s, genotype g "
			+ "where s.genotype_key = g.genotype_key";
		ResultSet rs3 = ex.executeProto(cmd3);
		while (rs3.next()) {
			String genotypeKey = rs3.getString("genotype_key");
			this.strains.put(genotypeKey, rs3.getString("background_strain"));
			this.alleles.put(genotypeKey, rs3.getString("combination_1"));
			if (rs3.getInt("is_conditional") != 0) {
				this.conditionalGenotypes.add(genotypeKey);
			}
		}
		rs3.close();
		logger.info("Retrieved straings and alleles for " + this.strains.size() + " genotypes");
	}
	
	private void cacheMarkerData() throws Exception {
		// look up symbols, synonyms, ID for mutated gene in each genotype
		this.markerData = new HashMap<String, HashSet<String>>();

		String cmd4 = "select distinct s.sample_key, m.symbol, m.primary_id, y.synonym "
			+ "from expression_ht_sample s "
			+ "inner join allele_to_genotype atg on (s.genotype_key = atg.genotype_key) "
			+ "inner join marker_to_allele mta on (atg.allele_key = mta.allele_key) "
			+ "inner join marker m on (mta.marker_key = m.marker_key) "
			+ "left outer join marker_synonym y on (m.marker_key = y.marker_key)";
		ResultSet rs4 = ex.executeProto(cmd4);
		while (rs4.next()) {
			String sampleKey = rs4.getString("sample_key");
			if (!this.markerData.containsKey(sampleKey)) {
				this.markerData.put(sampleKey, new HashSet<String>());
			}
			this.markerData.get(sampleKey).add(rs4.getString("symbol"));
			this.markerData.get(sampleKey).add(rs4.getString("primary_id"));
			if (rs4.getString("synonym") != null) {
				this.markerData.get(sampleKey).add(rs4.getString("synonym"));
			}
		}
		rs4.close();
		logger.info("Retrieved mutated genes for " + this.markerData.size() + " samples");
	}
	
	private void cacheCellTypeData() throws Exception {

                logger.info("Caching Cell Type info.");

		this.clTerms = new HashMap<String,String>();
		this.clTermIDs = new HashMap<String,String>();

		String cmd7 = "select t.term_key, t.primary_id, t.term "
			+ "from term t "
			+ "where t.vocab_name = 'Cell Ontology'";
		
		ResultSet rs7 = ex.executeProto(cmd7);
		while (rs7.next()) {
			String termKey = rs7.getString("term_key");
			String term = rs7.getString("term");
                        String termID = rs7.getString("primary_id");

                        this.clTerms.put(termKey,term);
                        this.clTermIDs.put(termKey,termID);
                }
                rs7.close();
                logger.info("Got terms, IDs, synonyms for " + this.clTerms.size() + " CL terms");
        }

	private void cacheEmapaData() throws Exception {
		// look up terms, IDs, synonyms for structure and its ancestors;
		// also look up ancestors (via DAG) for each term; is stage-aware when traversing DAG
		
		String emapTable = SharedQueries.createEmapTempTable(logger, ex);
		this.terms = new HashMap<String,String>();
		this.termIDs = new HashMap<String,String>();
		this.termStrings = new HashMap<String,HashSet<String>>();
		this.ancestors = new HashMap<String,HashMap<String,HashSet<String>>>();
		
		String cmd5 = "select t.term_key, t.primary_id, t.term, s.synonym "
			+ "from term t "
			+ "left outer join term_synonym s on (t.term_key = s.term_key) "
			+ "where t.vocab_name = 'EMAPA'";
		
		ResultSet rs5 = ex.executeProto(cmd5);
		while (rs5.next()) {
			String termKey = rs5.getString("term_key");
			if (!this.terms.containsKey(termKey)) {
				this.terms.put(termKey, rs5.getString("term"));
				this.termIDs.put(termKey, rs5.getString("primary_id"));
				
				// seed term and ID into set of searchable strings for this term
				this.termStrings.put(termKey, new HashSet<String>());
				this.termStrings.get(termKey).add(rs5.getString("term"));
				this.termStrings.get(termKey).add(rs5.getString("primary_id"));
			}
			this.termStrings.get(termKey).add(rs5.getString("synonym"));
		}
		rs5.close();
		logger.info("Got terms, IDs, synonyms for " + this.terms.size() + " EMAPA terms");
		
		String cmd6 = "select t.term_key, a.emapa_ancestor_key, a.stage "
			+ "from term t, " + emapTable + " a "
			+ "where t.term_key = a.emapa_descendant_key";

		ResultSet rs6 = ex.executeProto(cmd6);
		while (rs6.next()) {
			String stage = rs6.getString("stage");
			String termKey = rs6.getString("term_key");
			String ancestorKey = rs6.getString("emapa_ancestor_key");
			
			if (!this.ancestors.containsKey(termKey)) {
				this.ancestors.put(termKey, new HashMap<String,HashSet<String>>());
			}
			HashMap<String,HashSet<String>> stageMap = this.ancestors.get(termKey);

			if (!stageMap.containsKey(stage)) {
				stageMap.put(stage, new HashSet<String>());
			}
			HashSet<String> ancestorSet = stageMap.get(stage);
			
			if (!ancestorSet.contains(termKey)) {
				ancestorSet.add(termKey);
			}
			if (!ancestorSet.contains(ancestorKey)) {
				ancestorSet.add(ancestorKey);
			}
		}
		rs6.close();
		logger.info("Got ancestors for " + this.ancestors.size() + " EMAPA terms");
	}
	
	private String getTerm (String termKey) {
		// return term corresponding to termKey
		if (this.terms.containsKey(termKey)) {
			return this.terms.get(termKey);
		}
		if (this.clTerms.containsKey(termKey)) {
			return this.clTerms.get(termKey);
		}
		return null;
	}
	
	private String getTermID (String termKey) {
		// return term ID corresponding to termKey
		if (this.termIDs.containsKey(termKey)) {
			return this.termIDs.get(termKey);
		}
		if (this.clTermIDs.containsKey(termKey)) {
			return this.clTermIDs.get(termKey);
		}
		return null;
	}
	
	private HashSet<String> getTermStrings (String termKey) {
		// return all strings for the specified termKey (term, ID, synonyms)
		if (this.termStrings.containsKey(termKey)) {
			return this.termStrings.get(termKey);
		}
		return new HashSet<String>();
	}
	
	private HashSet<String> getTermStringsWithAncestors (String stage, String termKey) {
		// return all strings (term, ID, synonyms) for the specified termKey/stage pair, plus its ancestors
		if (!this.ancestors.containsKey(termKey)) {
			return new HashSet<String>();
		}
		if (!this.ancestors.get(termKey).containsKey(stage)) {
			return new HashSet<String>();
		}
		
		HashSet<String> out = new HashSet<String>();
		for (String ancestorKey : this.ancestors.get(termKey).get(stage)) {
			out.addAll(this.getTermStrings(ancestorKey));
		}
		return out;
	}

	public void index() throws Exception {
		logger.info("Beginning index() method");
		
		cacheReferenceIDs();	
		cacheNotes();
		cacheExperimentData();
		cacheGenotypeData();
		cacheMarkerData();
		cacheEmapaData();
		cacheCellTypeData();
		
		// main query for sample data
		String cmd = "select s.sample_key, s.emapa_key, s.theiler_stage, s.celltype_key, s.genotype_key, s.age, s.sex, "
			+ "  e.method, s.name as sample_name, s.organism, s.sequence_num, e.experiment_key, "
			+ "  e.name as title, e.description, s.genotype_key, s.age_min, s.age_max, e.study_type, s.relevancy "
			+ "from expression_ht_sample s, expression_ht_experiment e "
			+ "where s.experiment_key = e.experiment_key";

		logger.info("Processing samples...");
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		ResultSet rs = ex.executeProto(cmd, 5000);
		
		while (rs.next()) {
			String sampleKey = rs.getString("sample_key");
			String exptKey = rs.getString("experiment_key");
			String genotypeKey = rs.getString("genotype_key");
			String emapaKey = rs.getString("emapa_key");
                        String celltypeKey = rs.getString("celltype_key");
			String stage = rs.getString("theiler_stage");
			
			DistinctSolrInputDocument doc = new DistinctSolrInputDocument();
			doc.addField(GxdHtFields.SAMPLE_KEY, sampleKey);
			doc.addField(GxdHtFields.EXPERIMENT_KEY, exptKey);
			doc.addField(GxdHtFields.NAME, rs.getString("sample_name"));
			doc.addField(GxdHtFields.TITLE, rs.getString("title"));
			doc.addField(GxdHtFields.DESCRIPTION, rs.getString("description"));
			doc.addField(GxdHtFields.METHOD, rs.getString("method"));
			doc.addField(GxdHtFields.ORGANISM, rs.getString("organism"));
			doc.addField(GxdHtFields.BY_DEFAULT, rs.getString("sequence_num"));
			doc.addField(GxdHtFields.AGE, rs.getString("age"));
			doc.addField(GxdHtFields.AGE_MIN, rs.getString("age_min"));
			doc.addField(GxdHtFields.AGE_MAX, rs.getString("age_max"));
			doc.addField(GxdHtFields.SEX, rs.getString("sex"));
			doc.addField(GxdHtFields.STUDY_TYPE, rs.getString("study_type"));
			doc.addField(GxdHtFields.THEILER_STAGE, stage);
			doc.addField(GxdHtFields.RELEVANCY, rs.getString("relevancy"));
			
			if (this.experimentIDs.containsKey(exptKey)) {
				doc.addAllDistinct(GxdHtFields.EXPERIMENT_ID, this.experimentIDs.get(exptKey));
			}

			if (this.variables.containsKey(exptKey)) {
				doc.addAllDistinct(GxdHtFields.EXPERIMENTAL_VARIABLES, this.variables.get(exptKey));
			}

			if (this.refIDs.containsKey(exptKey)) {
				doc.addAllDistinct(GxdHtFields.REFERENCE_ID, this.refIDs.get(exptKey));
			}

			if (this.notes.containsKey(sampleKey)) {
				// add a conditional genotype note, if applicable
				if (this.conditionalGenotypes.contains(genotypeKey)) {
					addConditionalGenotypeNotes(sampleKey);
				}
				doc.addAllDistinct(GxdHtFields.NOTE, this.notes.get(sampleKey));
			} else if (this.conditionalGenotypes.contains(genotypeKey)) {
				// no other notes; add a conditional genotype one if needed.
				addConditionalGenotypeNotes(sampleKey);
				doc.addAllDistinct(GxdHtFields.NOTE, this.notes.get(sampleKey));
			}

			if (this.markerData.containsKey(sampleKey)) {
				doc.addAllDistinct(GxdHtFields.MUTATED_GENE, this.markerData.get(sampleKey));
			}
			
			if (genotypeKey != null) {
				if (this.strains.containsKey(genotypeKey)) {
					doc.addField(GxdHtFields.GENETIC_BACKGROUND, this.strains.get(genotypeKey));
				}
				if (this.alleles.containsKey(genotypeKey)) {
					doc.addField(GxdHtFields.MUTANT_ALLELES, this.alleles.get(genotypeKey));
				}
			}
			
			if (emapaKey != null) {
				doc.addField(GxdHtFields.STRUCTURE_TERM, this.getTerm(emapaKey));
				doc.addField(GxdHtFields.STRUCTURE_ID, this.getTermID(emapaKey));
				doc.addAllDistinct(GxdHtFields.STRUCTURE_SEARCH, this.getTermStringsWithAncestors(stage, emapaKey));
			}

                        if (celltypeKey != null) {
				doc.addField(GxdHtFields.CELLTYPE_ID, this.getTermID(celltypeKey));
				doc.addField(GxdHtFields.CELLTYPE_TERM, this.getTerm(celltypeKey));
                        }

			docs.add(doc);
			if (docs.size() > 10000) {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
				logger.info("Added stack of docs to Solr");
			}
		}
		rs.close();
		logger.info("Writing final batch of docs to Solr");
		writeDocs(docs);
		commit();
		logger.info("Done");
	}
}
