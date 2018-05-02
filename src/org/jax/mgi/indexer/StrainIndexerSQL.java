package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.jsonmodel.AccessionID;
import org.jax.mgi.shr.jsonmodel.SimpleStrain;

/* Is: an indexer that builds the index supporting the strain summary page (reachable from the strains/SNPs
 * 		minihome page).  Each document in the index represents data for a single mouse strain, and each strain
 * 		is represented in only one document.
 * Notes: The index is intended to support searches by a strain name, strain ID, and strain type.  Details
 * 		of the strains are encapsulated in a JSON object in the strain field.
 */
public class StrainIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

	private ObjectMapper mapper = new ObjectMapper();				// converts objects to JSON
	
	private Map<String,List<AccessionID>> accessionIDs = null;		// maps from strain key to list of IDs
	private Map<String,List<String>> synonyms = null;				// maps from strain key to synonyms
	private Map<String,List<String>> attributes = null;				// maps from strain key to attributes

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public StrainIndexerSQL() {
		super("strain");
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* gather the accession IDs that can be used to retrieve each strain, caching them in 'accessionIDs'
	 */
	private void cacheIDs() throws Exception {
		logger.info("caching accession IDs");
		String cmd = "select strain_key, logical_db, acc_id "
			+ "from strain_id "
			+ "order by sequence_num";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.info("  - finished ID query in " + ex.getTimestamp());
		
		int i = 0;
		accessionIDs = new HashMap<String,List<AccessionID>>();
		while (rs.next()) {
			String strainKey = rs.getString("strain_key");
			if (!accessionIDs.containsKey(strainKey)) {
				accessionIDs.put(strainKey, new ArrayList<AccessionID>());
			}
			accessionIDs.get(strainKey).add(
				new AccessionID(rs.getString("acc_id"), rs.getString("logical_db")) );
			i++;
		}
		rs.close();
		logger.info("  - cached " + i + " IDs for " + accessionIDs.size() + " strains");
	}
	
	/* get the set of accession IDs for the given strain key
	 */
	public List<AccessionID> getIDs(String strainKey) throws Exception {
		if (accessionIDs.containsKey(strainKey)) {
			return accessionIDs.get(strainKey);
		}
		return null;
	}
	
	/* get a List of just the ID strings for the given strain key
	 */
	public List<String> getIDStrings(String strainKey) throws Exception {
		List<AccessionID> objects = getIDs(strainKey);
		if (objects != null) {
			List<String> ids = new ArrayList<String>();
			for (AccessionID obj : objects) {
				ids.add(obj.getAccID());
			}
			return ids;
		}
		return null;
	}


	/* cache the synonyms for each strain in 'synonyms'
	 */
	private void cacheSynonyms() throws Exception {
		logger.info("caching synonyms");
		String cmd = "select strain_key, synonym "
			+ "from strain_synonym "
			+ "order by sequence_num";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.info("  - finished synonym query in " + ex.getTimestamp());
		
		int i = 0;
		synonyms = new HashMap<String,List<String>>();
		while (rs.next()) {
			String strainKey = rs.getString("strain_key");
			if (!synonyms.containsKey(strainKey)) {
				synonyms.put(strainKey, new ArrayList<String>());
			}
			synonyms.get(strainKey).add(rs.getString("synonym"));
			i++;
		}
		rs.close();
		logger.info("  - cached " + i + " synonyms for " + synonyms.size() + " strains");
		
	}
	
	/* get the list of synonyms for the given strain key
	 */
	public List<String> getSynonyms(String strainKey) throws Exception {
		if (synonyms.containsKey(strainKey)) {
			return synonyms.get(strainKey);
		}
		return null;
	}
	
	/* gather the attributes for each strain and cache them in 'attributes'
	 */
	private void cacheAttributes() throws Exception {
		logger.info("caching attributes");
		String cmd = "select strain_key, attribute "
			+ "from strain_attribute "
			+ "order by sequence_num";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.info("  - finished attribute query in " + ex.getTimestamp());
		
		int i = 0;
		attributes = new HashMap<String,List<String>>();
		while (rs.next()) {
			String strainKey = rs.getString("strain_key");
			if (!attributes.containsKey(strainKey)) {
				attributes.put(strainKey, new ArrayList<String>());
			}
			attributes.get(strainKey).add(rs.getString("attribute"));
			i++;
		}
		rs.close();
		logger.info("  - cached " + i + " attributes for " + attributes.size() + " strains");
	}
	
	/* get the set of attributes for the given strain key
	 */
	public List<String> getAttributes(String strainKey) throws Exception {
		if (attributes.containsKey(strainKey)) {
			return attributes.get(strainKey);
		}
		return null;
	}
	
	/* retrieve strains from the database, build Solr docs, and write them to the server.
	 */
	private void processStrains() throws Exception {
		logger.info("loading strains");
		
		String cmd = "select s.strain_key, s.primary_id, s.name, s.strain_type, n.by_strain, "
			+ "  count(distinct r.reference_key) as reference_count "
			+ "from strain s "
			+ "inner join strain_sequence_num n on (s.strain_key = n.strain_key) "
			+ "left outer join strain_to_reference r on (s.strain_key = r.strain_key) "
			+ "group by 1, 2, 3, 4, 5";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.info("  - finished main strain query in " + ex.getTimestamp());

		int i = 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while (rs.next())  {  
			i++;
			String strainKey = rs.getString("strain_key");
			String name = rs.getString("name");
			String primaryID = rs.getString("primary_id");
			List<String> synonyms = getSynonyms(strainKey);

			// build the object that we will store as JSON in the strain field of the index
			SimpleStrain strain = new SimpleStrain(name, primaryID);
			strain.setAttributes(getAttributes(strainKey));
			strain.setAccessionIDs(getIDs(strainKey));
			strain.setSynonyms(synonyms);
			strain.setReferenceCount(rs.getInt("reference_count"));
			
			// start building the solr document (will have only four fields total)
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.STRAIN_KEY, strainKey);
			doc.addField(IndexConstants.STRAIN_NAME, name);
			if (synonyms != null) {
				doc.addField(IndexConstants.STRAIN_NAME, synonyms);
			}
			doc.addField(IndexConstants.STRAIN_TYPE, rs.getString("strain_type"));
			doc.addField(IndexConstants.ACC_ID, getIDStrings(strainKey));
			doc.addField(IndexConstants.BY_DEFAULT, rs.getInt("by_strain"));
			doc.addField(IndexConstants.STRAIN, mapper.writeValueAsString(strain));
			doc.addField(IndexConstants.STRAIN_ATTRIBUTE, strain.getAttributes());
			
			// Add this doc to the batch we're collecting.  If the stack hits our
			// threshold, send it to the server and reset it.
			docs.add(doc);
			if (docs.size() >= solrBatchSize)  {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}

		// any leftover docs to send to the server?  (likely yes)
		writeDocs(docs);
		commit();
		rs.close();

		logger.info("done processing " + i + " strains");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		// collect various mappings needed for data lookup
		cacheAttributes();
		cacheIDs();
		cacheSynonyms();
		processStrains();
	}
}
