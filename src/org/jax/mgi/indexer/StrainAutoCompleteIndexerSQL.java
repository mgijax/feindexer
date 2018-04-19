package org.jax.mgi.indexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.mgi.shr.jsonmodel.AccessionID;
import org.jax.mgi.shr.jsonmodel.SimpleStrain;

/* Is: an indexer that builds the index supporting the strain autocomplete (for the strain minihome/QF)
 * 		Each document in the index represents data for either:
 * 		1. a single mouse strain by name
 * 		2. a mouse strain/synonym pair, so we can match the synonym but get the actual strain name
 */
public class StrainAutoCompleteIndexerSQL extends Indexer {

	/*--------------------------*/
	/*--- instance variables ---*/
	/*--------------------------*/

	private int cursorLimit = 10000;				// number of records to retrieve at once
	protected int solrBatchSize = 5000;				// number of docs to send to solr in each batch

  	private SmartAlphaComparator smartAlphaComparator = new SmartAlphaComparator();

	/*--------------------*/
	/*--- constructors ---*/
	/*--------------------*/

	public StrainAutoCompleteIndexerSQL() {
		super("strainAC");
	}

	/*-----------------------*/
	/*--- private classes ---*/
	/*-----------------------*/

	private class StrainPair {
		public String name;
		public String synonym;
		
		public StrainPair(String name) {
			this.name = name;
		}

		public StrainPair(String name, String synonym) {
			this.name = name;
			this.synonym = synonym;
		}

		public StrainPairComparator getComparator() {
			return new StrainPairComparator();
		}
		
		public class StrainPairComparator implements Comparator<StrainPair> {
			@Override
			public int compare(StrainPair a, StrainPair b) {
				// Sort rules:
				// 1. primary: smart-alpha by strain name (assumes strain names cannot be null)
				// 2. secondary: null synonym before non-null synonym
				// 3. tertiary: smart-alpha by synonym
				int out = smartAlphaComparator.compare(a.name, b.name);
				if (out != 0) { return out; }
				
				// smartAlphaComparator sorts nulls after non-nulls, so needs to be custom here
				if (a.synonym != null) {
					if (b.synonym != null) {
						return smartAlphaComparator.compare(a.synonym, b.synonym);
					} else {
						return 1;		// null b first
					}
				} else if (b.synonym != null) {
					return -1;			// null a first
				}
				return 0;
			}
		}
	}

	/*-----------------------*/
	/*--- private methods ---*/
	/*-----------------------*/

	/* retrieve strain data from the database, build Solr docs, and write them to the server.
	 */
	private void processStrains() throws Exception {
		logger.info("loading strains");
		
		String cmd = "select s.name, ss.synonym "
			+ "from strain s "
			+ "left outer join strain_synonym ss on (s.strain_key = ss.strain_key) "
			+ "order by 1, 2";
		
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		logger.info("  - finished query in " + ex.getTimestamp());
		
		// collect set of objects for final ordering (Postgres order-by is not smart-alpha)
		
		String previousName = "";
		List<StrainPair> pairs = new ArrayList<StrainPair>();
		while (rs.next()) {
			String name = rs.getString("name");
			String synonym = rs.getString("synonym");
			
			// If we have a row with no synonym, we can just add the strain name itself.
			if (synonym == null) {
				pairs.add(new StrainPair(name));
			} else {
				// If we have a row with a synonym, then we need to also add the strain name
				// by itself (but only once, in the case of multiple synonyms).
				if (!previousName.equals(name)) {
					pairs.add(new StrainPair(name));
					previousName = name;
				}
				pairs.add(new StrainPair(name, synonym));
			}
		}
		rs.close();
		
		// sort them by rules documented in Comparator class
		if (pairs.size() > 0) {
			Collections.sort(pairs, pairs.get(0).getComparator());
		}

		// now build solr docs and push them to the server
		
		int seqNum = 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for (StrainPair sp : pairs) {
			seqNum++;
			
			// build the solr document
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(IndexConstants.UNIQUE_KEY, seqNum);
			doc.addField(IndexConstants.STRAIN_NAME, sp.name);
			if (sp.synonym != null) {
				doc.addField(IndexConstants.VB_SYNONYM, sp.synonym);
			}
			doc.addField(IndexConstants.BY_DEFAULT, seqNum);
			
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

		logger.info("done processing " + seqNum + " documents");
	}
	
	/*----------------------*/
	/*--- public methods ---*/
	/*----------------------*/

	@Override
	public void index() throws Exception {
		processStrains();
	}
}
