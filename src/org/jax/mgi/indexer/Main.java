package org.jax.mgi.indexer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Refactored during MGI 5.x development.
 *
 *
 * Possible Arguments:
 * 	all   - runs all indexers
 * 	cre,reference,journalsAC, ...etc   - specific index name will run that indexer
 *  maxThreads=10 or maxThreads=20... etc   - set max thread count for solr document writing
 *
 *  -kstone
 */
public class Main {
	public static Logger logger = LoggerFactory.getLogger("FEINDEXER Main");
	public static List<String> SPECIFIED_INDEXERS = new ArrayList<String>();
	public static Map<String,Indexer> indexerMap = new HashMap<String,Indexer>();
	public static boolean RUN_ALL_INDEXERS=false;
	public static boolean ONLY_LIST=false;

	static {
		/*
		 * All indexers must be added to this list in order to be run.
		 * The key is the name you would use to specify your indexer as a command argument
		 * */
		indexerMap.put("anatomyAC",new AnatomyAutoCompleteIndexerSQL());
		indexerMap.put("emapaAC",new EmapaAutoCompleteIndexerSQL());
		indexerMap.put("driverAC",new DriverAutoCompleteIndexerSQL());
		indexerMap.put("gxdEmapaAC",new GXDEmapaAutoCompleteIndexerSQL());
		indexerMap.put("journalsAC",new JournalsAutoCompleteIndexerSQL());
		indexerMap.put("reference", new RefIndexerSQL());
		indexerMap.put("authorsAC", new AuthorsAutoCompleteIndexerSQL());
		indexerMap.put("marker", new MarkerIndexerSQL());
		indexerMap.put("markerPanesetImage", new MarkerPanesetIndexerSQL());
		indexerMap.put("image", new ImageIndexerSQL());
		indexerMap.put("allele", new AlleleIndexerSQL());
		indexerMap.put("markerAnnotation", new MarkerAnnotationIndexerSQL());
		indexerMap.put("creAssayResult", new CreAssayResultIndexerSQL());
		indexerMap.put("gxdLitIndex", new GXDLitIndexerSQL());
		indexerMap.put("dagEdge", new DagEdgeIndexerSQL());
		indexerMap.put("vocabTermAC", new VocabTermAutoCompleteIndexerSQL());
		indexerMap.put("gxdResult", new GXDResultIndexerSQL());
		indexerMap.put("gxdImagePane", new GXDImagePaneIndexerSQL());
		indexerMap.put("gxdDifferentialMarker", new GXDDifferentialIndexerSQL());
		indexerMap.put("mpAnnotation", new MPAnnotationIndexerSQL());
		indexerMap.put("interaction", new InteractionIndexerSQL());
		indexerMap.put("hdpDisease", new HdpDiseaseIndexerSQL());
		indexerMap.put("hdpGrid", new HdpGridIndexerSQL());
		indexerMap.put("hdpGene", new HdpGeneIndexerSQL());
		indexerMap.put("hdpGridAnnotation", new HdpGridAnnotationIndexerSQL());
	}

	// other command args
	public static int maxThreads = 2; // uses default unless set to > 0

	private static List<String> getIndexers() {
		List<String> indexes = new ArrayList<String>();

		for(String indexName : indexerMap.keySet()) {
			indexes.add(indexName);
		}
		Collections.sort(indexes);
		return indexes;
	}

	private static void parseCommandInput(String[] args) {
		Set<String> arguments = new HashSet<String>();
		for (int i=0;i<args.length;i++){ arguments.add(args[i]);}
		
		if(!arguments.isEmpty()) {
			RUN_ALL_INDEXERS = SPECIFIED_INDEXERS.size()==0 && arguments.contains("all");
			//start processing commands
			for(String arg : arguments) {
				if(arg.contains("maxThreads=")) {
					String argValue = arg.replace("maxThreads=", "");
					maxThreads = Integer.parseInt(argValue);
				} else if(indexerMap.containsKey(arg)) {
					SPECIFIED_INDEXERS.add(arg);
					logger.info("adding user specified index: "+arg+" to list of indexers to run.");
				} else if("hmdc".equalsIgnoreCase(arg) || "hdp".equalsIgnoreCase(arg)) {
					SPECIFIED_INDEXERS.add("hdpGene");
					SPECIFIED_INDEXERS.add("hdpDisease");
					SPECIFIED_INDEXERS.add("hdpGrid");
					SPECIFIED_INDEXERS.add("hdpGridAnnotation");
				} else if ("list".equalsIgnoreCase(arg)) {
					// only show the list of possible indexers,
					// one per line, then exit
					ONLY_LIST = true;
					for (String s : getIndexers()) {
						System.out.println(s);
					}
				} else {
					logger.info("unknown indexer \""+arg+"\"");
				}
			}
		}
	}

	public static void main(String[] args) {
		parseCommandInput(args);
		/*
		 * Generate list of indexes to run from program specified arguments
		 */
		if(RUN_ALL_INDEXERS) {
			SPECIFIED_INDEXERS = new ArrayList<String>();
			// default is to run all indexers
			logger.info("\"all\" option was selected. Beginning run of all indexers");
			for(String idxKey : indexerMap.keySet()) {
				Indexer idx = indexerMap.get(idxKey);
				SPECIFIED_INDEXERS.add(idxKey);
				// change maxThreads default if specified by user
				if(maxThreads>0) idx.setMaxThreads(maxThreads);
			}
		}
		if (ONLY_LIST) {
			System.exit(0);
		}
		if(SPECIFIED_INDEXERS==null || SPECIFIED_INDEXERS.size()==0) {
			exitWithMessage("There are no specified indexers to run. Exiting.");
		}

		// track failed indexers for later reporting
		List<String> failedIndexers = new ArrayList<String>();

		for(String idxKey: SPECIFIED_INDEXERS) {
			Indexer idx = indexerMap.get(idxKey);
			logger.info("Preparing to run: "+idx.getClass());
			idx.start();
		}
		
		for(String idxKey: SPECIFIED_INDEXERS) {
			Indexer idx = indexerMap.get(idxKey);
			logger.info("Waiting for: "+ idx.getClass() + " to finish");
			try {
				idx.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(idx.hasFailedThreads()) {
				failedIndexers.add(idxKey);
			}
		}

		// return error if any indexers failed
		if(failedIndexers.size()>0) {
			String errorMsg = "Failed or Incomplete Indexes: " + StringUtils.join(failedIndexers,",") + "\n Please view the above logs for more details.";
			exitWithMessage(errorMsg);
		} else {
			logger.info("Completed run of the following indexes:"+StringUtils.join(SPECIFIED_INDEXERS,","));
		}
	}
	
	private static void exitWithMessage(String errorMsg) {
		exitWithMessage(errorMsg,null);
	}
	
	private static void exitWithMessage(String errorMsg,Exception ex) {
		if(ex== null) logger.error(errorMsg);
		else logger.error(errorMsg,ex);
		// logger needs some time to work before we exit (I know it looks stupid to call sleep() here, but it works)
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// I really could care less if this exception is ever thrown
			e.printStackTrace();
		}
		System.exit(-1);
	}
}
