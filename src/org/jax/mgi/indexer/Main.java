package org.jax.mgi.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Refactored during MGI 5.x development. 
 */
public class Main 
{
	public static Logger logger = LoggerFactory.getLogger("FEINDEXER Main");
	public static List<Indexer> SPECIFIED_INDEXERS = new ArrayList<Indexer>();
	public static Map<String,Indexer> indexerMap = new HashMap<String,Indexer>();
	static
	{
		/* 
		 * All indexers must be added to this list in order to be run. 
		 * The key is the name you would use to specify your indexer as a command argument
		 * */
		indexerMap.put("journalsAC",new JournalsAutoCompleteIndexerSQL());
		indexerMap.put("reference", new RefIndexerSQL());
		indexerMap.put("authorsAC", new AuthorsAutoCompleteIndexerSQL());
		indexerMap.put("sequence", new SequenceIndexerSQL());
		indexerMap.put("cre", new CreIndexerSQL());
		indexerMap.put("marker", new MarkerIndexerSQL());
		indexerMap.put("markerPanesetImage", new MarkerPanesetIndexerSQL());
		indexerMap.put("image", new ImageIndexerSQL());
		indexerMap.put("allele", new AlleleIndexerSQL());
		indexerMap.put("markerAnnotation", new MarkerAnnotationIndexerSQL());
		indexerMap.put("creAlleleSystem", new CreAlleleSystemIndexerSQL());
		indexerMap.put("creAssayResult", new CreAssayResultIndexerSQL());
		indexerMap.put("gxdLitIndex", new GXDLitIndexerSQL());
		indexerMap.put("markerTissue", new MarkerTissueIndexerSQL());
		indexerMap.put("structureAC", new StructureAutoCompleteIndexerSQL());
		indexerMap.put("vocabTermAC", new VocabTermAutoCompleteIndexerSQL());
		indexerMap.put("gxdResult", new GXDResultIndexerSQL());
	
		//TODO: Are these obselete? If so can we get rid of these classes?
		//indexerMap.put("phenotypeImage",new PhenotypeImageIndexerSQL());
		//indexerMap.put("term",new TermIndexerSQL());
	}
	
	// other command args
	public static int maxThreads = 0; // uses default unless set to > 0
	
	public static void main(String[] args) 
	{
		parseCommandInput(args);
		
		if(SPECIFIED_INDEXERS == null || SPECIFIED_INDEXERS.size()==0)
		{
			// default is to run all indexers
			logger.info("No specific indexer indicated or found in command arguments. Default behavior is to run all indexers");
			for(Indexer idx : indexerMap.values())
			{
				SPECIFIED_INDEXERS.add(idx);
				// change maxThreads default if specified by user
				if(maxThreads>0) idx.setMaxThreads(maxThreads);
			}
		}
		for(Indexer idx : SPECIFIED_INDEXERS)
		{
			logger.info("Preparing to run: "+idx.getClass());
			idx.setupConnection();
			try{
				idx.index();
				logger.info("completed run of "+idx.getClass());
			}
			catch (IOException e)
			{
				logger.error(e.getMessage());
			}
		}
	}

	private static void parseCommandInput(String[] args)
    {
        Set<String> arguments = new HashSet<String>();
        
        for (int i=0;i<args.length;i++)
        {
            arguments.add(args[i]);
        }
        if(!arguments.isEmpty())
        {
            //start processing commands
        	for(String key : indexerMap.keySet())
        	{
        		if(arguments.contains(key)) 
        		{
        			SPECIFIED_INDEXERS.add(indexerMap.get(key));
        			logger.debug("adding user specified index: "+key+" to list of indexers to run.");
        		}
        	}
        	for(String arg : arguments)
        	{
        		if(arg.contains("maxThreads="))
        		{
        			String argValue = arg.replace("maxThreads=", "");
        			maxThreads = Integer.parseInt(argValue);
        		}
        	}
        }
    }
}
