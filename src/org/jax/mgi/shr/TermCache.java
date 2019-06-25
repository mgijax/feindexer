package org.jax.mgi.shr;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Is: a cache of (string) terms for objects identified by (integer) object key
 * Has: a cache of 0-n terms cached for each object key
 * Does: knows how to initialize itself and look up terms by object key
 * Notes: is abstract to force the initialize method to be defined in a subclass
 */
public abstract class TermCache {
	/*--- static variables ---*/
	private static List<String> emptyList = new ArrayList<String>();

	/*--- instance variables ---*/

	public Logger logger = LoggerFactory.getLogger(this.getClass());
	protected Map<Integer,List<String>> cache = null;
	protected int cursorLimit = 50000;
	
	/*--- public methods ---*/
	
	/* Cannot use the default constructor as this is an abstract class.  However, subclasses
	 * should have one that populates this.cache appropriately and throws an Exception if needed,
	 * like this:
	 *	public MyTermCacheSubclass() throws Exception {
	 *		...
	 *	}
	 */
	
	// return true if we have any terms for the given objectKey, false if not
	public boolean hasTerms(Integer objectKey) {
		return this.cache.containsKey(objectKey);
	}
	
	// return a list of (String) terms associated with 'objectKey';
	// returns an empty list if none
	public List<String> getTerms(Integer objectKey) {
		if (!this.cache.containsKey(objectKey)) {
			return emptyList;
		}
		return this.cache.get(objectKey);
	}
	
	// convenience wrapper for dealing with object keys as Strings
	public List<String> getTerms(String objectKey) {
		try {
			int objKey = Integer.parseInt(objectKey);
			return this.getTerms(objKey);
		} catch (Exception e) {
			logger.error("Non-integer object key: " + objectKey);
			return emptyList;
		}
	}
	
	/*--- private methods ---*/
	
	// convenience method for use by various 'initialize()' implementations in subclasses, where we
	// can just define a single SQL command that returns rows with an object_key and a term field,
	// and we can walk the corresponding list of results in order to populate this.cache.
	protected void populate(String cmd) throws Exception {
		this.cache = new HashMap<Integer,List<String>>();
		
		logger.info("initializing " + this.getClass().getName());
		SQLExecutor ex = new SQLExecutor();
		ResultSet rs = ex.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			Integer objectKey = rs.getInt("object_key");
			
			if (!this.cache.containsKey(objectKey)) {
				this.cache.put(objectKey, new ArrayList<String>());
			}
			this.cache.get(objectKey).add(rs.getString("term"));
		}
		rs.close();
		logger.info(" - done (" + this.cache.size() + " object keys)");
	}
}
