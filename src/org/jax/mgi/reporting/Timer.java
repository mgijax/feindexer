package org.jax.mgi.reporting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This used to be used to setup a timer, but has since been overtaken by 
 * log4j, so this might just go away entirely
 * @author mhall
 *
 */

public class Timer {
    private static long lastTime = System.currentTimeMillis();
    private static long firstTime = lastTime;
    private static Logger logger = LoggerFactory.getLogger(Timer.class);
    
    public static void reset() {
    	lastTime = System.currentTimeMillis();
    	firstTime = lastTime;
    }

    public static long getElapsed () {
    	long now = System.currentTimeMillis();
    	long elapsed = now - lastTime;
    	lastTime = now;
    	return elapsed;
    }
    
    public static String getElapsedMessage() {
    	return " in " + getElapsed() + " ms";
    }
    
    public static long write (String message) {
    	long elapsed = getElapsed();
    	logger.info(elapsed + "ms : " + message);
    	return elapsed;
    }

    public static void writeTotal () {
    	long now = System.currentTimeMillis();
    	logger.info((now - firstTime) + "ms : total time");
    	return;
    }
    
    public static String getTotal () {
        long now = System.currentTimeMillis();
        return (now - firstTime) + "ms : total time";        
    }
}
