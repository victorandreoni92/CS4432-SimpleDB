package simpledb.buffer.replacementPolicy;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;

/**
 * CS4432-Project1: LRU replacement policy to be used by buffer manager
 * 
 * Method relies on the fact that there was a check to ensure that there was at least one
 * unpinned buffer before calling this method.
 * 
 * @author Team 21
 *
 */
public class LeastRecentlyUsedPolicy implements ReplacementPolicy {

	/**
	 * CS4432-Project1: selects an index in the given buffer pool for replacement
	 */
	@Override
	public int chooseBufferForReplacement(Buffer[] bufferPool) {
		int index;
		int leastRecentlyUsed = 0;
		Date leastRecentlyUsedDate = bufferPool[0].getLastModifiedDate(); // Keep track of the least recently used time
		
		for( index = 0; index < bufferPool.length; index++ ) { // Loop through the pool and find the least recently used buffer
			if( 	!bufferPool[ index ].isPinned() &&
					bufferPool[ index ].getLastModifiedDate().before( leastRecentlyUsedDate ) ) {
				leastRecentlyUsed = index;
			}
		}
		
		// CS4432-Project1: Print debugging information for testing purposes
		if( BufferMgr.debuggingEnabled() ) {
			DateFormat format = new SimpleDateFormat("ss.SSS");
			System.out.println( "Selecting buffer " + leastRecentlyUsed + " for replacement using LRU replacement policy.  Bufferpool:" );
			for( int i = 0; i < bufferPool.length; i++ ) {
				System.out.print( i + ":" + format.format( bufferPool[i].getLastModifiedDate() ) + ( bufferPool[i].isPinned() ? "*" : " " ) + " " );
			}
			System.out.println( " " );
		}
		
		return leastRecentlyUsed; // Return the index of the least recently used buffer
	}

}
