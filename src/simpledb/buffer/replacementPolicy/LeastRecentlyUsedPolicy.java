package simpledb.buffer.replacementPolicy;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import simpledb.buffer.BasicBufferMgr;
import simpledb.buffer.Buffer;

/**
 * CS4432-Project1: An implementation of the Least Recently Used replacement policy
 */
public class LeastRecentlyUsedPolicy implements ReplacementPolicy {

	/**
	 * CS4432-Project1: selects an index in the given buffer pool for replacement
	 */
	@Override
	public int chooseBufferForReplacement(Buffer[] bufferPool) {
		int index;
		int leastRecentlyUsed = 0;
		Date leastRecentlyUsedDate = bufferPool[0].getLastModifiedDate();
		
		for( index = 0; index < bufferPool.length; index++ ) {
			if( 	!bufferPool[ index ].isPinned() &&
					bufferPool[ index ].getLastModifiedDate().before( leastRecentlyUsedDate ) ) {
				leastRecentlyUsed = index;
			}
		}
		
		if( BasicBufferMgr.TEST_BUFFER_MANAGER ) {
			DateFormat format = new SimpleDateFormat("ss.SSS");
			System.out.println( "Selecting buffer " + leastRecentlyUsed + " for replacement.  Bufferpool:" );
			for( int i = 0; i < bufferPool.length; i++ ) {
				System.out.print( i + ":" + format.format( bufferPool[i].getLastModifiedDate() ) + ( bufferPool[i].isPinned() ? "*" : " " ) + " " );
			}
			System.out.println( " " );
		}
		
		return leastRecentlyUsed;
	}

}
