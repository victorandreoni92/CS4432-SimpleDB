package simpledb.buffer.replacementPolicy;

import java.util.Date;

import simpledb.buffer.Buffer;


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
		
		return index;
	}

}
