package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;

/**
 * CS4432-Project1: Slightly modified original replacement policy. 
 * Left for reference and comparison
 * 
 * Method relies on the fact that there was a check to ensure that there was at least one
 * unpinned buffer before calling this method.
 * 
 * @author Team 21
 *
*/
public class SimplePolicy implements ReplacementPolicy {

	/**
	 * CS4432-Project1: selects an index in the given buffer pool for replacement
	 *Method relies on the fact that there was a check to ensure that there was at least one
	 * unpinned buffer before calling this method.
	 */
	@Override
	public int chooseBufferForReplacement(Buffer[] bufferPool) {
		int index;
		
		// CS4432-Project1: Loop only until first unpinned buffer is found
		for( index = 0; index < bufferPool.length; index++ ) {
			if (!bufferPool[ index ].isPinned())
				break; // If found a buffer, return immediatedly 
		}

		// CS4432-Project1: Print debugging information for testing purposes
		if( BufferMgr.debuggingEnabled() ) {
			System.out.println( "Selecting buffer " + index + " for replacement using Simple Policy.  Bufferpool:" );
			for( int i = 0; i < bufferPool.length; i++ ) {
				System.out.print( i + ( bufferPool[i].isPinned() ? "*" : " " ) + " " );
			}
			System.out.println( " " );
		}
		
		return index; // Return index to the first unpinned buffer available
	}

}
