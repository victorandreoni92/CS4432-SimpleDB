package simpledb.buffer.replacementPolicy;

import simpledb.buffer.BasicBufferMgr;
import simpledb.buffer.Buffer;

/**
 * CS4432-Project1: An implementation of the clock replacement policy
 */
public class ClockPolicy implements ReplacementPolicy {
	
	// CS4432-Project1: keeps track of the position of the clock hand
	private int clockPointer = 0;

	/**
	 * CS4432-Project1: selects an index in the given buffer pool for replacement
	 */
	@Override
	public int chooseBufferForReplacement( Buffer[] bufferPool ) {		
		int unpinnedBuffer = -1;
		Buffer currentBuffer;
		do {
			currentBuffer = bufferPool[ clockPointer ];
			if( !currentBuffer.isPinned() ) {
				if( currentBuffer.refBitSet() ) {
					currentBuffer.unsetRef();
				}
				else {
					unpinnedBuffer = clockPointer;
				}
			}
			
			clockPointer = ( clockPointer + 1 ) % bufferPool.length;
		} while( unpinnedBuffer == -1 );
		
		if( BasicBufferMgr.TEST_BUFFER_MANAGER ) {
			System.out.println( "Selecting buffer " + unpinnedBuffer + " for replacement.  Bufferpool:" );
			for( int i = 0; i < bufferPool.length; i++ ) {
				System.out.print( i + ( bufferPool[i].isPinned() ? bufferPool[i].refBitSet() ? "*+" : "* " : bufferPool[i].refBitSet() ? "+ " : "  " ) + " " );
			}
			System.out.println( " " );
			for( int i = 0; i < clockPointer; i++ ) {
				System.out.print( "    " );
			}
			System.out.println( "^" );
		}
		
		return unpinnedBuffer;
	}
}
