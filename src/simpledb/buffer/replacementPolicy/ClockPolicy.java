package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;

/**
 * CS4432-Project1: Clock replacement policy to be used by buffer manager
 * 
 * Method relies on the fact that there was a check to ensure that there was at least one
 * unpinned buffer before calling this method.
 * 
 * @author Team 21
 *
 */

public class ClockPolicy implements ReplacementPolicy {
	
	// CS4432-Project1: keeps track of the position of the clock hand
	private int clockPointer = 0;

	/**
	 * CS4432-Project1: selects an index in the given buffer pool for replacement
	 */
	@Override
	public int chooseBufferForReplacement( Buffer[] bufferPool ) {		
		// Initial index of unpinned frame set to -1
		int unpinnedBuffer = -1;

		Buffer currentBuffer; // Maintain a reference to the current buffer
		do {
			currentBuffer = bufferPool[ clockPointer ]; // Get each buffer while the hand moves
			if( !currentBuffer.isPinned() ) { // If buffer not pinned but ref bit set, set the ref and move on to next buffer
				if( currentBuffer.refBitSet() ) {
					currentBuffer.unsetRef();
				}
				else {
					unpinnedBuffer = clockPointer; // If ref bit was not set, select this frame
				}
			}
			clockPointer = ( clockPointer + 1 ) % bufferPool.length;
		} while( unpinnedBuffer == -1 );
		
		// CS4432-Project1: Print debugging information for testing purposes
		if( BufferMgr.debuggingEnabled() ) {
			System.out.println( "Selecting buffer " + unpinnedBuffer + " for replacement using Clock replacement policy.  Bufferpool:" );
			for( int i = 0; i < bufferPool.length; i++ ) {
				System.out.print( i + ( bufferPool[i].isPinned() ? bufferPool[i].refBitSet() ? "*+" : "* " : bufferPool[i].refBitSet() ? "+ " : "  " ) + " " );
			}
			System.out.println( " " );
			for( int i = 0; i < clockPointer; i++ ) {
				System.out.print( "    " );
			}
			System.out.println( "^" );
		}
		
		return unpinnedBuffer; // Return the index for the buffer that will be replaced
	}
}
