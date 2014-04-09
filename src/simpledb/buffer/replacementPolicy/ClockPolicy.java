package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;

/**
 * CS4432-Project1: Clock replacement policy to be used by buffer manager
 * 
 * @author Team 21
 *
 */

public class ClockPolicy implements ReplacementPolicy {
	
	// Pointer of current frame in clock hand
	static private int clockPointer = 0;

	@Override
	public int chooseBufferForReplacement( Buffer[] bufferPool ) {		
		// Initial index of unpinned frame set to -1
		int unpinnedBuffer = -1;

		do {
			if( !bufferPool[ clockPointer ].isPinned() ) { // If frame not pinned
				if( bufferPool[ clockPointer ].refBitSet() ) { // If ref bit is set, unset it
					bufferPool[ clockPointer ].unsetRef();
				}
				else {
					unpinnedBuffer = clockPointer; // If ref bit was not set, select this frame
				}
			}
			
			// Move clock hand pointer
			ClockPolicy.clockPointer = ( clockPointer + 1 ) % bufferPool.length;
		} while( unpinnedBuffer == -1 );
		
		return unpinnedBuffer; // Return the index of the buffer to take
	}

}
