package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;


/**
 * CS4432-Project1: Slightly modified original replacement policy. 
 * Left for reference and comparison
 * @author Team 21
 *
 */
public class SimplePolicy implements ReplacementPolicy {

	@Override
	public int chooseBufferForReplacement(Buffer[] bufferPool) {
		int index;
		
		// CS4432-Project1: Loop only until first unpinned buffer is found
		for( index = 0; index < bufferPool.length; index++ ) {
			if (!bufferPool[ index ].isPinned())
				break;
		}
		
		return index; // Return index to the first unpinned buffer available
	}

}
