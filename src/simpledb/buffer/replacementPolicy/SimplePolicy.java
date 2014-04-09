package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;

public class SimplePolicy implements ReplacementPolicy {

	@Override
	public int chooseBufferForReplacement(Buffer[] bufferPool) {
		int index;
		
		for( index = 0; index < bufferPool.length; index++ ) {
			if (!bufferPool[ index ].isPinned())
				break;
		}
		
		return index;
	}

}
