package simpledb.buffer.replacementPolicy;

import java.util.Date;

import simpledb.buffer.Buffer;

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