package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;

public class ClockPolicy implements ReplacementPolicy {
	
	static private int clockPointer = 0;

	@Override
	public int chooseBufferForReplacement( Buffer[] bufferPool ) {		
		int unpinnedBuffer = -1;
		//Buffer currentBuffer;
	
		do {
			//currentBuffer = bufferPool[ clockPointer ];
			if( !bufferPool[ clockPointer ].isPinned() ) {
				if( bufferPool[ clockPointer ].refBitSet() ) {
					bufferPool[ clockPointer ].unsetRef();
				}
				else {
					unpinnedBuffer = clockPointer;
				}
			}
			
			this.clockPointer = ( clockPointer + 1 ) % bufferPool.length;
		} while( unpinnedBuffer == -1 );
		
		return unpinnedBuffer;
	}

}
