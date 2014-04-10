package simpledb.buffer.replacementPolicy;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import simpledb.buffer.BasicBufferMgr;
import simpledb.buffer.Buffer;

/**
 * CS4432-Project1: An implementation of the default replacement policy used by SimpleDB
 */
public class SimplePolicy implements ReplacementPolicy {

	/**
	 * CS4432-Project1: selects an index in the given buffer pool for replacement
	 */
	@Override
	public int chooseBufferForReplacement(Buffer[] bufferPool) {
		int index;
		
		for( index = 0; index < bufferPool.length; index++ ) {
			if (!bufferPool[ index ].isPinned())
				break;
		}
		
		if( BasicBufferMgr.TEST_BUFFER_MANAGER ) {
			System.out.println( "Selecting buffer " + index + " for replacement.  Bufferpool:" );
			for( int i = 0; i < bufferPool.length; i++ ) {
				System.out.print( i + ( bufferPool[i].isPinned() ? "*" : " " ) + " " );
			}
			System.out.println( " " );
		}
		
		return index;
	}

}
