package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;

/**
 * CS4432-Project1: A Replacement Policy can be used to select Buffers for replacement
 * 
 * @author Team 21
 */
public interface ReplacementPolicy {
	
	/**
	 * CS4432-Project1: selects an index in the given buffer pool for replacement
	 * Interface is implemented by the different replacement policies for this assignment
	 * LRU, Clock, and simple policy
	 */
	public int chooseBufferForReplacement( Buffer[] bufferPool );
}
