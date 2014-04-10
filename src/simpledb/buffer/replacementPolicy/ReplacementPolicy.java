package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;

/**
 * A Replacement Policy can be used to select Buffers for replacement
 */
public interface ReplacementPolicy {
	/**
	 * CS4432-Project1: selects an index in the given buffer pool for replacement
	 */
	public int chooseBufferForReplacement( Buffer[] bufferPool );
}
