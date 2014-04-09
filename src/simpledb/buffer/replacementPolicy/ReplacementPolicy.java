package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;

/**
 * CS4432-Project1: Interface for replacement policies
 * 
 * @author Team 21
 *
 */
public interface ReplacementPolicy {
	
	// CS4432-Project1: Method to select the replacement policy
	public int chooseBufferForReplacement( Buffer[] bufferPool );
}
