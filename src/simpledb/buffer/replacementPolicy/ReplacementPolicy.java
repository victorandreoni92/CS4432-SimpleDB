package simpledb.buffer.replacementPolicy;

import simpledb.buffer.Buffer;

public interface ReplacementPolicy {
	public int chooseBufferForReplacement( Buffer[] bufferPool );
}
