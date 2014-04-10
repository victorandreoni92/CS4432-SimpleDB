package simpledb.buffer;

import simpledb.buffer.replacementPolicy.*;
import simpledb.file.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
public class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	
	// CS4432-Project1: added to keep track of the current replacement policy
	private ReplacementPolicy replacementPolicy;
	
	
	// CS4432-Project1: If set to true, extra output is written to the console for testing and debugging purposes
	public static boolean TEST_BUFFER_MANAGER;
	
	/**
	* Creates a buffer manager having the specified number 
	* of buffer slots.
	* This constructor depends on both the {@link FileMgr} and
	* {@link simpledb.log.LogMgr LogMgr} objects 
	* that it gets from the class
	* {@link simpledb.server.SimpleDB}.
	* Those objects are created during system initialization.
	* Thus this constructor cannot be called until 
	* {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
	* is called first.
	* @param numbuffs the number of buffer slots to allocate
	*/
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		for (int i=0; i<numbuffs; i++)
			bufferpool[i] = new Buffer();
		
		// CS4432-Project1: Added to parse configuration file
		try {
			Properties props = new Properties();
			InputStream propertiesFile = new FileInputStream( "config.txt" );
			props.load( propertiesFile );
			propertiesFile.close();
			replacementPolicy = (ReplacementPolicy) Class.forName( props.getProperty( "ReplacementPolicy", "ClockPolicy" ) ).newInstance();
			TEST_BUFFER_MANAGER = props.getProperty( "TestBufferManager", "No" ).equals( "Yes" );
		} catch (Exception e) {
			replacementPolicy = new ClockPolicy();
			TEST_BUFFER_MANAGER = false;
			e.printStackTrace();
		}
	}
	
	/**
	* Flushes the dirty buffers modified by the specified transaction.
	* @param txnum the transaction's id number
	*/
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
			buff.flush();
	}
	
	/**
	* Pins a buffer to the specified block. 
	* If there is already a buffer assigned to that block
	* then that buffer is used;  
	* otherwise, an unpinned buffer from the pool is chosen.
	* Returns a null value if there are no available buffers.
	* @param blk a reference to a disk block
	* @return the pinned buffer
	*/
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		buff.setRef();
		return buff;
	}
	
	/**
	* Allocates a new block in the specified file, and
	* pins a buffer to it. 
	* Returns null (without allocating the block) if 
	* there are no available buffers.
	* @param filename the name of the file
	* @param fmtr a pageformatter object, used to format the new block
	* @return the pinned buffer
	*/
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		numAvailable--;
		buff.pin();
		buff.setRef();
		return buff;
	}
	
	/**
	* Unpins the specified buffer.
	* @param buff the buffer to be unpinned
	*/
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}
	
	/**
	* Returns the number of available (i.e. unpinned) buffers.
	* @return the number of available buffers
	*/
	int available() {
		return numAvailable;
	}
	
	private Buffer findExistingBuffer(Block blk) {
		for (Buffer buff : bufferpool) {
			Block b = buff.block();
			if (b != null && b.equals(blk))
				return buff;
		}
		return null;
	}
	
	/**
	* CS4432-Project1: Chooses a buffer for replacement using the policy contained in replacementPolicy
	* @return a buffer to be replaced
	*/
	private Buffer chooseUnpinnedBuffer() {
		
		if( numAvailable == 0 ) {
			return null;
		}
		
		int bufferIndex = replacementPolicy.chooseBufferForReplacement( bufferpool );
	
		return bufferpool[ bufferIndex ];
	}
}
