package simpledb.buffer;

import java.util.Hashtable;
import java.util.LinkedList;
import simpledb.buffer.replacementPolicy.*;
import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 * @author Modified by Team21 for CS4432 at WPI
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   //CS4432-Project1: List to store available indexes of buffers
   private LinkedList<Integer> freeBufferIndexes;
   //CS4432-Project1: HashTable to access requested frames with blocks
   // Key is the block number being searched, value is index into pool where
   // corresponding buffer is located
   private Hashtable<Block, Integer> buffersOnPool;
   //CS4432-Project1: Variable indicating the replacement policy to use
   static Class replacementPolicy = ClockPolicy.class;
   
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
      freeBufferIndexes = new LinkedList<Integer>();
      buffersOnPool = new Hashtable<Block, Integer>(numbuffs);
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++){
         bufferpool[i] = new Buffer(i);
         //CS4432-Project1: Add indexes into free buffer list
         if (bufferpool[i].block() != null){ // make sure buffer is free, which must be the case
        	 throw new BufferAbortException(); // If not free during pool creation, throw exception
         }
         freeBufferIndexes.add(i);  
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
    * CS4432-Project1: Returns the information for each buffer in the pool
    * @return information about each buffer's id, block, and pin status
    */
   public String toString(){
	   String buffersInfo = new String();
	   for (Buffer buff : bufferpool){
		   buffersInfo += buff.toString() + System.getProperty("line.separator");
	   }
	   return buffersInfo;
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
         //CS4432-Project1: Add reference to block in table
         buffersOnPool.put(blk, buff.getBufferPoolIndex());
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      //CS4432-Project1: Set reference bit to be used by clock replacement policy
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
      //CS4432-Project1: Add reference to block in table
      buffersOnPool.put(buff.block(), buff.getBufferPoolIndex());
      numAvailable--;
      buff.pin();
      //CS4432-Project1: Set reference bit to be used by clock replacement policy
      buff.setRef();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()){
         numAvailable++;
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   /**
    * CS4432-Project1: Method that looks for buffer with specified disk page 
    * @param blk The block to look for in the pool
    * @return The buffer if it is on the pool, null otherwise
    */
   private Buffer findExistingBuffer(Block blk) {
      
	  //CS4432-Project1: Get index for buffer with blk, null if not present
	  Integer index = buffersOnPool.get(blk);
	  
	  if (index != null){
		  // If not null, return the buffer in the specified index
		  return bufferpool[index];
	  } else {
		  return null;
	  }
   }
   
   /**
    * CS4432-Project1: Method to retrieve an available buffer
    * The method first looks for an empty frame by looking at the freeBufferIndexes list
    * If there are no empty buffers, run the selected replacement policy to get a frame
    * If no frame could be selected, return null so that threads can block of BufferManager
    * @return
    */
   private Buffer chooseUnpinnedBuffer() {
	   
	  //CS4432-Project1: Check first if there are empty buffers
	  Integer index = freeBufferIndexes.pollFirst();
	  
	  if (index == null){ // If there were no empty buffers, apply buffer selection policy
		  
		  if (numAvailable == 0){ // First check if there are unpinned buffers.
			  index = null; // If there are not, set index to null and do not run any replacement
			  				// policy to make function more efficient
		  } else {
			  try {
				  index = ((ReplacementPolicy) replacementPolicy.newInstance()).chooseBufferForReplacement(bufferpool);
			  } catch (Exception e) {
				  e.printStackTrace();
				  return null;
			  }
		  }
	  }
	  
	  if (index != null){ //CS4432-Project1: If selection policy got a valid index, return buffer   
	      Buffer buff = bufferpool[index];
	      
    	  //CS4432-Project1: Since block is being reused, remove reference from buffersOnPool
	      //Check that block is not null for buffers without assigned blocks
	      if (buff.block() != null){
	    	  buffersOnPool.remove(buff.block());
	      }
	      
		  return buff;
	  } else {
		  return null; // If no frame selected, return null for caller to block
	  }
   }
   
   /**
    * CS4432-Project1: Function to set the replacement policy 
    * @param policy the policy class to select (original simple policy, LRU, or Clock)
    */
   public static void setReplacementPolicy(Class policy) {
		replacementPolicy = policy;
	}

}


	
	

