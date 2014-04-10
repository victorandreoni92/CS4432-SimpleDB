package simpledb.buffer;

import java.util.Date;

import simpledb.server.SimpleDB;
import simpledb.file.*;

/**
 * An individual buffer.
 * A buffer wraps a page and stores information about its status,
 * such as the disk block associated with the page,
 * the number of times the block has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding log record.
 * @author Edward Sciore
 * 
 * @author Modified by Team21 for CS4432 at WPI
 */
public class Buffer {
   private Page contents = new Page();
   private Block blk = null;
   private int pins = 0;
   //CS4432-Project1: Store index where buffer is allocated
   private int bufferPoolIndex;
   private int ref = 0; // CS4432-Project1: Added to represent the ref bit associated with this buffer
   private int modifiedBy = -1;  // negative means not modified
   private Date lastModified; // CS4432-Project1: Added to keep track of the date of last modification
   private int logSequenceNumber = -1; // negative means no corresponding log record

   /**
    * Creates a new buffer, wrapping a new 
    * {@link simpledb.file.Page page}.  
    * This constructor is called exclusively by the 
    * class {@link BasicBufferMgr}.   
    * It depends on  the 
    * {@link simpledb.log.LogMgr LogMgr} object 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * 
    * CS4432-Project1: Constructing a new buffer initialized the date of last modification
    */
   public Buffer() {
	   //CS4432-Project1: If no index is specified, add signal value for index
	   // This is most likely not necessary since the buffer constructor
	   // is only called by BasicBufferManager, but adding it here to prevent
	   // undocumented null errors (see getBufferPoolIndex)
	   this(-99); 
   }
   
   //CS4432-Project1: Modified constructor to add index record to buffer
   public Buffer(int index){
	   bufferPoolIndex = index;
	   lastModified = new Date();
   }
   
   /**
    * CS4432-Project1: Returns the index associated with the buffer
    * @return index of the buffer or null if no index associated
    */
   public Integer getBufferPoolIndex(){
	   if (bufferPoolIndex != -99){
		   return bufferPoolIndex;
	   } else {
		   return null;
	   }
   }
   
   /**
    * CS4432-Project1: Returns the information of the buffer in a readable format
    * @return information about the buffer's id, block, and pin status
    */
   public String toString(){
	   String bufferInfo =  "Buffer ID: " + bufferPoolIndex + " Pin count: " + pins
			   + " Allocated block: ";
	   if (blk != null){
		   bufferInfo += blk.toString();
	   } else {
		   bufferInfo += "No block assigned";
	   }
	   
	   return bufferInfo;
   }
   
   /**
    * Returns the integer value at the specified offset of the
    * buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
   public int getInt(int offset) {
      return contents.getInt(offset);
   }

   /**
    * Returns the string value at the specified offset of the
    * buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
   public String getString(int offset) {
      return contents.getString(offset);
   }

   /**
    * Writes an integer to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    * 
    * CS4432-Project1: This method updates the date of last modification
    */
   public void setInt(int offset, int val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setInt(offset, val);
      //CS4432-Project1: Update last modified date to current time. Used by LRU policy
      lastModified = new Date();
   }

   /**
    * Writes a string to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    * 
    * CS4432-Project1: This method updates the date of last modification
    */
   public void setString(int offset, String val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setString(offset, val);
      //CS4432-Project1: Update last modified date to current time. Used by LRU policy
      lastModified = new Date();
   }

   /**
    * Returns a reference to the disk block
    * that the buffer is pinned to.
    * @return a reference to a disk block
    */
   public Block block() {
      return blk;
   }

   /**
    * Writes the page to its disk block if the
    * page is dirty.
    * The method ensures that the corresponding log
    * record has been written to disk prior to writing
    * the page to disk.
    */
   void flush() {
      if (modifiedBy >= 0) {
         SimpleDB.logMgr().flush(logSequenceNumber);
         contents.write(blk);
         modifiedBy = -1;
      }
   }

   /**
    * Increases the buffer's pin count.
    */
   public void pin() {
      pins++;
   }

   /**
    * Decreases the buffer's pin count.
    */
   public void unpin() {
      pins--;
   }

   /**
    * Returns true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   public boolean isPinned() {
      return pins > 0;
   }
   
   /**
    * CS4432-Project1: Sets the reference bit of this buffer to 1
    */
   public void setRef() {
	   ref = 1;
   }
   
    /** CS4432-Project1: Sets the reference bit of this buffer to 0
     * 
    */
   public void unsetRef() {
	   ref = 0;
   }
   
   /**
    * CS4432-Project1:
    * @return true if the reference bit of this buffer is set to 1
    */
   public boolean refBitSet() {
	   return ref == 1;
   }
   
   /**
    * CS4432-Project1: Returns the last modified date of a buffer
    * @return the date of last modification
    */
   public Date getLastModifiedDate() {
	   return lastModified;
   }

   /**
    * Returns true if the buffer is dirty
    * due to a modification by the specified transaction.
    * @param txnum the id of the transaction
    * @return true if the transaction modified the buffer
    */
   boolean isModifiedBy(int txnum) {
      return txnum == modifiedBy;
   }

   /**
    * Reads the contents of the specified block into
    * the buffer's page.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param b a reference to the data block
    * 
    * CS4432-Project1: This method updates the date of last modification
    */
   void assignToBlock(Block b) {
      flush();
      blk = b;
      contents.read(blk);
      pins = 0;
      //CS4432-Project1: Update last modified date to current time. Used by LRU policy
      lastModified = new Date();
   }

   /**
    * Initializes the buffer's page according to the specified formatter,
    * and appends the page to the specified file.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    * 
    * CS4432-Project1: This method updates the date of last modification
    */
   void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      fmtr.format(contents);
      blk = contents.append(filename);
      pins = 0;
      //CS4432-Project1: Update last modified date to current time. Used by LRU policy
      lastModified = new Date();
   }
}