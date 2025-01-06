/* 
  Author: Michael Tufillaro
  Date: 04/22/2024
*/

/***
 *      ______ .___  ___. .______     _______.  ______              ____    __   __  
 *     /      ||   \/   | |   _  \   /       | /      |            |___ \  /_ | /_ | 
 *    |  ,----'|  \  /  | |  |_)  | |   (----`|  ,----'              __) |  | |  | | 
 *    |  |     |  |\/|  | |   ___/   \   \    |  |                  |__ <   | |  | | 
 *    |  `----.|  |  |  | |  |   .----)   |   |  `----.             ___) |  | |  | | 
 *     \______||__|  |__| | _|   |_______/     \______|            |____/   |_|  |_| 
 *                                                                                   
 */

//This was included to allow the use of boolean variables
#include <stdbool.h> 
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "mdadm.h"
#include "util.h"
#include "jbod.h"
#include "cache.h"
#include "net.h"

//boolean that keeps track of whether or the JBOD has been mounted
static bool isMounted = false;


//helper method that takes in diskID, blockID, and command and puts it into one unsigned int
uint32_t encode(int diskID, int blockID, int command, int reserved)
{
  uint32_t retval = 0x0, tempa, tempb, tempc, tempd;
  //shifts values then adds them together
  tempa = (reserved&0xff);
  tempb = (command&0xff) << 14;
  tempc = (blockID&0xff) << 20;
  tempd = (diskID&0xff) << 28;
  retval  = tempa|tempb|tempc|tempd;
  return retval;
}



int mdadm_mount(void) 
{
  //if it is unmounted, mounts it and returns 1
  if (!isMounted)
  {
    isMounted = true;
    //calls encode helper method to make create op variable
    uint32_t op = encode(0, 0, JBOD_MOUNT, 0);
    jbod_client_operation(op, NULL);
    return 1;
  }
  return -1;
}



int mdadm_unmount(void) 
{
  //if it is mounted, unmounts it and returns 1
  if (isMounted)
  {
    isMounted = false;
    //calls encode helper method to make create op variable
    uint32_t op = encode(0, 0, JBOD_UNMOUNT, 0);
    jbod_client_operation(op, NULL);
    return 1;
  }
  return -1;
}



//helper method that checks the input of mdadm_read and determines if it is valid or not
bool inputCheck(uint32_t addr, uint32_t len, bool isNull)
{
  bool invalidInput = false;
  //checks to see if the address is out of bounds or if the input is otherwise invalid
  if ((addr + len) > (JBOD_DISK_SIZE * JBOD_NUM_DISKS) || addr < 0 || len > 1024 || len < 0)
  {
    invalidInput = true;
  }
  //if read is passed a NULL pointer and non-zero length, it is invalid
  if (isNull && len != 0)
  {
    invalidInput = true;
  }
  return invalidInput;
}



int mdadm_read(uint32_t addr, uint32_t len, uint8_t *buf) 
{
  //boolean that holds whether the buffer is null
  bool isNull = (buf == NULL);
  //calls helper method to determine if the input is invalid
  bool invalidInput = inputCheck(addr, len, isNull);

  //returns -1 if the input is invalid or if read is called when it is unmounted 
  if (invalidInput || !isMounted)
  {
    return -1;
  }
  
  //caluclates disk and block id
  int diskID = addr / JBOD_DISK_SIZE;
  int blockID = (addr - (diskID * JBOD_DISK_SIZE))/JBOD_BLOCK_SIZE;

  //calls seek operations to move the IO values to the correct starting position
  int32_t op = encode(diskID, 0, JBOD_SEEK_TO_DISK, 0);
  int seekDiskCheck = jbod_client_operation(op, NULL);
  op = encode(0, blockID, JBOD_SEEK_TO_BLOCK, 0);
  int seekBlockCheck = jbod_client_operation(op, NULL);
  op = encode(0, 0, JBOD_READ_BLOCK, 0);
  int readBlockCheck = 0;
  int readCacheCheck = 0;
  //creates a buffer the size of a block
  uint8_t buffer[JBOD_BLOCK_SIZE];
  
  
  if(cache_enabled())
  {
    //calls cache lookup to see if the current block is in the cache
    readCacheCheck = cache_lookup(diskID, blockID, buffer);
    //if it is not in the cache, calls the JBOD operation to read from the database into the buffer and then writes the block to the cache
    if (readCacheCheck == -1)
    {
      //calls read block operation to read data from current disk and block into buffer
      readBlockCheck = jbod_client_operation(op, buffer);
      cache_insert(diskID, blockID, buffer);
    }
    else
    {
      //if cache_lookup is successful, seek to blockID + 1
      op = encode(0, blockID + 1, JBOD_SEEK_TO_BLOCK, 0);
      seekBlockCheck = jbod_client_operation(op, NULL);
    }
  }
  else
  {
    //calls read block operation to read data from current disk and block into buffer
    readBlockCheck = jbod_client_operation(op, buffer);
  }
  

  //returns -1 if any of the checks failed
  if (seekDiskCheck == -1 || seekBlockCheck == -1 || readBlockCheck == -1)
  {
    return -1;
  }
  
  //keeps track of how many bits have been read
  uint32_t remainingReadLen = len;

  //finds the starting address of the current block and disk
  int startAddrOfBlock = ((diskID) * JBOD_NUM_BLOCKS_PER_DISK * JBOD_BLOCK_SIZE) + ((blockID)*(JBOD_BLOCK_SIZE));
  //finds the index of addr within the current block
  int currentAddr = addr - startAddrOfBlock;
  
  //value that keeps track of how many numbers have been read into buf
  int numRead = 0;


  //loops until all required values have been read
  while (remainingReadLen > 0)
  {
    //variable that calculates the last index of the current disk
    int endOfDisk = ((diskID + 1)*(JBOD_BLOCK_SIZE * JBOD_NUM_BLOCKS_PER_DISK)) - 1;

    //if the read would extend beyond the current disk/block and the current block is the last on the disk
    if (((addr + numRead + remainingReadLen) > endOfDisk) && (currentAddr + remainingReadLen > JBOD_BLOCK_SIZE) && (blockID == JBOD_NUM_BLOCKS_PER_DISK - 1))
    {
      //copies the information in the buffer into the buf array
      memcpy(&buf[numRead], buffer + currentAddr, JBOD_BLOCK_SIZE - currentAddr);
      //subtracts the amount that was read from the remainingReadLen and adds it to numRead
      remainingReadLen -= (JBOD_BLOCK_SIZE - currentAddr);
      numRead += (JBOD_BLOCK_SIZE - currentAddr);
      //sets the currentAddr to 0 since the next read will start at the start of a block
      currentAddr = 0;

      //Moves to the next disk
      diskID++;
      blockID = 0;
      //calls seek operations to move the IO value to the correct starting position
      op = encode(diskID, 0, JBOD_SEEK_TO_DISK, 0);
      seekDiskCheck = jbod_client_operation(op, NULL);

      //primes the op for use in the read block method
      op = encode(0, 0, JBOD_READ_BLOCK, 0);


      if(cache_enabled())
      {
        //calls cache lookup to see if the current block is in the cache
        readCacheCheck = cache_lookup(diskID, blockID, buffer);
        //if it is not in the cache, calls the JBOD operation to read from the database into the buffer and then writes the block to the cache
        if (readCacheCheck == -1)
        {
          //calls read block operation to read data from current disk and block into buffer
          readBlockCheck = jbod_client_operation(op, buffer);
          cache_insert(diskID, blockID, buffer);
        }
        else
        {
          //if cache_lookup is successful, seek to blockID + 1
          op = encode(0, blockID + 1, JBOD_SEEK_TO_BLOCK, 0);
          seekBlockCheck = jbod_client_operation(op, NULL);
        }
      }
      else
      {
        //calls read block operation to read data from current disk and block into buffer
        readBlockCheck = jbod_client_operation(op, buffer);
      }


      //returns -1 if the block reading/disk movement fails
      if (seekDiskCheck == -1 || readBlockCheck == -1)
      {
        return -1;
      }
    }


    //if the read would extend beyond the current block
    else if((currentAddr + remainingReadLen) > JBOD_BLOCK_SIZE)
    {
      //copies the information in the buffer into the buf array
      memcpy(&buf[numRead], buffer + currentAddr, JBOD_BLOCK_SIZE - currentAddr);
      //subtracts the amount that was read from the remainingReadLen and adds it to numRead
      remainingReadLen -= (JBOD_BLOCK_SIZE - currentAddr);
      numRead += (JBOD_BLOCK_SIZE - currentAddr);
      //sets the currentAddr to 0 since the next read will start at the start of a block
      currentAddr = 0;
      //primes the op for use in the read block method
      op = encode(0, 0, JBOD_READ_BLOCK, 0);


      if(cache_enabled())
      {
        //calls cache lookup to see if the current block is in the cache
        readCacheCheck = cache_lookup(diskID, blockID, buffer);
        //if it is not in the cache, calls the JBOD operation to read from the database into the buffer and then writes the block to the cache
        if (readCacheCheck == -1)
        {
          //calls read block operation to read data from current disk and block into buffer
          readBlockCheck = jbod_client_operation(op, buffer);
          cache_insert(diskID, blockID, buffer);
        }
        else
        {
          //if cache_lookup is successful, seek to blockID + 1
          op = encode(0, blockID + 1, JBOD_SEEK_TO_BLOCK, 0);
          seekBlockCheck = jbod_client_operation(op, NULL);
        }
      }
      else
      {
        //calls read block operation to read data from current disk and block into buffer
        readBlockCheck = jbod_client_operation(op, buffer);
      }


      //returns -1 if the block reading fails
      if (readBlockCheck == -1)
      {
        return -1;
      }
      blockID++;
    }


    //if the read stays within the current block
    else if((currentAddr + remainingReadLen) <= JBOD_BLOCK_SIZE)
    {
      //read however much is left on remainingReadLen into buf, and set the remaining read length to zero
      memcpy(&buf[numRead], buffer + currentAddr, remainingReadLen);
      remainingReadLen = 0;
    }
  }
  return len;
}


int mdadm_write(uint32_t addr, uint32_t len, const uint8_t *buf) 
{
  //boolean that holds whether the buffer is null
  bool isNull = (buf == NULL);
  //calls helper method to determine if the input is invalid
  bool invalidInput = inputCheck(addr, len, isNull);

  //returns -1 if the input is invalid or if read is called when it is unmounted 
  if (invalidInput || !isMounted)
  {
    return -1;
  }
  
  //caluclates disk and block id
  int diskID = addr / JBOD_DISK_SIZE;
  int blockID = (addr - (diskID * JBOD_DISK_SIZE))/JBOD_BLOCK_SIZE;

  //calls seek operations to move the IO values to the correct starting position
  int32_t op = encode(diskID, 0, JBOD_SEEK_TO_DISK, 0);
  int seekDiskCheck = jbod_client_operation(op, NULL);
  op = encode(0, blockID, JBOD_SEEK_TO_BLOCK, 0);
  int seekBlockCheck = jbod_client_operation(op, NULL);
  op = encode(0, 0, JBOD_WRITE_BLOCK, 0);
  //finds the starting address of the current block and disk
  int startAddrOfBlock = ((diskID) * JBOD_NUM_BLOCKS_PER_DISK * JBOD_BLOCK_SIZE) + ((blockID)*(JBOD_BLOCK_SIZE));
  //finds the index of addr within the current block
  int currentAddr = addr - startAddrOfBlock;

  //creates buffer the size of a block
  uint8_t buffer[JBOD_BLOCK_SIZE];

  //calls mdadm_read to read a copy of the block
  int readBlockCheck = mdadm_read(startAddrOfBlock, JBOD_BLOCK_SIZE, buffer);


  //value that keeps track of how many numbers have been read from buf
  int numRead = 0;
  //keeps track of how many bits have been read
  uint32_t remainingReadLen = len;

  //check used to see if the write block method failed
  int writeBlockCheck = 0;
  //returns -1 if any of the checks failed
  if (seekDiskCheck == -1 || seekBlockCheck == -1 || readBlockCheck == -1)
  {
    return -1;
  }


  //loops until all values from buf have been read/written into the JBOD
  while (remainingReadLen > 0)
  {
    startAddrOfBlock = ((diskID) * JBOD_NUM_BLOCKS_PER_DISK * JBOD_BLOCK_SIZE) + ((blockID)*(JBOD_BLOCK_SIZE));

    //calls the seek to block method again becuase reading the block causes the IO value to shift
    op = encode(0, blockID, JBOD_SEEK_TO_BLOCK, 0);
    seekBlockCheck = jbod_client_operation(op, NULL);
    op = encode(0, 0, JBOD_WRITE_BLOCK, 0);

    if (seekBlockCheck == -1)
    {
      return -1;
    }
 
    int readCacheCheck = 0;
    //creates a buffer the size of a block that is used when checking if a block is in the cache
    uint8_t tempBuffer[JBOD_BLOCK_SIZE];

    //if the write would extend beyond the current block and the current block is the last on the disk
    if ((currentAddr + remainingReadLen > JBOD_BLOCK_SIZE) && (blockID == JBOD_NUM_BLOCKS_PER_DISK - 1))
    {

      //replace however many numbers are needed in the current block into buffer
      memcpy(&buffer[currentAddr], &buf[numRead], JBOD_BLOCK_SIZE - currentAddr);
      //subtracts the amount that was read from the remainingReadLen and adds it to numRead
      remainingReadLen -= (JBOD_BLOCK_SIZE - currentAddr);
      numRead += (JBOD_BLOCK_SIZE - currentAddr);
      //sets the currentAddr to 0 since the next read will start at the start of a block
      currentAddr = 0;
      
      //primes the op for use in the write block method
      op = encode(0, 0, JBOD_WRITE_BLOCK, 0);

    
      if(cache_enabled())
      {
        //calls cache_lookup to check if the block is already in the cache
        readCacheCheck = cache_lookup(diskID, blockID, tempBuffer);
        //if the block is currently not in the cache, writes it to the cache
        if (readCacheCheck == -1)
        {
          cache_insert(diskID, blockID, buffer);
        }
        //if the block is currently in the cache, updates it instead
        else
        {
          cache_update(diskID, blockID, buffer);
        }
      }
      //calls write block method to write the data into the JBOD
      writeBlockCheck = jbod_client_operation(op, buffer);

      //sets the disk and block ID's for the next read
      diskID++;
      blockID = 0;
      //calls seek operations to move the IO value to the correct starting position
      op = encode(diskID, 0, JBOD_SEEK_TO_DISK, 0);
      seekDiskCheck = jbod_client_operation(op, NULL);
      //calls mdadm_read to read a copy of the next block into buffer
      int readBlockCheck = mdadm_read(startAddrOfBlock + JBOD_BLOCK_SIZE, JBOD_BLOCK_SIZE, buffer);
      //returns -1 if the block reading/writing or disk movement fails
      if (seekDiskCheck == -1 || writeBlockCheck == -1 || readBlockCheck == -1)
      {
        return -1;
      }
    }
    

    //if the write would extend beyond the current block
    else if((currentAddr + remainingReadLen) > JBOD_BLOCK_SIZE)
    {
      //replace however many numbers are needed in the current block into buffer
      memcpy(&buffer[currentAddr], &buf[numRead], JBOD_BLOCK_SIZE - currentAddr);
      //subtracts the amount that was read from the remainingReadLen and adds it to numRead
      remainingReadLen -= (JBOD_BLOCK_SIZE - currentAddr);
      numRead += (JBOD_BLOCK_SIZE - currentAddr);
      //sets the currentAddr to 0 since the next read will start at the start of a block
      currentAddr = 0;
      //primes the op for use in the write block method
      op = encode(0, 0, JBOD_WRITE_BLOCK, 0);

      if(cache_enabled())
      {
        //calls cache_lookup to check if the block is already in the cache
        readCacheCheck = cache_lookup(diskID, blockID, tempBuffer);
        //if the block is currently not in the cache, writes it to the cache
        if (readCacheCheck == -1)
        {
          cache_insert(diskID, blockID, buffer);
        }
        //if the block is currently in the cache, updates it instead
        else
        {
          cache_update(diskID, blockID, buffer);
        }
      }

      //calls write block method to write the data into the JBOD
      writeBlockCheck = jbod_client_operation(op, buffer);
      //calls mdadm_read to read a copy of the next block into buffer
      int readBlockCheck = mdadm_read(startAddrOfBlock + JBOD_BLOCK_SIZE, JBOD_BLOCK_SIZE, buffer);
      //increments blockID by 1 
      blockID++;
      //returns -1 if the block reading fails
      if (writeBlockCheck == -1 || readBlockCheck == -1)
      {
        return -1;
      }
    }


    //if the read stays within the current block
    else if((currentAddr + remainingReadLen) <= JBOD_BLOCK_SIZE)
    {
      //replace the required values from buffer using memcpy
      memcpy(&buffer[currentAddr], &buf[numRead], remainingReadLen);
      //primes the op for use in the write block method
      op = encode(0, 0, JBOD_WRITE_BLOCK, 0);

      if(cache_enabled())
      {
        //calls cache_lookup to check if the block is already in the cache
        readCacheCheck = cache_lookup(diskID, blockID, tempBuffer);
        //if the block is currently not in the cache, writes it to the cache
        if (readCacheCheck == -1)
        {
          cache_insert(diskID, blockID, buffer);
        }
        //if the block is currently in the cache, updates it instead
        else
        {
          cache_update(diskID, blockID, buffer);
        }
      }

      //calls write block method to write info from buffer into blocks
      writeBlockCheck = jbod_client_operation(op, buffer);
      //adjusts numRead and remainingReadLen accordingly
      numRead += remainingReadLen;
      remainingReadLen = 0;
      //returns -1 if any of the checks failed
      if (writeBlockCheck == -1)
      {
        return -1;
      }
    }
  }

  return len;
}