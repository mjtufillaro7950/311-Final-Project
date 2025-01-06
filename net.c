#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <err.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include "net.h"
#include "jbod.h"


/* the client socket descriptor for the connection to the server */
int cli_sd = -1;

/* attempts to read n (len) bytes from fd; returns true on success and false on failure. 
It may need to call the system call "read" multiple times to reach the given size len. 
*/
static bool nread(int fd, int len, uint8_t *buf) 
{
  //repeatedly calls read until all len values have been read
  int numRead = 0;
  while (numRead < len)
  {
    numRead += read(fd, &buf[numRead], len - numRead);
  }
  if (numRead > len)
  {
    return false;
  }
  return true;
}

/* attempts to write n bytes to fd; returns true on success and false on failure 
It may need to call the system call "write" multiple times to reach the size len.
*/
static bool nwrite(int fd, int len, uint8_t *buf) 
{
  //repeatedly calls write until all len values have been written
  int numWritten = 0;
  while (numWritten < len)
  {
    numWritten += write(fd, &buf[numWritten], len - numWritten);
  }
  if (numWritten > len)
  {
    return false;
  }
  return true;
}

/* Through this function call the client attempts to receive a packet from sd 
(i.e., receiving a response from the server.). It happens after the client previously 
forwarded a jbod operation call via a request message to the server.  
It returns true on success and false on failure. 
The values of the parameters (including op, ret, block) will be returned to the caller of this function: 

op - the address to store the jbod "opcode"  
ret - the address to store the return value of the server side calling the corresponding jbod_operation function.
block - holds the received block content if existing (e.g., when the op command is JBOD_READ_BLOCK)

In your implementation, you can read the packet header first (i.e., read HEADER_LEN bytes first), 
and then use the length field in the header to determine whether it is needed to read 
a block of data from the server. You may use the above nread function here.  
*/
static bool recv_packet(int sd, uint32_t *op, uint16_t *ret, uint8_t *block) 
{
  //creates a buffer the size of the header length
  uint8_t header[HEADER_LEN];
  //reads the header from the system
  bool readReturn = nread(sd, HEADER_LEN, header);
  if (!readReturn)
  {
    return false;
  }

  //After reading, sets the variables equal to the ones recieved from nread, using memcpy
  //calls memcpy to put packet variables into parameters and convert them using ntohl/ntohs
  uint16_t len = 0;
  //placeholder served as the index, keeping track of where to continue reading from within the buffer
  int placeholder = 0;
  memcpy(&len, &header[placeholder], sizeof(uint16_t));
  len = ntohs(len);
  placeholder += sizeof(uint16_t);
  memcpy(op, &header[placeholder], sizeof(uint32_t));
  *op = ntohl(*op);
  placeholder += sizeof(uint32_t);
  memcpy(ret, &header[placeholder], sizeof(uint16_t));
  *ret = ntohs(*ret);
  placeholder += sizeof(uint16_t);
  //extracts the command from the op value
  uint32_t command = *op;
  //shifts the values right by 14 bits and then ANDs it with 111111 in hex (which is 0x3F) which replaces all other bits with 0
  command = (command >>14) & 0x3F;
  //if there is a block that needs to be read
  if (len > HEADER_LEN && (command == JBOD_READ_BLOCK || command == JBOD_SIGN_BLOCK))
  {
    //creates a buffer the size of a block, reads it, then puts it into the block parameter
    uint8_t blockPacket[JBOD_BLOCK_SIZE];
    readReturn = nread(sd, JBOD_BLOCK_SIZE, blockPacket);
    if (!readReturn)
    {
      return false;
    }
    memcpy(block, blockPacket, JBOD_BLOCK_SIZE);
  }
  return true;
}



/* The client attempts to send a jbod request packet to sd (i.e., the server socket here); 
returns true on success and false on failure. 

op - the opcode. 
block- when the command is JBOD_WRITE_BLOCK, the block will contain data to write to the server jbod system;
otherwise it is NULL.

The above information (when applicable) has to be wrapped into a jbod request packet (format specified in readme).
You may call the above nwrite function to do the actual sending.  
*/
static bool send_packet(int sd, uint32_t op, uint8_t *block) 
{
  //variable that determines the length of the packet, depending on if a block is needed or not
  uint16_t hLength = HEADER_LEN;
  //extracts the command from the op value
  uint32_t command = op;
  //shifts the values right by 14 bits and then ANDs it with 111111 in hex (which is 0x3F) which replaces all other bits with 0
  command = (command >>14) & 0x3F;
  if (command == JBOD_WRITE_BLOCK)
  {
    hLength += JBOD_BLOCK_SIZE;
  }
  
  uint16_t hReturnCode = 0;
  uint16_t nLength = htons(hLength);
  uint16_t nReturnCode = htons(hReturnCode);
  uint32_t nOp = htonl(op);
  
  
  //creates packet buffer
  uint8_t packet[HEADER_LEN + JBOD_BLOCK_SIZE];
  //memcpy all the values into it, with an int functioning as the placeholder/index.
  int placeholder = 0;
  memcpy(&packet[placeholder], &nLength, sizeof(uint16_t));
  placeholder += sizeof(uint16_t);
  memcpy(&packet[placeholder], &nOp, sizeof(uint32_t));
  placeholder += sizeof(uint32_t);
  memcpy(&packet[placeholder], &nReturnCode, sizeof(uint16_t));
  placeholder += sizeof(uint16_t);
  //if the required packet len is larger than header len, write block.
  if (hLength > HEADER_LEN)
  {
    memcpy(&packet[placeholder], block, JBOD_BLOCK_SIZE);
  }

  //call nwrite with the length and the packet, to write the data to the server
  bool writeReturn = nwrite(sd, hLength, packet);

  return writeReturn;
}



/* attempts to connect to server and set the global cli_sd variable to the
 * socket; returns true if successful and false if not. 
 * this function will be invoked by tester to connect to the server at given ip and port.
 * you will not call it in mdadm.c
*/
bool jbod_connect(const char *ip, uint16_t port) 
{
  //creates a structure that holds the family, port, and ip address
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  //call inet_aton with the ip and addr, which adds it to the struct
  if (inet_aton(ip, &(addr.sin_addr)) == 0) 
  {
    return false;
  }
  //creates a socket
  cli_sd = socket (AF_INET, SOCK_STREAM, 0);
  if (cli_sd == -1)
  {
    return false;
  } 

  
  //connects the server and returns true if it succeded
  int connectCheck = connect(cli_sd, (const struct sockaddr *)&addr, sizeof(addr));
  return (connectCheck == 0);
}




/* disconnects from the server and resets cli_sd */
void jbod_disconnect(void) 
{
  close(cli_sd);
  cli_sd = -1;
}



/* sends the JBOD operation to the server (use the send_packet function) and receives 
(use the recv_packet function) and processes the response. 

The meaning of each parameter is the same as in the original jbod_operation function. 
return: 0 means success, -1 means failure.
*/
int jbod_client_operation(uint32_t op, uint8_t *block) 
{
  //call send_packet and returns -1 if it is false
  bool sendCheck = send_packet(cli_sd, op, block);
  if (!sendCheck)
  {
    return -1;
  }
  //creates a return value and calls recv check
  uint16_t ret = 0;
  bool recvCheck = recv_packet(cli_sd, &op, &ret, block);
  return recvCheck && (ret == 0);
}
