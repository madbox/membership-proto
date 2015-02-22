/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 *              Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
  for( int i = 0; i < 6; i++ ) {
    NULLADDR[i] = 0;
  }
  this->memberNode = member;
  this->emulNet = emul;
  this->log = log;
  this->par = params;
  this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 *              This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
  if ( memberNode->bFailed ) {
    return false;
  }
  else {
    return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
  }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
  Queue q;
  return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 *              All initializations routines for a member.
 *              Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
  Address joinaddr;
  joinaddr = getJoinAddress();

  // Self booting routines
  if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
    exit(1);
  }

  if( !introduceSelfToGroup(&joinaddr) ) {
    finishUpThisNode();
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
    exit(1);
  }

  return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
  /*
   * This function is partially implemented and may require changes
   */
  int id = *(int*)(&memberNode->addr.addr);
  int port = *(short*)(&memberNode->addr.addr[4]);

//  log->LOG(&memberNode->addr, "Initialization started: '%i':'%i'", id, port);


  memberNode->bFailed = false;
  memberNode->inited = true;
  memberNode->inGroup = false;
  // node is up!
  memberNode->nnb = 0;
  memberNode->heartbeat = 0;
  memberNode->pingCounter = TFAIL;
  memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

//  log->LOG(&memberNode->addr, "Initialization done: '%i':'%i'", id, port);

  if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "!!!...");
#endif
    MemberListEntry newlistentry(id, port);
    memberNode->memberList.push_back(newlistentry);
    log->logNodeAdd(&memberNode->addr, joinaddr);
  }

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
  MessageHdr *msg;
#ifdef DEBUGLOG
  static char s[1024];
#endif

  if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
    // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Starting up group...");
#endif
    memberNode->inGroup = true;
  }
  else {
    //~ size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
    //~ msg = (MessageHdr *) malloc(msgsize * sizeof(char));
//~
    //~ // create JOINREQ message: format of data is {struct Address myaddr}
    //~ msg->msgType = JOINREQ;
    //~ memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    //~ memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

    size_t msgsize = sizeof(MessageHdr); // no additional data for JOINREQ, the header only
    MemberListEntry mle(0, 0, memberNode->heartbeat, par->getcurrtime());
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    msg->senderAddr = memberNode->addr;
    msg->senderData = mle;
    msg->msgDataSize = 0;

#ifdef DEBUGLOG
    sprintf(s, "Trying to join...");
    log->LOG(&memberNode->addr, s);
#endif

    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

    free(msg);
  }

  return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
  /*
   * Your code goes here
   */
  return(true);
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 *              Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
  if (memberNode->bFailed) {
    return;
  }

  // Check my messages
  checkMessages();

  // Wait until you're in the group...
  if( !memberNode->inGroup ) {
    return;
  }

  // ...then jump in and share your responsibilites!
  nodeLoopOps();

  return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
  void *ptr;
  int size;

  // Pop waiting messages from memberNode's mp1q
  while ( !memberNode->mp1q.empty() ) {
    ptr = memberNode->mp1q.front().elt;
    size = memberNode->mp1q.front().size;
    memberNode->mp1q.pop();
    recvCallBack((void *)memberNode, (char *)ptr, size);
  }
  return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
  /*
   * Your code goes here
   */

  MessageHdr *msg;
  MessageHdr *respmsg;
//  Address *newnodeaddr;

#ifdef DEBUGLOG
  char str[3000] = {0};
  int i;
  for(i=0;i<size;i++)
    sprintf(str+i*2,"%02x",data[i]);
  log->LOG(&memberNode->addr, "Message recieved, size: %i, '%s'", size, str);
#endif

  msg = (MessageHdr *) malloc(size * sizeof(char));
  memcpy((char *)(msg), data, size);

  // newnodeaddr = (Address *) malloc(sizeof(Address));
  // memcpy((char *)(newnodeaddr->addr), data+sizeof(MessageHdr), sizeof(newnodeaddr->addr));

  if(msg->msgType == JOINREQ) {
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Message type is JOINREQ: '%02x'", msg->msgType);
    log->LOG(&memberNode->addr, "New addr is: '%s'", msg->senderAddr.getAddress().c_str());
#endif

    MemberListEntry newlistentry;
    memberNode->memberList.push_back(newlistentry);
    log->logNodeAdd(&memberNode->addr, &msg->senderAddr);

    log->LOG(&memberNode->addr, "MemberListSize: %i", memberNode->memberList.size());

    // Acknowledge join request. Send memberlist back.
//    unsigned long membercount = memberNode->memberList.size();
    size_t memberlistdatalength = sizeof(MemberListEntry)*memberNode->memberList.size();
    size_t respmsgsize = sizeof(MessageHdr) + memberlistdatalength + 1;
    respmsg = (MessageHdr *) malloc(respmsgsize * sizeof(char));

    MemberListEntry mle(0, 0, memberNode->heartbeat, par->getcurrtime());
    // create JOINREP message: format of data is {struct Address myaddr}
    respmsg->msgType = JOINREP;
    //memcpy((char *)(respmsg+1), &membercount, sizeof(membercount));
    //memcpy((char *)(respmsg+1) + sizeof(membercount), memberNode->memberList.data(), memberlistdatalength);
    respmsg->senderAddr = memberNode->addr;
    respmsg->senderData = mle;
    respmsg->msgDataSize = memberlistdatalength;
    memcpy((char *)(respmsg) + sizeof(MessageHdr), memberNode->memberList.data(), memberlistdatalength);

#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Sending JOINREP to '%s'. msgsize: '%i', member count: '%i'",
             msg->senderAddr.getAddress().c_str(),
             respmsgsize,
             memberNode->memberList.size());
#endif

    // send JOINREP message to newby
    emulNet->ENsend(&memberNode->addr, &msg->senderAddr, (char *)respmsg, respmsgsize);

    free(respmsg);

  } else if (msg->msgType == JOINREP) {
    memberNode->inGroup = true;
    log->LOG(&memberNode->addr,
     "Message type JOINREP! I'm added to the group!: msgsize: '%i', senderAddr: '%s', senderData-id: '%i', msgDataSize: '%i'",
     size,
     msg->senderAddr.getAddress().c_str(),
     msg->senderData.getid(),
     msg->msgDataSize);
 //   memberNode->memberList.resize(msg->msgDataSize/sizeof(MemberListEntry));
  } else {
    log->LOG(&memberNode->addr, "Message type unexpected!: '%02x'", msg->msgType);
  };

  free(msg);
//  free(newnodeaddr);

  return(true);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 *              the nodes
 *              Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

  /*
   * Your code goes here
   */

#ifdef DEBUGLOG
//    log->LOG(&memberNode->addr, "nodeLoopOps");
#endif

  return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
  return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
  Address joinaddr;

  memset(&joinaddr, 0, sizeof(Address));
  *(int *)(&joinaddr.addr) = 1;
  *(short *)(&joinaddr.addr[4]) = 0;

  return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
  memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
  printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
         addr->addr[3], *(short*)&addr->addr[4]) ;
}
