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

int MP1Node::getid() const {
  return *(int*)(&memberNode->addr.addr);
}

int MP1Node::getport() const {
  return *(short*)(&memberNode->addr.addr[4]);
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
    // log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
    exit(1);
  }

  if( !introduceSelfToGroup(&joinaddr) ) {
    finishUpThisNode();
#ifdef DEBUGLOG
    // log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
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

//  // log->LOG(&memberNode->addr, "Initialization started: '%i':'%i'", id, port);
  srand(time(NULL));

  memberNode->bFailed = false;
  memberNode->inited = true;
  memberNode->inGroup = false;
  // node is up!
  memberNode->nnb = 0;
  memberNode->heartbeat = 0;
  memberNode->pingCounter = TFAIL;
  memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

  // log->LOG(&memberNode->addr, "Initialization done: '%i':'%i'", getid(), getport());

  if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
#ifdef DEBUGLOG
    // log->LOG(&memberNode->addr, "!!!...");
#endif
    MemberListEntry newlistentry(getid(), getport(), 0, par->getcurrtime());
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
    // log->LOG(&memberNode->addr, "Starting up group...");
#endif
    memberNode->inGroup = true;
  }
  else {
    size_t msgsize = sizeof(MessageHdr); // no additional data for JOINREQ, the header only
    MemberListEntry mle(getid(), getport(), memberNode->heartbeat, par->getcurrtime());
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    msg->senderAddr = memberNode->addr;
    msg->senderData = mle;
    msg->msgDataSize = 0;

#ifdef DEBUGLOG
    sprintf(s, "Trying to join...");
    // log->LOG(&memberNode->addr, s);
#endif

    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

    free(msg);
  }

  return 1;

}

int MP1Node::sendHeartbeatReply(Address *trg_addr){
    MessageHdr *msg;

    //TODO: do heartbeat++ here?

    size_t memberlistdatalength = sizeof(MemberListEntry)*memberNode->memberList.size();
    size_t respmsgsize = sizeof(MessageHdr) + memberlistdatalength + 1;
    msg = (MessageHdr *) malloc(respmsgsize * sizeof(char));

    MemberListEntry mle(getid(), getport(), memberNode->heartbeat, par->getcurrtime());
    msg->msgType = HEARTBEATREP;
    msg->senderAddr = memberNode->addr;
    msg->senderData = mle;

    // Adding bytes from MemberList.data() beyond the MessageHdr in Response Message.
    msg->msgDataSize = memberlistdatalength;
    memcpy((char *)(msg) + sizeof(MessageHdr), memberNode->memberList.data(), memberlistdatalength);

    #ifdef DEBUGLOG
    // log->LOG(&memberNode->addr, "Sending HEARTBEATREP to '%s'. msgsize: '%i', member count: '%i'",
    //         trg_addr->getAddress().c_str(),
    //         respmsgsize,
    //         memberNode->memberList.size());

    //~ char str3[3000] = {0};
    //~ if (memberNode->memberList.size()==9){
        //~ for(unsigned int i3=0;i3<32;i3++)
            //~ sprintf(str3+i3*2,"%02x",((char *)(memberNode->memberList.data()))[i3]);
        //~ // log->LOG(&memberNode->addr, "------ MemberList vector data: %s", str3);
    //~ }
    #endif

    // send HEARTBEATREP message to newby
    emulNet->ENsend(&memberNode->addr, trg_addr, (char *)msg, respmsgsize);

    free(msg);

    return 0;
}

int MP1Node::sendHeartbeat(Address *trg_addr){
  MessageHdr *msg;

  memberNode->heartbeat++;

  size_t msgsize = sizeof(MessageHdr); // no additional data for HEARTBEAT, the header only
  MemberListEntry mle(getid(), getport(), memberNode->heartbeat, par->getcurrtime());
  msg = (MessageHdr *) malloc(msgsize * sizeof(char));
  // create HEARTBEAT message: format of data is {struct Address myaddr}
  msg->msgType = HEARTBEAT;
  msg->senderAddr = memberNode->addr;
  msg->senderData = mle;
  msg->msgDataSize = 0;

  // send JOINREQ message to introducer member

  emulNet->ENsend(&memberNode->addr, trg_addr, (char *)msg, msgsize);

  free(msg);

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
  MessageHdr *msg;
  MessageHdr *respmsg;

//~ #ifdef DEBUGLOG
  //~ char str[3000] = {0};
  //~ int i;
  //~ for(i=0;i<size;i++)
    //~ sprintf(str+i*2,"%02x",data[i]);
  //~ // log->LOG(&memberNode->addr, "Message recieved, size: %i, '%s'", size, str);
//~ #endif

  // Reading incoming Message into the nice struct form of MessageHdr
  msg = (MessageHdr *) malloc(size * sizeof(char));
  memcpy((char *)(msg), data, size);

  if(msg->msgType == JOINREQ) { // ----------- JOINREQ type of incoming Message

    memberNode->memberList.push_back(msg->senderData);
    log->logNodeAdd(&memberNode->addr, &msg->senderAddr); // --------- Output for Grader

#ifdef DEBUGLOG
    // log->LOG(&memberNode->addr, "Message type is JOINREQ. Sender: Id '%i', Addr '%s'",
    //         msg->senderData.getid(),
    //         msg->senderAddr.getAddress().c_str());
    // log->LOG(&memberNode->addr, "MemberListSize: %i", memberNode->memberList.size());
#endif

    size_t memberlistdatalength = sizeof(MemberListEntry)*memberNode->memberList.size();
    size_t respmsgsize = sizeof(MessageHdr) + memberlistdatalength + 1;
    respmsg = (MessageHdr *) malloc(respmsgsize * sizeof(char));

    MemberListEntry mle(getid(), getport(), memberNode->heartbeat, par->getcurrtime());
    respmsg->msgType = JOINREP;
    respmsg->senderAddr = memberNode->addr;
    respmsg->senderData = mle;

    // Adding bytes from MemberList.data() beyond the MessageHdr in Response Message.
    respmsg->msgDataSize = memberlistdatalength;
    memcpy((char *)(respmsg) + sizeof(MessageHdr), memberNode->memberList.data(), memberlistdatalength);

#ifdef DEBUGLOG
    // log->LOG(&memberNode->addr, "Sending JOINREP to '%s'. msgsize: '%i', member count: '%i'",
    //         msg->senderAddr.getAddress().c_str(),
    //         respmsgsize,
    //         memberNode->memberList.size());

    char str3[3000] = {0};
    if (memberNode->memberList.size()==9){
        for(unsigned int i3=0;i3<32;i3++)
            sprintf(str3+i3*2,"%02x",((char *)(memberNode->memberList.data()))[i3]);
        // log->LOG(&memberNode->addr, "------ MemberList vector data: %s", str3);
    }
#endif

    // send JOINREP message to newby
    emulNet->ENsend(&memberNode->addr, &msg->senderAddr, (char *)respmsg, respmsgsize);

    free(respmsg);

  } else if (msg->msgType == JOINREP) { // ----------- JOINREP type of incoming Message
#ifdef DEBUGLOG
    // log->LOG(&memberNode->addr,
     //~ "Message type JOINREP! I'm added to the group!: msgsize: '%i', senderAddr: '%s', senderData-id: '%i', msgDataSize: '%i'",
     //~ size,
     //~ msg->senderAddr.getAddress().c_str(),
     //~ msg->senderData.getid(),
     //~ msg->msgDataSize);
#endif

    memberNode->inGroup = true;

    unsigned long incomingMemberListSize = 0;
    incomingMemberListSize = msg->msgDataSize / sizeof(MemberListEntry);

    // log->LOG(&memberNode->addr, "Incoming memberlist size: %i, current my member list size: %i", incomingMemberListSize, memberNode->memberList.size());

    // TODO: nullify memberList, it should be empty.
    memberNode->memberList.resize(incomingMemberListSize);
    memcpy(memberNode->memberList.data(), ((char *)(msg)) + sizeof(MessageHdr), msg->msgDataSize);

#ifdef DEBUGLOG
    char str2[3000] = {0};
    for(unsigned int i2=0;i2<msg->msgDataSize;i2++)
        sprintf(str2+i2*2,"%02x",(((char *)(msg)) + sizeof(MessageHdr))[i2]);
    // log->LOG(&memberNode->addr, "'%s'", str2);
#endif

  } else if (msg->msgType == HEARTBEAT) { // ----------- HEARTBEAT type of incoming Message
#ifdef DEBUGLOG
    // log->LOG(&memberNode->addr,
      //~ "Message type HEARTBEAT! msgsize: '%i', senderAddr: '%s', senderData-id: '%i', senderData-heartbeat: '%i'",
      //~ size,
      //~ msg->senderAddr.getAddress().c_str(),
      //~ msg->senderData.getid(),
      //~ msg->senderData.getheartbeat());
#endif

    updateMemberListTable(memberNode, &msg->senderData);

    // generating HEARTBEATREP message
    sendHeartbeatReply(&msg->senderAddr);

  } else if (msg->msgType == HEARTBEATREP) {
    updateMemberListTable(memberNode, &msg->senderData);

    // loading incoming memberlist
    vector <MemberListEntry> incomingMemberList;
    unsigned long incomingMemberListSize = 0;
    incomingMemberListSize = msg->msgDataSize / sizeof(MemberListEntry);

    // log->LOG(&memberNode->addr, "HEARTBEATREP Incoming memberlist size: %i, current my member list size: %i", incomingMemberListSize, memberNode->memberList.size());

    incomingMemberList.resize(incomingMemberListSize);
    memcpy(incomingMemberList.data(), ((char *)(msg)) + sizeof(MessageHdr), msg->msgDataSize);

    updateMemberListTable(memberNode, &incomingMemberList);

  } else { // ----------- UNKNOWN type of incoming Message
#ifdef DEBUGLOG
    // log->LOG(&memberNode->addr, "Message type unexpected!: '%02x'", msg->msgType);
#endif
  };

  free(msg);
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
  char addr_str[50];


  // log->LOG(&memberNode->addr, "MemberList size: '%i'", memberNode->memberList.size());
  // update this node heartbeat in memberlist and remove failed nodes

  clanupMemberListTable(memberNode);

  if (par->getcurrtime()%THEARTBEATRATE==0){
    int i = rand() % memberNode->memberList.size();
    //~ // log->LOG(&memberNode->addr, "Rand: '%i'", i);
    if (memberNode->memberList.at(i).getid() == getid()) {
      i = (i + 1) % memberNode->memberList.size();
      //~ // log->LOG(&memberNode->addr, "Rand2: '%i'", i);
    }

    sprintf(addr_str, "%i:0", memberNode->memberList.at(i).getid());
    Address trg_addr(addr_str);
    if (!(trg_addr == memberNode->addr)) {
      // log->LOG(&memberNode->addr, "Sending Heartbeat to '%s'", trg_addr.getAddress().c_str());
      sendHeartbeat(&trg_addr);
    } else {
        // log->LOG(&memberNode->addr, "Will not sending Heartbeat to myself '%s'", trg_addr.getAddress().c_str());
    }
  }

#ifdef DEBUGLOG
//    // log->LOG(&memberNode->addr, "nodeLoopOps");
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

void MP1Node::clanupMemberListTable(Member *memberNode) {
  for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
    if (it->id == getid()) {
//      // log->LOG(&memberNode->addr, "Updating own timestamp, memberlist size: '%i'", memberNode->memberList.size());
      it->timestamp = par->getcurrtime();
      ++it;
    } else if (par->getcurrtime() - it->timestamp > TFAIL) {
      // log->LOG(&memberNode->addr, "MemberList entry: '%i' failed. Erasing it!", it->id);
      char addr_str[50];
      sprintf(addr_str, "%i.0.0.0:0", it->id);
      Address tmp_addr(addr_str);
      log->logNodeRemove(&memberNode->addr, &tmp_addr);
      it = memberNode->memberList.erase(it);
    } else {
      ++it;
    }
  }
}

/**
 * FUNCTION NAME: updateMemberListTable
 *
 * DESCRIPTION: Update MemberList with new heartbeat and timestamp if entry with same id exists. If no such id found, than new entry will be added to MemberList.
 */
int MP1Node::updateMemberListTable(Member *memberNode, MemberListEntry *incomingMLE) {
  for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
    if (it->id == incomingMLE->getid()) {
      // log->LOG(&memberNode->addr, "updateMemberListTable - entry found: '%i', hb: '%i', inchb: '%i'", it->id, it->heartbeat, incomingMLE->heartbeat);
      if (it->heartbeat<incomingMLE->heartbeat) {
        it->heartbeat = incomingMLE->heartbeat;
        it->timestamp = par->getcurrtime();
      }
      return it->id;
      // TODO: test for duplicated MemberList entries
    }
  }
  // log->LOG(&memberNode->addr, "updateMemberListTable - entry NOT found: '%i'. Adding!", incomingMLE->getid());
  memberNode->memberList.push_back(*incomingMLE);

  char addr_str[50];
  sprintf(addr_str, "%i.0.0.0:0", incomingMLE->id);
  Address tmp_addr(addr_str);

  log->logNodeAdd(&memberNode->addr, &tmp_addr);
  return 0;
}

/**
 * FUNCTION NAME: updateMemberList
 *
 * DESCRIPTION: Update MemberList with new heartbeat and timestamp if entry with same id exists. If no such id found, than new entry will be added to MemberList.
 */
int MP1Node::updateMemberListTable(Member *memberNode, vector <MemberListEntry> *incomingML) {
  for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
    for (auto it_inc = incomingML->begin(); it_inc != incomingML->end();) {
      if (it->id == it_inc->id) {
        if (it->heartbeat<it_inc->heartbeat) {
          it->heartbeat = it_inc->heartbeat;
          it->timestamp = it_inc->timestamp;
        }
        it_inc = incomingML->erase(it_inc);
      } else {
        ++it_inc;
      }
    }

  }

  if (incomingML->size() > 0) {
    // log->LOG(&memberNode->addr, "updateMemberListTable v - entries NOT found: '%i'pcs. Adding!", incomingML->size());

    for (auto it_inc = incomingML->begin(); it_inc != incomingML->end(); ++it_inc) {

      if (par->getcurrtime() - it_inc->timestamp < TFAIL) {
          memberNode->memberList.push_back(*it_inc);
          char addr_str[50];
          sprintf(addr_str, "%i.0.0.0:0", it_inc->id);
          Address tmp_addr(addr_str);
          log->logNodeAdd(&memberNode->addr, &tmp_addr);
      }
    }
  }
  return 0;
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
