package bftsmart.multi_zone;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.communication.SystemMessage;
import bftsmart.tom.util.TOMUtil;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MZNodeMan {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ServerViewController controller;
    private ServerCommunicationSystem cs;                       // comunication system
    private Map<Integer, HashSet<Integer>> subscriberMap;       // nodeId --> stripeId other nodes subscribed
    private MZSubscribeMan mzSubscribeMan;
    private MZRelayerMan mzRelayerMan;
    private Map<Integer, Long> heartbeatRecvTime;           // heartbeat msg received time.
    private Map<Integer, Long> nextTimeBroadcastHeartbeat;      // nextTimeBroadcastHeartbeat
    private boolean isRelayer = false;
    private boolean isConsensusNode = false;
    private MZMessageSeralizeTool mzmMsgSrlzTool;

    // to process LATENCY_DETECT and RELAYER msgs.
    private LinkedList<MZMessage> gossipedMsg; // msg(LATENCY-DETECT, RELAYER) that I received and broadcasted.
    private Map<MZMessage, HashSet<Integer>> gossipedMsgSender; // msg(LATENCY-DETECT, RELAYER) they send to me.
    private MZMessage lastReceivedRelayerMsg;
    private Map<Integer, Map<Integer, Long>> routingTable; // Map<consensusNode, <nextJumpNode, latency>>

    // stripe, block, mzbatch that need to forward.
    private Queue<ArrayList<Object>> dataQueue;
    private Map<String, SystemMessage> candidateBlockMap;

    // ordinary block I send and received 
    private Map<SystemMessage, Set<Integer>>    blockSendMap;
    private Map<SystemMessage, Set<Integer>>    blockRecvMap;
    private Map<SystemMessage, Integer>         msgSendCntMap;

    public Map<String, MZBlock> blockchain;

    private int myId;
    private int myZoneId;

    // for asking relay_node info
    private AtomicLong lastTimeAskRelayNode = new AtomicLong();
    private AtomicInteger nRelayNodesMsgReceived = new AtomicInteger();

    // last time broadcast relayer
    private AtomicLong nextTimeBroadcastRelayer = new AtomicLong();
    private AtomicLong nextTimeBroadcastLatencyDetect = new AtomicLong();
    private AtomicLong nextTimeDetect = new AtomicLong();

    public MZNodeMan(ServerViewController controller, ServerCommunicationSystem cs) {
        this.controller = controller;
        this.myId = this.controller.getStaticConf().getProcessId();
        this.myZoneId = this.controller.getStaticConf().getZoneId(myId);

        this.subscriberMap = new ConcurrentHashMap<>();
        this.mzRelayerMan = new MZRelayerMan(this.myId, this.myZoneId);
        this.mzSubscribeMan = new MZSubscribeMan(this.myId);

        this.heartbeatRecvTime = new HashMap<>();
        this.routingTable = new ConcurrentHashMap<>();
        this.nextTimeBroadcastHeartbeat = new ConcurrentHashMap<>();

        this.gossipedMsg = new LinkedList<>();
        this.gossipedMsgSender = new ConcurrentHashMap<>();
        this.lastReceivedRelayerMsg = null;

        // for send data to subscriber node.
        this.dataQueue = new ConcurrentLinkedQueue<>();
        this.candidateBlockMap = new ConcurrentHashMap<>();

        this.blockchain = new HashMap<>();

        this.isRelayer = false;
        this.isConsensusNode = false;
        this.cs = cs;
        this.mzmMsgSrlzTool = new MZMessageSeralizeTool(System.currentTimeMillis());

        this.lastTimeAskRelayNode.set(0);
        this.nRelayNodesMsgReceived.set(0);
        this.nextTimeBroadcastRelayer.set(0);
        this.nextTimeBroadcastLatencyDetect.set(0);

        this.blockRecvMap = new ConcurrentHashMap<>();
        this.blockSendMap = new ConcurrentHashMap<>();
        this.msgSendCntMap = new ConcurrentHashMap<>();

        this.nextTimeDetect.set(System.currentTimeMillis()+60000);
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes, int digit) {
		char[] hexChars = new char[bytes.length * 2];
		for ( int j = 0; j < bytes.length; j++ ) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
        String str = new String(hexChars);
		return str.substring(0,Math.min(digit, str.length()));
	}

    public MZMessage createMZMessage(int type, byte[] content) {
        long ts = System.currentTimeMillis();
        MZMessage replyMsg = new MZMessage(this.myId, type, ts, this.myZoneId, content);
        return replyMsg;
    }

    public void addForwardData(SystemMessage msg) {
        assert(msg != null);
        ArrayList<Object> arr = new ArrayList<>();
        arr.add(msg);
        arr.add(System.currentTimeMillis());
        this.dataQueue.add(arr);
    }

    // forward a new block to subscribers.
    public SystemMessage getCandidateBlock(byte[] blockHash) {
        String hash = MZNodeMan.bytesToHex(blockHash, 16);
        SystemMessage msg = this.candidateBlockMap.get(hash);
        if (msg == null) {
            logger.info("getCandidateBlock can not find the candidateblock [{}]", hash);
            return null;
        }
        return msg;
    }

    public void addMZCandidateBlock(byte[] blockHash, Mz_Propose propose, byte[] seralizedPropose) {
        if (seralizedPropose == null || propose == null) {
            logger.error("Error candidateblock {}", bytesToHex(blockHash, 8));
        }
        MZBlock candidateBlock = new MZBlock(myId, blockHash, propose, seralizedPropose);
        String hash = MZNodeMan.bytesToHex(blockHash, 16);
        this.candidateBlockMap.put(hash, candidateBlock);
        // logger.info("addMZCandidateBlock: Node {} add msg [{}] to candidateblock [{}]", myId, hash, candidateBlock.toString());
    }

    public void storeCandidateBlock(byte[] blockHash, ConsensusMessage msg) {
        String hash = MZNodeMan.bytesToHex(blockHash, 16);
        this.candidateBlockMap.put(hash, msg);
        logger.info("storeCandidateBlock: Node {} add msg: [{}] to candidateblock, blockheight: [{}]", myId, hash, msg.getNumber());
    }

    public DataHashMessage generateDataHashMsg(SystemMessage msg) {
        DataHashMessage datahashMsg = null;
        long now = System.currentTimeMillis();
        int dataType = -1;
        byte[] dataHash = null;
        int[] appendix = null;
        if (msg instanceof ConsensusMessage ){
            ConsensusMessage consMsg = (ConsensusMessage)(msg);
            if (consMsg.getType() == MessageFactory.MZPROPOSE) {
                dataType = TOMUtil.DH_BLODKHASH;
                dataHash = consMsg.getHash();
                appendix = new int[]{consMsg.getNumber()};
            }
        }
        else if (msg instanceof MZBlock) {
            MZBlock block = (MZBlock)(msg);
            block.getPropose();
            dataType = TOMUtil.DH_BLODKHASH;
            dataHash = block.getBlockHash();
            appendix = new int[]{block.getPropose().blockHeight};
        }
        else if (msg instanceof MZStripeMessage) {
            MZStripeMessage stripeMsg = (MZStripeMessage)(msg);
            dataType = TOMUtil.DH_STRIPEHASH;
            dataHash = null;
            appendix = new int[3];
            appendix[0] = stripeMsg.getBatchChainId();
            appendix[1] = stripeMsg.getHeight();
            appendix[2] = stripeMsg.getStripeId();
        }
        datahashMsg = new DataHashMessage(this.myId, dataType, now, appendix, dataHash);
        return datahashMsg;
    }

    public void ForwardDataToSubscriber() {
        if (this.dataQueue.isEmpty()) 
            return;
        // logger.info("dataQueue size: {}, consensus nodes: {}, subscriberMap: {}", dataQueue.size(), this.controller.getCurrentViewAcceptors(),this.subscriberMap);
        int networkmode = this.controller.getStaticConf().getNetworkingMode();
        int fabricTTL = this.controller.getStaticConf().getFabricTTL();
        int fabricFout = this.controller.getStaticConf().getFabricFout();
        int fabricTTLDirect = this.controller.getStaticConf().getFabricTTLDirect();
        while (this.dataQueue.isEmpty() == false) {
            ArrayList<Object> element = this.dataQueue.poll();
            SystemMessage msg = (SystemMessage)(element.get(0));
            long enterTime = (long)(element.get(1));
            long now = System.currentTimeMillis() ;
            long diff = now - enterTime;
            if (diff > 5000) {
                if (msg instanceof MZBlock) {
                    MZBlock block = (MZBlock)(msg);
                    logger.info("forward {} exceed {} ms", block.toString(), diff);
                }
                else if (msg instanceof MZStripeMessage) {
                    MZStripeMessage stripe = (MZStripeMessage)(msg);
                    logger.info("forward {} exceed {} ms", stripe.toString(), diff);
                }
            }
            msg.setSender(this.myId);
            if (this.controller.isInCurrentView() && msg instanceof MZBlock ) {
                // change block sending time
                MZBlock block = (MZBlock)(msg);
                logger.info("forward blockheight: {}, wait {} ms", block.getBlockHeight(), diff);
                block.getPropose().timestamp = now;
                msg = block;
            }
            if (networkmode == TOMUtil.NM_STAR) {
                // only consensus nodes forward data to other nodes in a star topology.
                if (this.controller.isInCurrentView() == false)
                    continue;
                ArrayList<Integer> neighbors = getNeighborNodesWithoutConsensusNodes();
                multicastMsg(msg, neighbors);
                // logger.info("ForwardDataToSubscriber Node {} broadcast [{}] to neighbors: {}", this.myId, msg.toString(), neighbors);
            }
            else if (networkmode == TOMUtil.NM_MULTI_ZONE){
                for(Map.Entry<Integer, HashSet<Integer>> entry: this.subscriberMap.entrySet()) {
                    int nodeId = entry.getKey();
                    int[] target = {nodeId};
                    if (msg instanceof  ConsensusMessage) {
                        ConsensusMessage conMsg = (ConsensusMessage)(msg);
                        // logger.info("ForwardDataToSubscriber Node {} broadcast {} to {}", this.myId, msg.toString(), target);
                        sendMsg(target, conMsg);
                    } else if (msg instanceof MZBlock) {
                        MZBlock block = (MZBlock)(msg);
                        // seralize block content
                        Mz_Propose propose = block.getPropose();
                        // logger.info("ForwardDataToSubscriber Node {} boradcast block {} to {}, propose {}, batch: {}",this.myId, bytesToHex(block.getBlockHash(),8), target, propose.toString(), block.getBlockContent().length);
                        sendMsg(target, block);
                    } else if (msg instanceof MZStripeMessage){
                        MZStripeMessage stripe = (MZStripeMessage)(msg);
                        HashSet<Integer> stripeSet = entry.getValue();
                        if (stripeSet == null || stripeSet.contains(stripe.getStripeId())) {
                            // logger.info("ForwardDataToSubscriber Node {} broadcast stripe {} to subscriber {}", this.myId, msg.toString(), target);
                            sendMsg(target, stripe);
                        } 
                    }
                }
                if (msg instanceof DataHashMessage) {
                    // multicast data hash to extra neighbors for robustness
                    ArrayList<Integer> neighbors = this.getNeighborNodesWithoutConsensusNodes();
                    if (this.isConsensusNode()) {
                        Set<Integer> relayerSet = this.mzRelayerMan.getAllRelayers();
                        neighbors = new ArrayList<Integer>(relayerSet);
                    } else {
                        neighbors = this.getNeighborNodesWithoutConsensusNodes();
                    }
                    neighbors.removeAll(this.subscriberMap.keySet());
                    multicastMsg(msg, neighbors);
                } else {
                    // add those datahashMsg to queue and broadcast later
                    DataHashMessage datahashMsg = generateDataHashMsg(msg);
                    addForwardData(datahashMsg);
                }
            }
            else if (networkmode == TOMUtil.NM_RANDOM) {
                int ds = this.controller.getStaticConf().getDataDisForRandom();
                ArrayList<Integer> neighbors = this.getNeighborNodesWithoutConsensusNodes();
                if (ds == TOMUtil.DS_RANDOM_ENHANCED_FAB) {
                    if (this.msgSendCntMap.containsKey(msg) == false)
                        this.msgSendCntMap.put(msg, 0);
                    int sendCnt = this.msgSendCntMap.get(msg);
                    // leader node only send once or stop to send when sendCnt exceeds TTL
                    if ((sendCnt == 1 && this.controller.isInCurrentView()) || sendCnt > fabricTTL)
                        continue;
                    // randomly choose fout neighbors
                    Collections.shuffle(neighbors);
                    ArrayList<Integer> foutReceivers = new ArrayList<>();
                    for(int i = 0; i < Math.min(fabricFout, neighbors.size()); ++i)
                        foutReceivers.add(neighbors.get(i));
                    // directlly send block when sendCnt < TTLdirect
                    if (sendCnt < fabricTTLDirect) {
                        neighbors.addAll(foutReceivers);
                        multicastMsg(msg, foutReceivers);
                        // logger.info("ForwardDataToSubscriber Node {} broadcast [{}] to neighbors {}, sendcnt: {}", this.myId, msg , foutReceivers, sendCnt);
                    } 
                    // send digest.
                    else {
                        DataHashMessage datahashMsg = generateDataHashMsg(msg);
                        multicastMsg(datahashMsg, foutReceivers);
                        // logger.info("ForwardDataToSubscriber Node {} broadcast datahash [{}] to neighbors {}", this.myId, datahashMsg.toString(), foutReceivers);
                    }
                    ++sendCnt;
                    this.msgSendCntMap.put(msg, sendCnt);
                }
                else if (ds == TOMUtil.DS_RANDOM_BTC) {
                    if (msg instanceof MZStripeMessage) {
                        DataHashMessage datahashMsg = generateDataHashMsg(msg);
                        multicastMsg(datahashMsg, neighbors);
                    }else {
                        multicastMsg(msg, neighbors);
                    }
                    
                }
                else if (ds == TOMUtil.DS_RANDOM_ETH) {
                    if (msg instanceof MZStripeMessage) {
                        Collections.shuffle(neighbors);
                        double tmp = Math.sqrt(Long.valueOf(neighbors.size()));
                        int transferLen = (int)Math.max(Math.ceil(tmp), 4);
                        transferLen = Math.min(transferLen, neighbors.size());
                        ArrayList<Integer> receiveDataNeighbors = new ArrayList<>();
                        ArrayList<Integer> receiveHashNeighbors = new ArrayList<>();
                        for(int i = 0; i < neighbors.size(); ++i) {
                            if (i < transferLen)
                                receiveDataNeighbors.add(neighbors.get(i));
                            else
                                receiveHashNeighbors.add(neighbors.get(i));
                        }
                        multicastMsg(msg, receiveDataNeighbors);
                        DataHashMessage datahashMsg = generateDataHashMsg(msg);
                        multicastMsg(datahashMsg, receiveHashNeighbors);
                    } else{
                        multicastMsg(msg, neighbors);
                    }
                }
            }
            // broadcast data hash to other nodes.
            // Todo
        }
    }

    public ArrayList<Integer> getNeighborNodes() {
        return cs.getServersConn().getNeighborNodes();
    }

    public ArrayList<Integer> getZoneNeighbors(int zone) {
        ArrayList<Integer> neighbors = getNeighborNodes();
        ArrayList<Integer> res = new ArrayList<>();
        for(int node : neighbors) {
            int tmpZoneId = this.controller.getStaticConf().getZoneId(node);
            if (this.controller.isCurrentViewMember(node) || tmpZoneId == zone)
                res.add(node);
        }
        return res;
    }

    public ArrayList<Integer> getNeighborNodesWithoutConsensusNodes(){
        ArrayList<Integer> neighbors = cs.getServersConn().getNeighborNodes();
        int[] consensusNodes = this.controller.getCurrentViewAcceptors();
        ArrayList<Integer> tmpNodes = new ArrayList<>();
        for (int nodeId: consensusNodes) {
            tmpNodes.add(nodeId);
        }
        neighbors.removeAll(tmpNodes);
        return neighbors;
    }

    public boolean isRelayer() {
        return isRelayer;
    }

    public boolean isOrdinary(){
        return this.isRelayer() == false && this.controller.isInCurrentView() == false;
    }

    public void setRelayer(boolean state) {
        isRelayer = state;
    }

    public boolean isConsensusNode() {
        return isConsensusNode;
    }

    public void setConsensusNode(boolean state) {
        isConsensusNode = state;
    }

    public void addSubscriber(int subscriberId, int stripeId) {
        if (this.subscriberMap.containsKey(subscriberId) == false) {
            this.subscriberMap.put(subscriberId, new HashSet<Integer>());
        }
        this.subscriberMap.get(subscriberId).add(stripeId);
    }

    public void addSubscriber(int subscriberId, ArrayList<Integer> stripes) {
        if (stripes != null) {
            for (Integer i : stripes) {
                addSubscriber(subscriberId, i);
            }
        }
    }

    /**
     * add a message to gossip it
     * @param msg
     * @return
     */
    public boolean addGossipedMsg(MZMessage msg) {
        if (this.gossipedMsg.contains(msg) || msg.getSender() == myId)
            return false;
        this.gossipedMsg.add(msg);
        if (gossipedMsgSender.containsKey(msg) == false)
            gossipedMsgSender.put(msg, new HashSet<>());
        this.gossipedMsgSender.get(msg).add(msg.getSender());
        if (this.gossipedMsg.size() > 1000) {
            MZMessage tmpMsg = this.gossipedMsg.pollFirst();
            this.gossipedMsgSender.remove(tmpMsg);
        }
        return true;
    }

    public void sendSubscribeMsgTo(int target, Set<Integer> subStripes) {
        if (subStripes == null || subStripes.isEmpty() || target == this.myId)
            return;
        byte[] content = this.mzmMsgSrlzTool.seralizeSubscribe(subStripes);
        MZMessage subscribeMsg = createMZMessage(TOMUtil.SUBSCRIBE, content);
        int[] targets = { target };
        sendMsg(targets, subscribeMsg);
        this.mzSubscribeMan.addSubscribeMsg(target, new HashSet<Integer>(subStripes));
        logger.info("sendSubscribeMsgTo Send [{}] to {}, subscripes: {}", subscribeMsg, target, this.mzSubscribeMan.getSubscribeMsgSentTo(target));
    }

    public void sendUnSubscribedMsgTo(int target, HashSet<Integer> stripes) {
        byte[] content = this.mzmMsgSrlzTool.seralizeSubscribe(stripes);
        MZMessage unsubscribeMsg = createMZMessage(TOMUtil.UNSUBSCRIBE, content);
        int[] targets = { target };
        sendMsg(targets, unsubscribeMsg);
        // if I am a relayer node, update relayerStripeMap.
        if (isRelayer() && this.controller.isCurrentViewMember(target) ) {
            this.mzRelayerMan.deleteRelayerStripe(this.myId, this.myZoneId, new ArrayList<Integer>(stripes));
        }

        this.mzSubscribeMan.removeSubscribe(target, stripes);
        logger.info("sendUnSubscribedMsgTo Send [{}] {} to {}, current I relayStripeMap: {}", unsubscribeMsg, stripes, target, this.mzRelayerMan.getRelayerStripe(this.myId));
    }

    /**
     * received a GET_RELAY_NODE msg
     * 
     * @param msg
     */
    public void getRelayMsgReceived(MZMessage msg) {
        assert (msg != null);
        byte[] content = this.mzRelayerMan.seralizeRelayer(mzmMsgSrlzTool, msg.getZoneId());
        MZMessage replyMsg = createMZMessage(TOMUtil.RELAY_NODES, content);
        int target[] = { msg.getSender() };
        this.sendMsg(target, replyMsg);
        // logger.info("getRelayMsgReceived: received msg: [{}], relayerStripeMap: {}", msg.toString(),
        //         this.relayerStripeMap);
    }

    /**
     * received a RELAY_NODES msg
     * 
     * @param msg
     */
    public void relayNodesMsgReceived(MZMessage msg) {
        nRelayNodesMsgReceived.incrementAndGet();
        // skip a old relayer msg
        if (this.lastReceivedRelayerMsg != null &&
                this.lastReceivedRelayerMsg.getTimeStamp() > msg.getTimeStamp())
            return;
        HashMap<Integer, HashSet<Integer>> tmpRelayStripeMap = this.mzmMsgSrlzTool.deseralizeRelayNodes(msg.getValue());
        for (HashMap.Entry<Integer, HashSet<Integer>> entry : tmpRelayStripeMap.entrySet()) {
            int relayerId = entry.getKey();
            int relayerZone = this.controller.getStaticConf().getZoneId(relayerId);
            mzRelayerMan.addNewRelayer(relayerId, relayerZone, new ArrayList<>(entry.getValue()));
        }
        this.lastReceivedRelayerMsg = msg;
    }

    public void subscribeMsgReceived(MZMessage msg) {
        int maxSubscriber = this.controller.getStaticConf().getMaximumSubscriber();
        if (this.isRelayer())
            maxSubscriber += this.controller.getCurrentViewN();
        boolean senderIsRelayer = this.mzRelayerMan.isRelayer(msg.getSender());
        ArrayList<Integer> subStripes = this.mzmMsgSrlzTool.deseralizeSubscribe(msg.getValue());
        if (this.isConsensusNode() || this.subscriberMap.size() < maxSubscriber || senderIsRelayer) {
            if (this.subscriberMap.get(msg.getSender()) == null)
                this.subscriberMap.put(msg.getSender(), new HashSet<>());
            this.addSubscriber(msg.getSender(), subStripes);
            // if I am a leader, mark that node as a relayer.
            if (this.controller.isInCurrentView()) {
                mzRelayerMan.addRelayerStripes(msg.getSender(), msg.getZoneId(), subStripes);
                logger.info("{} is a new relayer and relayer {}", msg.getSender(), subStripes);
            }

            MZMessage replyMsg = createMZMessage(TOMUtil.ACCEPT_SUBSCRIBE, msg.getValue());
            int target[] = { msg.getSender() };
            sendMsg(target, replyMsg);
        } else {
            msg.setType(TOMUtil.REJECT_SUBSCRIBE);
            getRelayMsgReceived(msg);
            // reject the subscribe msg and return available child nodes.
            ArrayList<Integer> neighbors = new ArrayList<>(this.subscriberMap.keySet());
            byte[] data = this.mzmMsgSrlzTool.seralizeRejectSubscribe(neighbors, subStripes);
            msg.setValue(data);
            int sender =  msg.getSender();
            msg.setSender(this.myId);
            sendMsg(new int[]{sender}, msg);
            logger.info("Node {}'s subscriberMap: {}, send a rejectSubscribeMsg to {}", this.myId, this.subscriberMap, sender);
        }
    }

    public void unsubscribeMsgReceived(MZMessage msg) {
        if (this.subscriberMap.containsKey(msg.getSender()) == false)
            return;
        ArrayList<Integer> unsubStripes = this.mzmMsgSrlzTool.deseralizeSubscribe(msg.getValue());
        if (this.subscriberMap.containsKey(msg.getSender())) {
            this.subscriberMap.get(msg.getSender()).removeAll(unsubStripes);
            if (this.subscriberMap.get(msg.getSender()).isEmpty())
                this.subscriberMap.remove(msg.getSender());
        }

        // change node's relayer info
        if (this.controller.isInCurrentView()) {
            mzRelayerMan.deleteRelayerStripe(msg.getSender(), msg.getZoneId(), unsubStripes);
        }
        logger.info("unsubscribeMsgReceived receive [{}] , relayerStripeMap: {}, sender's subscriberMap: {}", msg,
            this.mzRelayerMan.getRelayerStripe(myId), this.subscriberMap.get(msg.getSender()));
    }

    public void acceptSubscribeMsgReceived(MZMessage msg) {
        ArrayList<Integer> subStripes = this.mzmMsgSrlzTool.deseralizeSubscribe(msg.getValue());
        HashSet<Integer> hs = new HashSet<>(subStripes);
        if(hs.isEmpty()){
            logger.warn("the subdstripe I send to {} is null", msg.getSender());
            return;
        }
        // if sender is a consensus node, then, mark myself is a new relayer
        if (this.controller.isCurrentViewMember(msg.getSender())) {
            mzRelayerMan.addRelayerStripes(this.myId, this.myZoneId, subStripes);
            this.setRelayer(true);
            logger.info("{} becomes a relayer and forward {}", this.myId, this.mzRelayerMan.getRelayerStripe(this.myId));
        }

        // send unsubscribe msg to previous sender
        HashMap<Integer, HashSet<Integer>> unsubscribeMap = new HashMap<>();
        if (hs != null) {
            HashMap<Integer, Integer> previousStripeSenderMap = this.mzSubscribeMan.getStripeSender(hs);
            this.mzSubscribeMan.addSubscribe(msg.getSender(), hs);
            for(int stripeId: previousStripeSenderMap.keySet()) {
                int previousSender = previousStripeSenderMap.get(stripeId);
                if (previousSender != this.mzSubscribeMan.noSender) {
                    if (unsubscribeMap.containsKey(previousSender) == false)
                        unsubscribeMap.put(previousSender, new HashSet<Integer>());
                        unsubscribeMap.get(previousSender).add(stripeId);
                }
            }
            for (HashMap.Entry<Integer, HashSet<Integer>> entry : unsubscribeMap.entrySet()) 
                sendUnSubscribedMsgTo(entry.getKey(), entry.getValue());
            // remove sent subscribe history
            this.mzSubscribeMan.removeSubscribeMsg(msg.getSender(), subStripes);
        }
        
        logger.info("acceptSubscribeMsgReceived received msg: {}, subscribeMap: {}, stripeSender: {}", 
            msg.toString(), this.mzSubscribeMan.subscribeMapToString(), this.mzSubscribeMan.stripeSenderMapToString());
        this.nextTimeBroadcastRelayer.set(System.currentTimeMillis());
    }

    /*
     * when received a reject message, 
     */
    public void rejectSubscribeMsgReceived(MZMessage msg){
        int sender = msg.getSender();
        ArrayList<Integer> neighbors = new ArrayList<Integer>();
        ArrayList<Integer> subedStripeList =  new ArrayList<Integer>();
        this.mzmMsgSrlzTool.deseralizeRejectSubscribe(msg.getValue(), neighbors, subedStripeList);
        
        for(int stripeId: subedStripeList) {
            this.mzSubscribeMan.addStripeSubscribeSource(stripeId, neighbors);
        }
        // when I receive a reject, that means I can not subscribe any stripe from that sender
        this.mzSubscribeMan.removeStripeSubscribeSource(sender);
        this.mzSubscribeMan.removeSubscribeMsg(sender, subedStripeList);
        logger.info("Node {} received a rejectSubscribeMsg from {}, I can subscribe stripe {} from {}", this.myId, msg.getSender(), subedStripeList, neighbors);
        
        // choose another sender and continue to subscribe this stripe
        for(int stripeId: subedStripeList) {
            subscribeStripe(stripeId);
        }
    }

    public void heartbeatMsgReceived(MZMessage msg) {
        long now = System.currentTimeMillis();
        // logger.info("heartbeatMsgReceived {} received a heartbeat from {} at {}", this.myId, msg.getSender(), now);
        this.heartbeatRecvTime.put(msg.getSender(), now);
    }


    public void latencyDetectMsgReceived(MZMessage msg) {
        if (addGossipedMsg(msg) == false)
            return;
        // logger.info("latencyDetectMsgReceived: received {}, routingTable: {}", msg.toString(), this.routingTable);
        ArrayList<Object> content = this.mzmMsgSrlzTool.deseraliceLatencyDetect(msg.getValue());
        ArrayList<Integer> nodeIdList = new ArrayList<>();
        ArrayList<Long> tsList = new ArrayList<>();

        // parse the message
        int i = 0;
        while (i < content.size()) {
            Integer nodeId = Integer.parseInt(content.get(i++).toString());
            nodeIdList.add(nodeId);
            Long ts = Long.parseLong(content.get(i++).toString());
            tsList.add(ts);
        }
        if (nodeIdList.contains(myId))
            return;

        // update routing table
        long latency = 0;
        for (i = tsList.size() - 1; i > 0; --i) {
            if (tsList.get(i) - tsList.get(i - 1) >= 0)
                latency += tsList.get(i) - tsList.get(i - 1);
            else {
                latency = -1;
                break;
            }
        }
        // a valid Latency detect msg.
        if (latency != -1) {
            if (this.routingTable.containsKey(msg.getMsgCreator()) == false)
                this.routingTable.put(msg.getMsgCreator(), new HashMap<>());
            if (this.routingTable.get(msg.getMsgCreator()).containsKey(msg.getSender()) == false)
                this.routingTable.get(msg.getMsgCreator()).put(msg.getSender(), latency + 1);
            long oldLatency = this.routingTable.get(msg.getMsgCreator()).get(msg.getSender());
            this.routingTable.get(msg.getMsgCreator()).put(msg.getSender(), Math.min(oldLatency, latency));
        }

        if (latency != -1) {
            ArrayList<Integer> neighbors = getNeighborNodes();
            neighbors.removeAll(nodeIdList);
            content.add(this.myId);
            content.add(System.currentTimeMillis());
            byte[] newContent = this.mzmMsgSrlzTool.seralizeLatencyDetect(content);
            msg.setValue(newContent);
            msg.setSender(this.myId);
            msg.setZoneId(this.myZoneId);
            // Don't set timestap for latency-detect msg !!!!
            // msg.setTimeStamp(System.currentTimeMillis());
            multicastMsg(msg, neighbors);
            // logger.info("latencyDetectMsgReceived: multicast {} to neighbors {}, routing table{} ", msg.toString(),
            //         neighbors, this.routingTable);
        }
    }

    /**
     * Receive a RELAYER message
     * 
     * @param msg
     */
    public void relayerAliveMsgReceived(MZMessage msg) {
        if (addGossipedMsg(msg) == false)
            return;
        HashSet<Integer> relayedStripes = this.mzmMsgSrlzTool.deseralizeRelayer(msg.getValue());
        int relayerId = msg.getMsgCreator();
        int relayerZone = this.controller.getStaticConf().getZoneId(relayerId);
        this.mzRelayerMan.addNewRelayer(relayerId, relayerZone, new ArrayList<Integer>(relayedStripes));
        logger.info("relayerAliveMsgReceived received msg: {}, stripes: {}", msg.toString(), relayedStripes);
        // a relayer should subscribe stripes from a relayer to recude consensus nodes'
        // bandwidth burden.
        // only process msg from a same zone
        if (this.isRelayer() && relayerZone == this.myZoneId) {
            HashSet<Integer> hs = this.mzRelayerMan.getRelayerStripe(this.myId);
            hs.retainAll(relayedStripes);
            if (this.myId < relayerId) {
                // a relayer can not subscribe all stripes from another relayer.
                if (hs.size() == this.mzRelayerMan.getRelayerStripe(this.myId).size()) {
                    for (int stripe : hs) {
                        hs.remove(stripe);
                        break;
                    }
                }
                sendSubscribeMsgTo(relayerId, hs);
            }
            else {
                // if that relayer relayed only one stripe and I also relay that stripe
                // and (the number of current relayers is more than N or I relayed more than one stripes)
                // I should subscribe msg from that node
                // this design can reduce the number of relayers to N.
                int nRelayers = this.mzRelayerMan.getRelayerNums(this.myZoneId);
                if (relayedStripes.size() == 1 && hs.isEmpty() == false ) {  
                    sendSubscribeMsgTo(relayerId, hs);
                }
                // if (relayedStripes.size() == 1 && hs.isEmpty() == false && 
                //     (nRelayers > this.controller.getCurrentViewN() || this.mzRelayerMan.getNumOfRelayedStripes(this.myId) > 1 )) {  
                //     sendSubscribeMsgTo(relayerId, hs);
                // }
            }

            // I should change stripe sender if a stripe sender do not relay my subscribed stripe
            // for example, 0, 1, 2, 3 are consensus nodes. I am node 5 and subscribe stripe 1 from node 4, 
            // but node 4 do not relay stripe 1 and now node 6 relays stripe 1.
            // In such a case, I should subscribe stripe 1 from node 6.
            // logger.info("Node {} check if I should change stripeSender", this.myId);
            HashSet<Integer> stripeCanSub = new HashSet<>();
            for (Integer stripeId: relayedStripes) {
                int currentSender = this.mzSubscribeMan.getStripeSender(stripeId);
                if (this.controller.isCurrentViewMember(currentSender) || this.mzSubscribeMan.isSendSubscribeMsgBefore(stripeId)) 
                    continue;
                if (currentSender == this.mzSubscribeMan.noSender || 
                    this.mzRelayerMan.isRelayer(currentSender) == false || 
                    this.mzRelayerMan.getRelayerStripe(currentSender).contains(stripeId) == false)
                    stripeCanSub.add(stripeId);
            } 
            sendSubscribeMsgTo(relayerId, stripeCanSub);
        }

        // record the sender and creator have send this msg to me.
        if (this.gossipedMsgSender.containsKey(msg) == false)
            this.gossipedMsgSender.put(msg, new HashSet<>());
        this.gossipedMsgSender.get(msg).add(msg.getSender());
        this.gossipedMsgSender.get(msg).add(msg.getMsgCreator());

        // forward to neighbors exclude msg creator and those neighbors that send that msg to me.
        // ArrayList<Integer> neighbors = getNeighborNodes();
        // neighbors.removeAll(this.gossipedMsgSender.get(msg));
        // only forward zone msg that belongs to myzone, only forward to my subscribers.
        if (relayerZone == this.myZoneId || this.isConsensusNode()) {
            ArrayList<Integer> neighbors = this.getZoneNeighbors(relayerZone);
            neighbors.removeAll(this.gossipedMsgSender.get(msg));
            msg.setSender(myId);
            msg.setZoneId(this.myZoneId);
            // Don't set timestap for relayer msg !!!!
            // msg.setTimeStamp(System.currentTimeMillis());
            
            multicastMsg(msg, neighbors);
        }
    }

    public void adjustFromReceivedRelayNodeMsgs() {
        long now = System.currentTimeMillis();
        int N = this.controller.getCurrentViewN();
        while (this.nRelayNodesMsgReceived.get() < N && (now - this.lastTimeAskRelayNode.get()) < 10000) {
            try {
                logger.info("waiting for TOMutil_RELAYER_NODE, current received {} replies", this.nRelayNodesMsgReceived.get());
                Thread.sleep(1000);
                now = System.currentTimeMillis();
            } catch (InterruptedException ie) {
                logger.error("Error in waitting for TOMutil_RELAYER_NODE: {}", ie);
                System.exit(1);
            } 
        }
        // if (this.nRelayNodesMsgReceived.get() == N
        //         || (now - this.lastTimeAskRelayNode.get()) > 10000 && this.nRelayNodesMsgReceived.get() > 0) {
        if (this.nRelayNodesMsgReceived.get() > 0) {
            // Todo to be a relayer or a ordinary node?
            logger.info("Node {} receives {} RELAY_NODES messages, N = {}", myId, this.nRelayNodesMsgReceived.get(), N);
            HashSet<Integer> hs = this.mzRelayerMan.getZoneRelayers(this.myZoneId);
            // if there is no relayer, I should subscribe stripes from all consensus nodes.
            if (hs == null || hs.isEmpty()) {
                logger.info("Zoneid = {}, no relayers", this.myZoneId);
                int[] consensusnodes = this.controller.getCurrentViewAcceptors();
                for (int nodeId : consensusnodes) {
                    Set<Integer> subStripes = new HashSet<>();
                    subStripes.add(nodeId);
                    sendSubscribeMsgTo(nodeId, subStripes);
                }
            }
            // There may be not enough relay node in my zone, I should be a relayer
            else {
                HashSet<Integer> stripeCansubscribe = this.mzSubscribeMan.getStripeCanSubscribe(0, N);
                logger.info("Zoneid = {}, relayers: {}, currentviewN = {} ", this.myZoneId, hs,
                        this.controller.getCurrentViewN());
                // if the number of relayers is less than N
                if (hs.size() < N) {
                    // find a relayer that relay most stripes.
                    int maxStripesnodeId = -1;
                    int maxStripes = 0;
                    HashMap<Integer, HashSet<Integer>> requestNodeStripes = new HashMap<>();
                    for (int relayerId : hs) {
                        HashSet<Integer> stripes = this.mzRelayerMan.getRelayerStripe(relayerId);
                        // find node that relayed most stripes
                        if (stripes.size() > maxStripes) {
                            maxStripes = stripes.size();
                            maxStripesnodeId = relayerId;
                        }
                        // request at least one stripe from one relayer.
                        for (int stripeId : stripes) {
                            if (stripeCansubscribe.contains(stripeId)) {
                                if (requestNodeStripes.containsKey(relayerId) == false)
                                    requestNodeStripes.put(relayerId, new HashSet<>());
                                requestNodeStripes.get(relayerId).add(stripeId);
                                stripeCansubscribe.remove(stripeId);
                                break;
                            }
                        }
                    }
                    HashSet<Integer> maxRelayedStripeSet = this.mzRelayerMan.getRelayerStripe(maxStripesnodeId);
                    logger.info("{} relayed most stripes {}", maxStripesnodeId, maxRelayedStripeSet);
                    for(Integer nodeId: requestNodeStripes.keySet()) 
                        maxRelayedStripeSet.removeAll(requestNodeStripes.get(nodeId));
                    logger.info("{} now relayed stripes {}", maxStripesnodeId, maxRelayedStripeSet);
                    // choose half stripes from nodes that relay most stripes
                    for (int stripe : maxRelayedStripeSet) {
                        if (stripeCansubscribe.contains(stripe) == false)
                            continue;
                        else if (requestNodeStripes.get(maxStripesnodeId).size() * 2 >= maxStripes)
                            break;
                        requestNodeStripes.get(maxStripesnodeId).add(stripe);
                        stripeCansubscribe.remove(stripe);
                    }

                    // subscribe requestNodeStripes stripes from relayers.
                    for (int relayerId : hs) {
                        HashSet<Integer> requestStripeSet = requestNodeStripes.get(relayerId);
                        if (requestStripeSet != null && requestStripeSet.isEmpty() == false)
                            sendSubscribeMsgTo(relayerId, requestStripeSet);
                    }
                    // subscribe remain stripes from consensus nodes.
                    for (int stripeId : stripeCansubscribe) {
                        Set<Integer> stripe = new HashSet<>();
                        stripe.add(stripeId);
                        sendSubscribeMsgTo(stripeId, stripe);
                    }
                }
                // I can subscribe data from enough relayers, subscribe one stripe from one
                // relayer
                else {
                    for (int nodeId : hs) {
                        HashSet<Integer> stripes = this.mzRelayerMan.getRelayerStripe(nodeId);
                        if (stripes == null) continue;
                        // just subscribe one stripe and break;
                        for (int stripeId : stripes) {
                            if (stripeCansubscribe.contains(stripeId) == false)
                                continue;
                            stripeCansubscribe.remove(stripeId);
                            Set<Integer> subStripes = new HashSet<>();
                            subStripes.add(stripeId);
                            sendSubscribeMsgTo(nodeId, subStripes);
                            break;
                        }
                    }
                }
            }
            
        }
        // adjust after 60 seconds
        this.nextTimeDetect.set(System.currentTimeMillis()+60000);
        
    }

    public void multicastMsg(SystemMessage msg, ArrayList<Integer> neighbors) {
        int[] targets = new int[neighbors.size()];
        for (int i = 0; i < neighbors.size(); ++i)
            targets[i] = neighbors.get(i);
        this.sendMsg(targets, msg);
    }
    /**
     * periodically broadcast RELAYER msg to neighbors.
     */
    public void boradcastRelayerAliveMsg() {
        long now = System.currentTimeMillis();
        // if (this.isRelayer() && now > this.nextTimeBroadcastRelayer.get()) {
        //     logger.info("Its time to multicast boradcastRelayerAliveMsg");
        // }
        HashSet<Integer> hs = this.mzRelayerMan.getRelayerStripe(this.myId);
        if (this.isRelayer() && now > this.nextTimeBroadcastRelayer.get()) {
            byte[] content = this.mzmMsgSrlzTool.seralizeRelayer(hs);
            MZMessage msg = this.createMZMessage(TOMUtil.RELAYERALIVE, content);
            // only broadcast to nodes in myzone and consensus nodes.
            ArrayList<Integer> neighbors = getNeighborNodes();
            ArrayList<Integer> tmpNeighbors = new ArrayList<>();
            for(int nodeId: neighbors) {
                int tmpZoneId = this.controller.getStaticConf().getZoneId(nodeId);
                if (this.controller.isCurrentViewMember(nodeId) || tmpZoneId == this.myZoneId)
                    tmpNeighbors.add(nodeId);
            }
            this.multicastMsg(msg, tmpNeighbors);
            // broadcast every one minute.
            this.nextTimeBroadcastRelayer.set(now + 60000);
            logger.info("boradcastRelayerAliveMsg: Multicast [{}] to neighbors {}, stripes I relayed: {}", msg.toString(),
                     neighbors, this.mzRelayerMan.getRelayerStripe(myId));
            if (hs.isEmpty()){
                this.setRelayer(false);
                this.mzRelayerMan.removeRelayer(this.myId, this.myZoneId);
            }
        }
    }

    /**
     * periodically broadcast LATENCY_DETECT msg to neighbors.
     */
    public void broadcastLatencyDetectMsg() {
        if (isConsensusNode() == false) return;
        long now = System.currentTimeMillis();
        if (now < this.nextTimeBroadcastLatencyDetect.get())
            return;
        ArrayList<Object> nodeIdTimeStampList = new ArrayList<>();
        nodeIdTimeStampList.add(myId);
        nodeIdTimeStampList.add(System.currentTimeMillis());
        byte[] content = this.mzmMsgSrlzTool.seralizeLatencyDetect(nodeIdTimeStampList);
        MZMessage msg = createMZMessage(TOMUtil.LATENCY_DETECT, content);
        ArrayList<Integer> neighbors = new ArrayList<>();
        for(Integer nodeId: subscriberMap.keySet()) {
            if (this.subscriberMap.get(nodeId) != null)
                neighbors.add(nodeId);
        }
        multicastMsg(msg, neighbors);
        nextTimeBroadcastLatencyDetect.set(now + 120000);
        logger.info("Multicast [{}] to neighbors {}, content: {}", msg.toString(), neighbors, nodeIdTimeStampList);
    }

    /**
     * Node periodically broadcast heartbeat message to neighbors.
     */
    public void broadcastHeartbeatMsg(){
        if (this.controller.isInCurrentView())
            return;
        long now = System.currentTimeMillis();
        ArrayList<Integer> neighbors = this.getNeighborNodes();
        ArrayList<Integer> candidates = new ArrayList<>();
        for(int node: neighbors) {
            if (this.nextTimeBroadcastHeartbeat.containsKey(node) == false || 
                now >= this.nextTimeBroadcastHeartbeat.get(node)) {
                candidates.add(node);
            }
        }
        MZMessage msg = createMZMessage(TOMUtil.CM_HEARTBEAT, null);
        multicastMsg(msg, candidates);
    }

    public void subscribeStripe(int stripe) {
        if (this.mzSubscribeMan.isSendSubscribeMsgBefore(stripe))
            return;
        ArrayList<Integer> downloadSourceList = this.mzSubscribeMan.getStripeSubscribeSource(stripe);
        // try to find relayers to subscribe data.
        if (downloadSourceList.size() == 1 && downloadSourceList.get(0) == stripe) {
            Set<Integer> relayers = this.mzRelayerMan.getRelayerWhoRelay(stripe, this.myZoneId);
            Set<Integer> rejectNodeSet = this.mzSubscribeMan.getRejectNodeSet();
            relayers.removeAll(rejectNodeSet);
            if (relayers.isEmpty() == false) {
                downloadSourceList.clear();
                downloadSourceList.addAll(relayers);
            }
        }
        HashSet<Integer> senderSet = this.mzSubscribeMan.getSenderSet();
        // try to subscribe from different nodes
        if (downloadSourceList.size() > senderSet.size())
            downloadSourceList.removeAll(senderSet);
        Collections.shuffle(downloadSourceList);
        HashSet<Integer> subedStripeSet = new HashSet<>();
        subedStripeSet.add(stripe);
        this.sendSubscribeMsgTo(downloadSourceList.get(0), subedStripeSet);
    }

    public boolean processStripeWithNoSender(){
        int nc = this.controller.getCurrentViewN();
        HashSet<Integer> noSenderStripeSet = this.mzSubscribeMan.getStripeCanSubscribe(0, nc);
        boolean haveEnoughSender = true;
        for(Integer stripe: noSenderStripeSet) {
            logger.info("{} find stripe {} have no sender, stripeSender:{}, subscribeMap: {}", this.myId, 
            stripe,this.mzSubscribeMan.stripeSenderMapToString(), this.mzSubscribeMan.subscribeMapToString());
            subscribeStripe(stripe);
            haveEnoughSender = false;
        }
        return haveEnoughSender;
    }

    /**
     * A node peirodically connect to other node and connect to them
     */
    public void connectNodeForRobust(){
        // connect with enough nodes to keep robust.
        ArrayList<Integer> neighbors = this.getNeighborNodesWithoutConsensusNodes();
        int quorumCon = this.controller.getStaticConf().getNRandomNeighbor();
        if (neighbors.size() < quorumCon) {
            ArrayList<Integer> candidateNeighbors = this.cs.getServersConn().getNeighborZoneNodes();
            candidateNeighbors.removeAll(neighbors);
            candidateNeighbors.removeAll(this.subscriberMap.keySet());
            Collections.shuffle(candidateNeighbors);
            int i = 0;
            while (neighbors.size() < quorumCon && i < candidateNeighbors.size()) {
                int nodeId = candidateNeighbors.get(i++);
                this.cs.getServersConn().getConnection(nodeId);
                logger.info("{} connect with {} as backup connection",this.myId, nodeId);
            }
        }
    }

    /**
     * A node will disconnect with overdue node
     */
    public HashSet<Integer> disconnectOverdueNode() {
        // if a nodes does not send heartbeat message for a long time, we think it may be off-line
        long now = System.currentTimeMillis();
        int interval = this.controller.getStaticConf().getHeartbeatInterval();
        ArrayList<Integer> neighbors = this.getNeighborNodesWithoutConsensusNodes();
        HashSet<Integer> eraseSet = new HashSet<>();
        Random r = new Random();
        double multi = 1.5 + r.nextDouble();
        for(int node: neighbors) {
            long diff = this.heartbeatRecvTime.containsKey(node) ? (now - this.heartbeatRecvTime.get(node)): 0;
            if ( diff > multi * interval) {
                int tmpZoneId = this.controller.getStaticConf().getZoneId(node);
                if (tmpZoneId == this.myZoneId) {
                    logger.info("{} is a relayer in zone {}, it is offline", node, tmpZoneId);
                }
                // remove this node as a subscribe or subscriber
                this.mzSubscribeMan.nodeOffline(node);
                this.subscriberMap.remove(node);
                // put node into eraseSet
                eraseSet.add(node);
                // close connection
                cs.getServersConn().closeConnection(node);
                this.heartbeatRecvTime.remove(node);

                // consensus node remove that relayer as it only 
                // if (this.isConsensusNode()) {
                int relayerZone = this.controller.getStaticConf().getZoneId(node);
                this.mzRelayerMan.removeRelayer(node, relayerZone);
                // }
                    
                logger.info("{} disconnect with {} afer {} ms, stripeSender:{}, subscribeMap{}, its relayerinfo {}", 
                    this.myId, node, diff, this.mzSubscribeMan.stripeSenderMapToString(), this.mzSubscribeMan.subscribeMapToString(), this.mzRelayerMan.getRelayerStripe(node));

                }
        }

        // /**
        //  * if some stripes have no relayers, I should be a relayer
        //  */
        // if (false == this.isConsensusNode()) {
        //     for(Integer relayerNode: eraseSet) {
        //         if (this.mzRelayerMan.isRelayer(relayerNode) == false)
        //             continue;
        //         HashSet<Integer> relayerStripeSet = this.mzRelayerMan.getRelayerStripe(relayerNode);
        //         int relayerZone = this.controller.getStaticConf().getZoneId(relayerNode);
        //         this.mzRelayerMan.removeRelayer(relayerNode, relayerZone);
        //         if (relayerZone == this.myZoneId) {
        //             for(int stripe: relayerStripeSet) {
        //                 // if I just subscribe this stripe before ,skip this
        //                 if (this.mzSubscribeMan.shouldISubscribe(stripe) == false) {
        //                     logger.info("{} will not forward {}, its sender is {}", this.myId, stripe,  this.mzSubscribeMan.getStripeSender(stripe));
        //                     continue;
        //                 }
                            
        //                 HashSet<Integer> hs = new HashSet<>();
        //                 hs.add(stripe);
        //                 sendSubscribeMsgTo(stripe, hs);
        //                 logger.info("{} find {} is a relayer and it is off-line, prepare to be a relayer and forward {}", this.myId, relayerNode, hs);
        //             }
        //         }
        //     }
        // }
        return eraseSet;
    }

    /**
     * An ordinay node will always check if there are enough relayers
     * if there is more than N relayers, it will become a new relayers
     */
    public void checkAndBecomeRelayer(){
        // If some stripes have no relayers, I should subscribe them.
        int nc = this.controller.getCurrentViewN();
        HashSet<Integer> noRelayerStripeSet = this.mzRelayerMan.findStripeWithNoRelayer(nc);
        HashSet<Integer> subedStripeSet = new HashSet<>();
        for(Integer stripe: noRelayerStripeSet) {
            if (this.mzSubscribeMan.shouldISubscribe(stripe)){
                subedStripeSet.add(stripe);
                sendSubscribeMsgTo(stripe, new HashSet<Integer>(stripe));
                logger.info("{} find stripe {} have no relayers and prepare to become a relayer", this.myId, stripe);
            }
            else {
                logger.info("stripe {} have a relayer or a sender, I will not become a relayer, my sender: {}", stripe, this.mzSubscribeMan.getStripeSender(stripe));
            }
        } 

        if (this.isOrdinary() == false)
            return;
        
        HashSet<Integer> myZoneRelayers = this.mzRelayerMan.getZoneRelayers(this.myZoneId);
        logger.info("{} is an ordinary node and find there are {} relayers", this.myId, myZoneRelayers.size());
        if (myZoneRelayers.size() < nc) {
            
            int stripeIshouldRelay = myZoneRelayers.size() == 0 ? nc : nc / myZoneRelayers.size();
            for(int relayer: myZoneRelayers) {
                HashSet<Integer> stripeSet = this.mzRelayerMan.getRelayerStripe(relayer);
                if(stripeSet.size() > 1) {
                    int tmpStripe = stripeSet.iterator().next();
                    if (subedStripeSet.add(tmpStripe)){
                        this.sendSubscribeMsgTo(tmpStripe, new HashSet<Integer>(tmpStripe));
                        logger.info("{} find there are {} relayers, prepare to be a relayer and forward {}", this.myId, myZoneRelayers.size(), tmpStripe);
                    }
                }
                if (subedStripeSet.size() >= stripeIshouldRelay)
                    break;
            }    
        }
    }

    /**
     * periodically detect other nodes in my zone, neighbor zone and other zones.
     * Todo
     */
    public void detectNodes() {
        long now = System.currentTimeMillis();
        if (now < nextTimeDetect.get()) 
            return;
            
        disconnectOverdueNode();
        int basicWait = 25000;
        if (this.isConsensusNode() == false) {
            
            checkAndBecomeRelayer();

            processStripeWithNoSender();
            
            connectNodeForRobust();

        }
        long finish = System.currentTimeMillis();
        Random r = new Random();
        this.nextTimeDetect.set(System.currentTimeMillis() + r.nextInt(basicWait) + 5000); 
        if (finish-now > 500)
            logger.info("{} detect nodes use {} ms", this.myId, finish-now);
    }

    public final void deliver(MZMessage msg) {
        // abandon too old messages or too new messages
        long now = System.currentTimeMillis();
        if (now - msg.getTimeStamp() > 120000 || msg.getTimeStamp() - now > 30000)
            return;
        if (msg.getType() == TOMUtil.GET_RELAY_NODE) {
            getRelayMsgReceived(msg);
        } else if (msg.getType() == TOMUtil.RELAY_NODES) {
            relayNodesMsgReceived(msg);
        } else if (msg.getType() == TOMUtil.SUBSCRIBE) {
            subscribeMsgReceived(msg);
        } else if (msg.getType() == TOMUtil.UNSUBSCRIBE) {
            unsubscribeMsgReceived(msg);
        } else if (msg.getType() == TOMUtil.ACCEPT_SUBSCRIBE) {
            acceptSubscribeMsgReceived(msg);
        } else if (msg.getType() == TOMUtil.RELAYERALIVE) {
            relayerAliveMsgReceived(msg);
        } else if (msg.getType() == TOMUtil.LATENCY_DETECT) {
            latencyDetectMsgReceived(msg);
        } else if (msg.getType() == TOMUtil.REJECT_SUBSCRIBE) {
            rejectSubscribeMsgReceived(msg);
        }
        // else if (msg.getType() != TOMUtil.CM_HEARTBEAT) {
        //     logger.error("received a message: --message type: " + msg.getVerboseType()
        //             + " but is regard as out of context, from " + msg.getSender());
        // }
        heartbeatMsgReceived(msg);
    }

    /**
     * sending GET_RELAY_NODE to consensus nodes
     * and collect current relay_node info.
     */
    public void askRelayNodes() {
        int[] initialV = this.controller.getCurrentViewAcceptors();
        ArrayList<Integer> consensusNodes = new ArrayList<>();
        for(int i = 0; i < initialV.length; ++i)    consensusNodes.add(i);
        boolean asked = false;
        while (!asked) {
            ArrayList<Integer> neighbors = this.getNeighborNodes();
            if (neighbors.containsAll(consensusNodes) == false) {
                try {
                    Thread.sleep(1000);
                    continue;
                } catch (InterruptedException ie) {
                    LoggerFactory.getLogger(this.getClass()).error("Interruption during sleep", ie);
                    System.exit(1);
                }
            }
            long now = System.currentTimeMillis();
            MZMessage msg = this.createMZMessage(TOMUtil.GET_RELAY_NODE, null);
            logger.info("Node {} multicast [{}] to consensus node {}", myId, msg.toString(), initialV);
            this.nRelayNodesMsgReceived.set(0);
            this.lastTimeAskRelayNode.set(now);
            multicastMsg(msg, consensusNodes);
            asked = true;
        }
    }

    public void sendMsg(int[] targets, SystemMessage sm) {
        this.cs.send(targets, sm);
        long now = System.currentTimeMillis();
        int interval = this.controller.getStaticConf().getHeartbeatInterval();
        for(int node: targets) {
            this.nextTimeBroadcastHeartbeat.put(node, now+interval);
        }
    }
}
