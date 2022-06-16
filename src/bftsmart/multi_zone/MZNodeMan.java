package bftsmart.multi_zone;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.communication.SystemMessage;
import bftsmart.tom.util.TOMUtil;
import bftsmart.multi_zone.DataHashMessage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ArrayList;
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
    private Map<Integer, HashSet<Integer>> subscribeMap;        // nodeId --> stripeId I subscribed
    private Map<Integer, Integer> stripeSenderMap;              // stripeId --> nodeId send to me
    private Map<Integer, HashSet<Integer>> zoneRelayerMap;
    private Map<Integer, HashSet<Integer>> relayerStripeMap;    // relayerId-> stripe it relayed.
    private Map<Integer, HashSet<Integer>> subscribeMsgSentMap; // subscribe msg I send to other nodes but not received replies.
    private boolean isRelayer = false;
    private boolean isConsensusNode = false;
    private MZMessageSeralizeTool mzmMsgSrlzTool;

    // to process LATENCY_DETECT and RELAYER msgs.
    private LinkedList<MZMessage> gossipedMsg; // msg(LATENCY-DETECT, RELAYER) that I received and broadcasted.
    private Map<MZMessage, HashSet<Integer>> gossipedMsgSender; // msg(LATENCY-DETECT, RELAYER) they send to me.
    private MZMessage lastReceivedRelayerMsg;
    private Map<Integer, Map<Integer, Long>> routingTable; // Map<consensusNode, <nextJumpNode, latency>>

    // stripe, block, mzbatch that need to forward.
    private Queue<SystemMessage> dataQueue ;
    private Map<String, SystemMessage> candidateBlockMap;

    // ordinary block I send and received 
    private Map<SystemMessage, Set<Integer>>    blockSendMap;
    private Map<SystemMessage, Set<Integer>>    blockRecvMap;
    private Map<SystemMessage, Integer>         msgSendCntMap;

    public Map<String, MZBlock> blockchain;

    private int myId;
    private int zoneId;
    private boolean adjust;

    // for asking relay_node info
    private AtomicLong lastTimeAskRelayNode = new AtomicLong();
    private AtomicInteger nRelayNodesMsgReceived = new AtomicInteger();

    // last time broadcast relayer
    private AtomicLong nextTimeBroadcastRelayer = new AtomicLong();
    private AtomicLong nextTimeBroadcastLatencyDetect = new AtomicLong();

    public MZNodeMan(ServerViewController controller, ServerCommunicationSystem cs) {
        this.controller = controller;

        this.subscriberMap = new ConcurrentHashMap<>();
        this.zoneRelayerMap = new ConcurrentHashMap<>();
        this.subscribeMap = new ConcurrentHashMap<>();
        this.relayerStripeMap = new ConcurrentHashMap<>();
        this.subscribeMsgSentMap = new ConcurrentHashMap<>();
        this.stripeSenderMap = new ConcurrentHashMap<>();
        this.routingTable = new ConcurrentHashMap<>();

        this.gossipedMsg = new LinkedList<>();
        this.gossipedMsgSender = new ConcurrentHashMap<>();
        this.lastReceivedRelayerMsg = null;

        // for send data to subscriber node.
        this.dataQueue = new ConcurrentLinkedQueue<SystemMessage>();
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

        this.myId = this.controller.getStaticConf().getProcessId();
        this.zoneId = this.controller.getStaticConf().getZoneId(myId);

        this.blockRecvMap = new ConcurrentHashMap<>();
        this.blockSendMap = new ConcurrentHashMap<>();
        this.msgSendCntMap = new ConcurrentHashMap<>();

        adjust = true;
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
        MZMessage replyMsg = new MZMessage(myId, type, ts, zoneId, content);
        return replyMsg;
    }

    public void addForwardData(SystemMessage msg) {
        assert(msg != null);
        this.dataQueue.add(msg);
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
        logger.info("addMZCandidateBlock:Node {} add msg [{}] to candidateblock [{}]", myId, hash, candidateBlock.toString());
    }

    public void storeCandidateBlock(byte[] blockHash, ConsensusMessage msg) {
        String hash = MZNodeMan.bytesToHex(blockHash, 16);
        this.candidateBlockMap.put(hash, msg);
        logger.info("storeCandidateBlock: Node {} add msg: [{}] to candidateblock, blockheight: [{}]", myId, hash, msg.getNumber());
    }

    public void ForwardDataToSubscriber() {
        if (this.dataQueue.isEmpty()) 
            return;
        logger.info("dataQueue size: {}, consensus nodes: {}, subscriberMap: {}", dataQueue.size(), this.controller.getCurrentViewAcceptors(),this.subscriberMap);
        int networkmode = this.controller.getStaticConf().getNetworkingMode();
        int fabricTTL = this.controller.getStaticConf().getFabricTTL();
        int fabricFout = this.controller.getStaticConf().getFabricFout();
        int fabricTTLDirect = this.controller.getStaticConf().getFabricTTLDirect();
        while (this.dataQueue.isEmpty() == false) {
            SystemMessage msg = this.dataQueue.poll();
            msg.setSender(this.myId);
            if (networkmode == TOMUtil.NM_STAR) {
                // only consensus nodes forward data to other nodes in a star topology.
                if (this.controller.isInCurrentView() == false)
                    continue;
                ArrayList<Integer> neighbors = getNeighborNodesWithoutConsensusNodes();
                multicastMsg(msg, neighbors);
                logger.info("ForwardDataToSubscriber Node {} broadcast [{}] to neighbors: {}", this.myId, msg.toString(), neighbors );
            }
            else if (networkmode == TOMUtil.NM_MULTI_ZONE){
                for(Map.Entry<Integer, HashSet<Integer>> entry: this.subscriberMap.entrySet()) {
                    int nodeId = entry.getKey();
                    int[] target = {nodeId};
                    if (msg instanceof  ConsensusMessage) {
                        ConsensusMessage conMsg = (ConsensusMessage)(msg);
                        logger.info("ForwardDataToSubscriber Node {} broadcast {} to {}", this.myId, msg.toString(), target);
                        cs.send(target, conMsg);
                    } else if (msg instanceof MZBlock) {
                        MZBlock block = (MZBlock)(msg);
                        // seralize block content
                        Mz_Propose propose = block.getPropose();
                        logger.info("ForwardDataToSubscriber Node {} boradcast block {} to {}, propose {}, batch: {}",this.myId, bytesToHex(block.getBlockHash(),8), target, propose.toString(), block.getBlockContent().length);
                        cs.send(target, block);
                    } else if (msg instanceof MZStripeMessage){
                        MZStripeMessage stripe = (MZStripeMessage)(msg);
                        HashSet<Integer> stripeSet = entry.getValue();
                        if (stripeSet == null || stripeSet.contains(stripe.getStripeId())) {
                            logger.info("ForwardDataToSubscriber Node {} broadcast stripe {} to subscriber {}", this.myId, msg.toString(), target);
                            cs.send(target, stripe);
                        } 
                    }
                }
            }
            else if (networkmode == TOMUtil.NM_RANDOM) {
                int ds = this.controller.getStaticConf().getDataDisForRandom();
                if (ds == TOMUtil.DS_RANDOM_ENHANCED_FAB) {
                    ArrayList<Integer> neighbors = this.getNeighborNodesWithoutConsensusNodes();
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
                        logger.info("ForwardDataToSubscriber Node {} broadcast [{}] to subscriber {}", this.myId, msg , foutReceivers);
                    } 
                    // send block digest.
                    else {
                        long now = System.currentTimeMillis();
                        int dataType = -1;
                        byte[] dataHash = null;
                        int[] appendix = null;
                        DataHashMessage datahashMsg = null;
                        if (msg instanceof ConsensusMessage ){
                            ConsensusMessage consMsg = (ConsensusMessage)(msg);
                            dataType = TOMUtil.DH_BLODKHASH;
                            dataHash = consMsg.getHash();
                            appendix = new int[]{consMsg.getNumber()};
                            datahashMsg = new DataHashMessage(this.myId, dataType, now, appendix, dataHash);
                        }
                        else if (msg instanceof MZStripeMessage) {
                            MZStripeMessage stripeMsg = (MZStripeMessage)(msg);
                            dataType = TOMUtil.DH_STRIPEHASH;
                            dataHash = null;
                            appendix = new int[3];
                            appendix[0] = stripeMsg.getBatchChainId();
                            appendix[1] = stripeMsg.getHeight();
                            appendix[2] = stripeMsg.getStripeId();
                            datahashMsg = new DataHashMessage(this.myId, dataType, now, appendix, dataHash);
                        }
                        multicastMsg(datahashMsg, foutReceivers);
                        logger.info("ForwardDataToSubscriber Node {} broadcast datahash [{}] to subscriber {}", this.myId, datahashMsg.toString(), foutReceivers);
                    }
                    ++sendCnt;
                    this.msgSendCntMap.put(msg, sendCnt);
                }
            }
            // broadcast data hash to other nodes.
            // Todo
        }
    }

    public ArrayList<Integer> getNeighborNodes() {
        return cs.getServersConn().getNeighborNodes();
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

    public void setRelayer(boolean state) {
        isRelayer = state;
    }

    public boolean isConsensusNode() {
        return isConsensusNode;
    }

    public void setConsensusNode(boolean state) {
        isConsensusNode = state;
    }

    public HashSet<Integer> getStripeCanSubscribe() {
        HashSet<Integer> hs = new HashSet<>();
        for (int i = 0; i < this.controller.getCurrentViewN(); ++i)
            hs.add(i);
        if (this.stripeSenderMap.keySet() != null)
            hs.removeAll(this.stripeSenderMap.keySet());
        return hs;
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

    // public void unscribe(int subscriberId, int stripeId) {
    //     if (subscriberMap.containsKey(subscriberId) == false)
    //         return;
    //     HashSet<Integer> hs = subscriberMap.get(subscriberId);
    //     hs.remove(stripeId);
    // }

    public void sendSubscribeMsgTo(int target, Set<Integer> subStripes) {
        if (subStripes == null || subStripes.isEmpty())
            return;
        byte[] content = this.mzmMsgSrlzTool.seralizeSubscribe(subStripes);
        MZMessage subscribeMsg = createMZMessage(TOMUtil.SUBSCRIBE, content);
        int[] targets = { target };
        cs.send(targets, subscribeMsg);
        this.subscribeMsgSentMap.put(target, new HashSet<Integer>(subStripes));
        logger.info("sendSubscribeMsgTo Send [{}] to {}, subscripes: {}", subscribeMsg, target, this.subscribeMsgSentMap.get(target));
        this.nextTimeBroadcastRelayer.set(System.currentTimeMillis() + 5000);
    }

    public void sendUnSubscribedMsgTo(int target, Set<Integer> stripes) {
        byte[] content = this.mzmMsgSrlzTool.seralizeSubscribe(stripes);
        MZMessage unsubscribeMsg = createMZMessage(TOMUtil.UNSUBSCRIBE, content);
        int[] targets = { target };
        cs.send(targets, unsubscribeMsg);
        // if I am a relayer node, update relayerStripeMap.
        if (isRelayer() && this.controller.isCurrentViewMember(target) && this.relayerStripeMap.containsKey(myId)) {
            this.relayerStripeMap.get(myId).removeAll(stripes);
            updateRelayer(myId, zoneId, new ArrayList<>(this.relayerStripeMap.get(myId)));
        }
        // update subscribeMap
        if(this.subscribeMap.containsKey(target)) {
            this.subscribeMap.get(target).removeAll(stripes);
            if (this.subscribeMap.get(target).isEmpty())
                this.subscribeMap.remove(target);
        }
            
        logger.info("sendUnSubscribedMsgTo Send [{}] to {}, relayStripeMap: {}", unsubscribeMsg, target, this.relayerStripeMap.get(myId));
    }

    /**
     * received a GET_RELAY_NODE msg
     * 
     * @param msg
     */
    public void getRelayMsgReceived(MZMessage msg) {
        assert (msg != null);
        HashSet<Integer> relayerSet = zoneRelayerMap.get(msg.getZoneId());
        byte[] content = this.mzmMsgSrlzTool.seralizeRelayNodes(relayerSet, this.relayerStripeMap);
        MZMessage replyMsg = createMZMessage(TOMUtil.RELAY_NODES, content);
        int target[] = { msg.getSender() };
        this.cs.send(target, replyMsg);
        logger.info("getRelayMsgReceived: received msg: [{}], relayerStripeMap: {}", msg.toString(),
                this.relayerStripeMap);
    }

    public void updateRelayer(int relayerId, int relayerZone, ArrayList<Integer> relayerStripes) {
        // assert (null != relayerStripes) : "updateRelayer: relayerStripes is empty!!";
        if (this.zoneRelayerMap.containsKey(relayerZone) == false)
            this.zoneRelayerMap.put(relayerZone, new HashSet<>());
        // add to relayerStripeMap
        if (this.relayerStripeMap.containsKey(relayerId) == false)
            this.relayerStripeMap.put(relayerId, new HashSet<>());
        // clear
        if (this.relayerStripeMap.containsKey(relayerId))
            this.relayerStripeMap.get(relayerId).clear();
        if (relayerStripes.isEmpty() == false)
            addRelayer(relayerId, relayerZone, relayerStripes);
        else {
            // an empty relayerStripes means that node do not be a relayer anymore.
            if (zoneRelayerMap.containsKey(relayerZone))
                this.zoneRelayerMap.get(relayerZone).remove(relayerId);
            this.relayerStripeMap.remove(relayerId);
            logger.info("updateRelayer Node {} receive an empty relayerMsg for zone {}, this.zoneRelayerMap: {}", this.myId, relayerZone, this.zoneRelayerMap.get(relayerId));
        }
    }

    public void addRelayer(int relayerId, int relayerZone, ArrayList<Integer> relayerStripes) {
        // assert (null != relayerStripes) : "addRelayer: relayerStripes is empty!!";
        // add to zoneRelayerMap
        if (this.zoneRelayerMap.containsKey(relayerZone) == false)
            this.zoneRelayerMap.put(relayerZone, new HashSet<>());
        this.zoneRelayerMap.get(relayerZone).add(relayerId);

        // add to relayerStripeMap
        if (this.relayerStripeMap.containsKey(relayerId) == false)
            this.relayerStripeMap.put(relayerId, new HashSet<>());
        this.relayerStripeMap.get(relayerId).addAll(relayerStripes);
    }

    /**
     * received a RELAY_NODES msg
     * 
     * @param msg
     */
    public void relayNodesMsgReceived(MZMessage msg) {
        // skip a old relayer msg
        if (this.lastReceivedRelayerMsg != null &&
                this.lastReceivedRelayerMsg.getTimeStamp() > msg.getTimeStamp())
            return;
        nRelayNodesMsgReceived.incrementAndGet();
        HashMap<Integer, HashSet<Integer>> tmpRelayStripeMap = this.mzmMsgSrlzTool.deseralizeRelayNodes(msg.getValue());
        logger.info("relayNodesMsgReceived received msg: [{}], zoneRelayerMap: {}, relayerStripeMap:{}", msg.toString(),
                this.zoneRelayerMap, this.relayerStripeMap);
        for (HashMap.Entry<Integer, HashSet<Integer>> entry : tmpRelayStripeMap.entrySet()) {
            int relayerId = entry.getKey();
            int relayerZone = this.controller.getStaticConf().getZoneId(relayerId);
            updateRelayer(relayerId, relayerZone, new ArrayList<>(entry.getValue()));
        }
        this.lastReceivedRelayerMsg = msg;
    }

    public void subscribeMsgReceived(MZMessage msg) {
        // a relayer node only receives 32 connections
        if (this.isRelayer() && this.subscriberMap.size() >= 32) {
            msg.setType(TOMUtil.GET_RELAY_NODE);
            getRelayMsgReceived(msg);
        } else {

            if (this.subscriberMap.get(msg.getSender()) == null)
                this.subscriberMap.put(msg.getSender(), new HashSet<>());
            ArrayList<Integer> subStripes = this.mzmMsgSrlzTool.deseralizeSubscribe(msg.getValue());
            addSubscriber(msg.getSender(), subStripes);

            // if I am a leader, mark that node as a relayer.
            if (this.controller.isInCurrentView()) {
                addRelayer(msg.getSender(), msg.getZoneId(), subStripes);
            }

            MZMessage replyMsg = createMZMessage(TOMUtil.ACCEPT_SUBSCRIBE, null);
            int target[] = { msg.getSender() };
            cs.send(target, replyMsg);

        }
        logger.info("subscribeMsgReceived received msg: [{}], zoneRelayerMap: {}, relayerStripeMap:{}, subscriberMap: {}", msg.toString(),
                this.zoneRelayerMap, this.relayerStripeMap, this.subscriberMap);

    }

    public void unsubscribeMsgReceived(MZMessage msg) {
        if (this.subscriberMap.containsKey(msg.getSender()) == false)
            return;
        ArrayList<Integer> unsubStripes = this.mzmMsgSrlzTool.deseralizeSubscribe(msg.getValue());
        if (this.subscriberMap.containsKey(msg.getSender()))
            this.subscriberMap.get(msg.getSender()).removeAll(unsubStripes);
        if (this.subscriberMap.get(msg.getSender()).isEmpty())
            this.subscriberMap.remove(msg.getSender());

        // adjust node's relayer info
        if (this.controller.isInCurrentView() && this.relayerStripeMap.containsKey(msg.getSender())) {
            this.relayerStripeMap.get(msg.getSender()).removeAll(unsubStripes);
            // this node will be a non-relayer if its relayed stripes are empty
            if (this.relayerStripeMap.get(msg.getSender()).isEmpty()) {
                this.relayerStripeMap.remove(msg.getSender());
                if (this.zoneRelayerMap.containsKey(msg.getZoneId()))
                    this.zoneRelayerMap.get(msg.getZoneId()).remove(msg.getSender());
            }
        }
        logger.info("unsubscribeMsgReceived receive [{}] , relayerStripeMap: {}, sender's subscriberMap: {}", msg,
            this.relayerStripeMap.get(myId), this.subscriberMap.get(msg.getSender()));
    }

    public void acceptSubscribeMsgReceived(MZMessage msg) {
        HashSet<Integer> hs = this.subscribeMsgSentMap.get(msg.getSender());
        assert(hs != null):"hs is empty!!";
        
        // if sender is a consensus node, then, mark myself is a new relayer
        if (this.controller.isCurrentViewMember(msg.getSender())) {
            this.setRelayer(true);
            addRelayer(this.myId, this.zoneId, new ArrayList<>(hs));
        }

        // send unsubscribe msg to previous sender
        HashMap<Integer, Set<Integer>> unsubscribeMap = new HashMap<>();
        // update my subscribeMap
        if (this.subscribeMap.get(msg.getSender()) == null)
            this.subscribeMap.put(msg.getSender(), new HashSet<Integer>());
        if (hs != null) {
            for (int stripeId : hs) {
                Integer previousSender = this.stripeSenderMap.get(stripeId);
                if (previousSender != null && previousSender.intValue() != msg.getSender()) {
                    if (unsubscribeMap.containsKey(previousSender.intValue()) == false)
                        unsubscribeMap.put(previousSender.intValue(), new HashSet<Integer>());
                    unsubscribeMap.get(previousSender.intValue()).add(stripeId);

                }
                // change stripe sender
                this.stripeSenderMap.put(stripeId, msg.getSender());
                this.subscribeMap.get(msg.getSender()).add(stripeId);
            }
        }
        for (HashMap.Entry<Integer, Set<Integer>> entry : unsubscribeMap.entrySet()) 
            sendUnSubscribedMsgTo(entry.getKey(), entry.getValue());

        // remove sent subscribe history
        this.subscribeMsgSentMap.remove(msg.getSender());
        logger.info("acceptSubscribeMsgReceived received msg: {}, subscribeMap: {}, stripeSender: {}", msg.toString(), this.subscribeMap, this.stripeSenderMap);
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
            content.add(myId);
            content.add(System.currentTimeMillis());
            byte[] newContent = this.mzmMsgSrlzTool.seralizeLatencyDetect(content);
            msg.setValue(newContent);
            msg.setSender(myId);
            msg.setZoneId(zoneId);
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
    public void relayerMsgReceived(MZMessage msg) {
        if (addGossipedMsg(msg) == false)
            return;
        HashSet<Integer> relayedStripes = this.mzmMsgSrlzTool.deseralizeRelayer(msg.getValue());
        logger.info("relayerMsgReceived received msg: {}, stripes: {}", msg.toString(), relayedStripes);
        int relayerId = msg.getMsgCreator();
        int relayerZone = this.controller.getStaticConf().getZoneId(relayerId);
        // a relayer should subscribe stripes from a relayer to recude consensus nodes'
        // bandwidth burden.
        // only process msg from a same zone
        if (this.isRelayer() && relayerZone == this.zoneId) {
            HashSet<Integer> hs = new HashSet<>();
            hs.addAll(this.relayerStripeMap.get(myId));
            hs.retainAll(relayedStripes);
            if (this.myId < relayerId) {
                // a relayer can not subscribe all stripes from another relayer.
                if (hs.size() == this.relayerStripeMap.get(myId).size()) {
                    for (int stripe : hs) {
                        hs.remove(stripe);
                        break;
                    }
                }
                if (hs.isEmpty() == false) {
                    sendSubscribeMsgTo(relayerId, hs);
                }
            }
            else {
                // if that relayer relayed only one stripe and I also relay that stripe
                // and the number of current relayers is more than N
                // I should subscribe msg from that node
                if (this.zoneRelayerMap.containsKey(this.zoneId)){
                    int nRelayers = this.zoneRelayerMap.get(this.zoneId).size();
                    if (relayedStripes.size() == 1 && hs.isEmpty() == false && nRelayers > this.controller.getCurrentViewN()) {  
                        sendSubscribeMsgTo(relayerId, hs);
                    }
                }
            }

            // 
            // I should change stripe sender if my a stripe sender do not relay my subscribed stripe
            // for example, 0, 1, 2, 3 are consensus nodes, I am node 5, subscribe stripe 1 from node 4, but node 4 do not relay stripe 1 and now node 6 relays stripe 1.
            // I should subscribe stripe 1 from node 6.
            hs.clear();
            for (Integer stripeId: relayedStripes) {
                if (this.stripeSenderMap.containsKey(stripeId) == false)
                    continue;  
                int currentSender = this.stripeSenderMap.get(stripeId);
                if (this.controller.isCurrentViewMember(currentSender)) 
                    continue;
                if (this.relayerStripeMap.containsKey(currentSender) == false || 
                    this.relayerStripeMap.get(currentSender).contains(stripeId) == false)
                    hs.add(stripeId);
            }
            if (hs.isEmpty() == false)
                sendSubscribeMsgTo(relayerId, hs);
        }
        
        updateRelayer(relayerId, relayerZone, new ArrayList<Integer>(relayedStripes));

        // record the sender and creator have send this msg to me.
        if (this.gossipedMsgSender.containsKey(msg) == false)
            this.gossipedMsgSender.put(msg, new HashSet<>());
        this.gossipedMsgSender.get(msg).add(msg.getSender());
        this.gossipedMsgSender.get(msg).add(msg.getMsgCreator());

        // forward to neighbors exclude msg creator and those neighbors that send that msg to me.
        // ArrayList<Integer> neighbors = getNeighborNodes();
        // neighbors.removeAll(this.gossipedMsgSender.get(msg));
        // only forward zone msg that belongs to myzone, only forward to my subscribers.
        if (relayerZone == this.zoneId) {
            ArrayList<Integer> neighbors = new ArrayList<>(this.subscriberMap.keySet());
            neighbors.removeAll(this.gossipedMsgSender.get(msg));
            msg.setSender(myId);
            msg.setZoneId(zoneId);
            // Don't set timestap for relayer msg !!!!
            // msg.setTimeStamp(System.currentTimeMillis());
            
            multicastMsg(msg, neighbors);
            logger.info("relayerMsgReceived: forward [{}] to neighbors {}, zoneRelayerMap:{}, relayerStripeMap:{}, subscribeMap: {}, subscriberMap:{}",
                    msg.toString(), neighbors, this.zoneRelayerMap, this.relayerStripeMap, this.subscribeMap, this.subscriberMap);
        }
    }

    public void adjustFromReceivedRelayNodeMsgs() {
        long now = System.currentTimeMillis();
        int N = this.controller.getCurrentViewN();
        int F = this.controller.getCurrentViewF();
        if (adjust == false)
            return;
        if (this.nRelayNodesMsgReceived.get() == N
                || (now - this.lastTimeAskRelayNode.get()) > 10000 && this.nRelayNodesMsgReceived.get() > 0) {
            // Todo to be a relayer or a ordinary node?
            logger.info("Node {} receives {} RELAY_NODES messages, N = {}", myId, this.nRelayNodesMsgReceived.get(), N);
            HashSet<Integer> hs = this.zoneRelayerMap.get(zoneId);
            // if there is no relayer, I should subscribe stripes from all consensus nodes.
            if (hs == null || hs.isEmpty()) {
                logger.info("Zoneid = {}, no relayers", zoneId);
                int[] consensusnodes = this.controller.getCurrentViewAcceptors();
                for (int nodeId : consensusnodes) {
                    Set<Integer> subStripes = new HashSet<>();
                    subStripes.add(nodeId);
                    sendSubscribeMsgTo(nodeId, subStripes);
                }
            }
            // There may be not enough relay node in my zone, I should be a relayer
            else {
                HashSet<Integer> stripeCansubscribe = getStripeCanSubscribe();
                logger.info("Zoneid = {}, {} relayers, currentviewN = {} ", zoneId, hs.size(),
                        this.controller.getCurrentViewN());
                // if the number of relayers is less than N
                if (hs.size() < N) {
                    // find a relayer that relay most stripes.
                    int maxStripesnodeId = -1;
                    int maxStripes = 0;
                    HashMap<Integer, HashSet<Integer>> requestNodeStripes = new HashMap<>();
                    for (int relayerId : hs) {
                        HashSet<Integer> stripes = this.relayerStripeMap.get(relayerId);
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

                    // choose half stripes from nodes that relay most stripes
                    for (int stripe : this.relayerStripeMap.get(maxStripesnodeId)) {
                        if (stripeCansubscribe.contains(stripe) == false)
                            continue;
                        else if (requestNodeStripes.get(maxStripesnodeId).size() * 2 >= relayerStripeMap
                                .get(maxStripesnodeId).size())
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
                        HashSet<Integer> stripes = this.relayerStripeMap.get(nodeId);
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
            adjust = false;
        }
    }

    public void multicastMsg(SystemMessage msg, ArrayList<Integer> neighbors) {
        int[] targets = new int[neighbors.size()];
        for (int i = 0; i < neighbors.size(); ++i)
            targets[i] = neighbors.get(i);
        this.cs.send(targets, msg);
    }

    /**
     * periodically broadcast RELAYER msg to neighbors.
     */
    public void broadcastRelayerMsg() {
        long now = System.currentTimeMillis();
        if (this.isRelayer() && now > this.nextTimeBroadcastRelayer.get()) {
            byte[] content = this.mzmMsgSrlzTool.seralizeRelayer(this.relayerStripeMap.get(myId));
            MZMessage msg = this.createMZMessage(TOMUtil.RELAYER, content);
            // only broadcast to nodes in myzone and consensus nodes.
            ArrayList<Integer> neighbors = getNeighborNodes();
            ArrayList<Integer> tmpNeighbors = new ArrayList<>();
            for(int nodeId: neighbors) {
                int tmpZoneId = this.controller.getStaticConf().getZoneId(nodeId);
                if (this.controller.isCurrentViewMember(nodeId) || tmpZoneId == this.zoneId)
                    tmpNeighbors.add(nodeId);
            }
            this.multicastMsg(msg, tmpNeighbors);
            // broadcast every one minute.
            this.nextTimeBroadcastRelayer.set(now + 60000);
            logger.info("broadcastRelayerMsg: Multicast [{}] to neighbors {}, stripes I relayed: {}", msg.toString(),
                    neighbors, this.relayerStripeMap.get(myId));
        
            // If I do not relay stripes from consensus node, I should be an ordinary node.
            if (this.relayerStripeMap.get(myId).isEmpty() || relayerStripeMap.size() == 0) {
                this.setRelayer(false);
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
     * periodically detect other nodes in my zone, neighbor zone and other zones.
     * Todo
     */
    public void detectOtherNodes() {

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
        } else if (msg.getType() == TOMUtil.RELAYER) {
            relayerMsgReceived(msg);
        } else if (msg.getType() == TOMUtil.LATENCY_DETECT) {
            latencyDetectMsgReceived(msg);
        } else {
            logger.error("received a message: --message type: " + msg.getVerboseType()
                    + " but is regard as out of context, from " + msg.getSender());
        }
    }

    /**
     * sending GET_RELAY_NODE to consensus nodes
     * and collect current relay_node info.
     */
    public void askRelayNodes() {
        long now = System.currentTimeMillis();
        if (now - this.lastTimeAskRelayNode.get() < 60000)
            return;
        MZMessage msg = this.createMZMessage(TOMUtil.GET_RELAY_NODE, null);
        int[] initialV = this.controller.getCurrentViewAcceptors();
        logger.info("Node {} multicast [{}] to consensus node {}", myId, msg.toString(), initialV);
        cs.send(initialV, msg);
        this.lastTimeAskRelayNode.set(now);
        this.nRelayNodesMsgReceived.set(0);
    }
}
