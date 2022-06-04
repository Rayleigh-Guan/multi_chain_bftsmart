package bftsmart.multi_zone;

import bftsmart.tom.util.MzProposeBuilder;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.communication.SystemMessage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;
import bftsmart.tom.util.TOMUtil;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MZNodeMan {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ServerViewController controller;
    private ServerCommunicationSystem cs; // comunication system
    private Map<Integer, HashSet<Integer>> subscriberMap; // nodeId --> stripeId other nodes subscribed
    private Map<Integer, HashSet<Integer>> subscribeMap; // nodeId --> stripeId I subscribed
    private Map<Integer, Integer> stripeSenderMap; // stripeId --> nodeId send to me
    private Map<Integer, HashSet<Integer>> zoneRelayerMap;
    private Map<Integer, HashSet<Integer>> relayerStripeMap; // relayerId-> stripe it relayed.
    private Map<Integer, HashSet<Integer>> subscribeMsgSentMap; // subscribe msg I send to other nodes but not
                                                                // received
                                                                // replies.
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

        adjust = true;
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for ( int j = 0; j < bytes.length; j++ ) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
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
    public void forwardNewBlock(byte[] blockHash) {
        String hash = new String(blockHash).substring(0, 16);
        SystemMessage msg = this.candidateBlockMap.get(hash);
        if (msg == null) {
            logger.info("forwardNewBlock can not find the candidateblock [{}]", hash);
            return;
        }
        // baozhuang msg into a MZMessage.
        addForwardData(msg);
    }

    public void addCandidateBlock(byte[] blockHash, Mz_Propose propose, byte[] seralizedPropose) {
        MZBlock candidateBlock = new MZBlock(myId, blockHash, propose, seralizedPropose);
        String hash = new String(blockHash).substring(0, 16);
        this.candidateBlockMap.put(hash, candidateBlock);
        logger.info("Node {} add msg [{}] to candidateblock [{}]", myId, hash);
    }

    public void ForwardDataToSubscriber() {
        if (this.dataQueue.isEmpty()) 
            return;
        logger.info("dataQueue size: {}, consensus nodes: {}, subscriberMap: {}", dataQueue.size(), this.controller.getCurrentViewAcceptors(),this.subscriberMap);
        SystemMessage msg = this.dataQueue.poll();
        for(Map.Entry<Integer, HashSet<Integer>> entry: this.subscriberMap.entrySet()) {
            int nodeId = entry.getKey();
            int[] target = {nodeId};
            if (msg instanceof  ConsensusMessage) {
                ConsensusMessage conMsg = (ConsensusMessage)(msg);
                logger.info("Forward {} to {}", msg.toString(), target);
                cs.send(target, conMsg);
            } else if (msg instanceof MZBlock) {
                MZBlock block = (MZBlock)(msg);
                // seralize block content
                Mz_Propose propose = block.getPropose();
                boolean useSig = (this.controller.getStaticConf().getUseSignatures() == 1);
                MzProposeBuilder mzpb = new MzProposeBuilder(System.nanoTime());
                mzpb.makeMzPropose(propose.list, propose.notsyncreq, propose.numNounces, System.currentTimeMillis(), useSig);
                logger.info("Forward {} to {}", msg.toString(), target);
                cs.send(target, block);
            } else if (msg instanceof MZStripeMessage){
                MZStripeMessage stripe = (MZStripeMessage)(msg);
                logger.info("Forward {} to {}", msg.toString(), target);
                cs.send(target, stripe);
            }
        } 
        // broadcast data hash to other nodes.
        // Todo
    }

    public ArrayList<Integer> getNeighborNodes() {
        return cs.getServersConn().getNeighborNodes();
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

    public boolean addGossipedMsg(MZMessage msg) {
        if (this.gossipedMsg.contains(msg))
            return false;
        this.gossipedMsg.add(msg);
        if (gossipedMsgSender.containsKey(msg.getSender()) == false)
            gossipedMsgSender.put(msg, new HashSet<>());
        this.gossipedMsgSender.get(msg).add(msg.getSender());
        if (this.gossipedMsg.size() > 1000) {
            MZMessage tmpMsg = this.gossipedMsg.pollFirst();
            this.gossipedMsgSender.remove(tmpMsg);
        }
        return true;
    }

    public void unscribe(int subscriberId, int stripeId) {
        if (subscriberMap.containsKey(subscriberId) == false)
            return;
        HashSet<Integer> hs = subscriberMap.get(subscriberId);
        hs.remove(stripeId);
    }

    public void sendSubscribeMsgTo(int target, Set<Integer> subStripes) {
        if (subStripes == null || subStripes.isEmpty())
            return;
        byte[] content = this.mzmMsgSrlzTool.seralizeSubscribe(subStripes);
        MZMessage subscribeMsg = createMZMessage(TOMUtil.SUBSCRIBE, content);
        int[] targets = { target };
        cs.send(targets, subscribeMsg);
        this.subscribeMsgSentMap.put(target, new HashSet<Integer>(subStripes));
        logger.info("Send {} to {}, subscripes: {}", subscribeMsg, target, this.subscribeMsgSentMap.get(target));
        this.nextTimeBroadcastRelayer.set(System.currentTimeMillis() + 5000);
    }

    public void sendUnSubscribedMsgTo(int target, Set<Integer> stripes) {
        byte[] content = this.mzmMsgSrlzTool.seralizeSubscribe(stripes);
        MZMessage unsubscribeMsg = createMZMessage(TOMUtil.UNSUBSCRIBE, content);
        int[] targets = { target };
        cs.send(targets, unsubscribeMsg);
        if (isRelayer() && this.controller.isCurrentViewMember(target)) {
            this.relayerStripeMap.get(myId).removeAll(stripes);
        }
        logger.info("Send {} to {}, relayStripeMap: {}", unsubscribeMsg, target, this.relayerStripeMap.get(myId));
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
        logger.info("getRelayMsgReceived: received msg: {}, relayerStripeMap: {}", msg.toString(),
                this.relayerStripeMap);
    }

    public void updateRelayer(int relayerId, int relayerZone, ArrayList<Integer> relayerStripes) {
        assert (null != relayerStripes) : "updateRelayer: relayerStripes is empty!!";
        // clear
        if (this.relayerStripeMap.containsKey(relayerId))
            this.relayerStripeMap.get(relayerId).clear();
        addRelayer(relayerId, relayerZone, relayerStripes);
    }

    public void addRelayer(int relayerId, int relayerZone, ArrayList<Integer> relayerStripes) {
        assert (null != relayerStripes) : "addRelayer: relayerStripes is empty!!";
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
        logger.info("relayNodesMsgReceived received msg: {}, zoneRelayerMap: {}, relayerStripeMap:{}", msg.toString(),
                this.zoneRelayerMap, this.relayerStripeMap);
        for (HashMap.Entry<Integer, HashSet<Integer>> entry : tmpRelayStripeMap.entrySet()) {
            int relayerId = entry.getKey();
            int relayerZone = this.controller.getStaticConf().getZoneId(relayerId);
            updateRelayer(relayerId, relayerZone, new ArrayList<>(entry.getValue()));
        }
        this.lastReceivedRelayerMsg = msg;
    }

    public void subscribeMsgReceived(MZMessage msg) {
        if (this.zoneRelayerMap.containsKey(msg.getZoneId()) == false)
            this.zoneRelayerMap.put(msg.getZoneId(), new HashSet<>());
        HashSet<Integer> relayerSet = this.zoneRelayerMap.get(msg.getZoneId());

        if (relayerSet.size() > this.controller.getCurrentViewN()) {
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
        logger.info("subscribeMsgReceived received msg: {}, zoneRelayerMap: {}, relayerStripeMap:{}, subscriberMap: {}", msg.toString(),
                this.zoneRelayerMap, this.relayerStripeMap, this.subscriberMap);

    }

    public void unsubscribeMsgReceived(MZMessage msg) {
        if (this.subscriberMap.containsKey(msg.getSender()) == false)
            return;
        ArrayList<Integer> unsubStripes = this.mzmMsgSrlzTool.deseralizeSubscribe(msg.getValue());
        this.subscriberMap.get(msg.getSender()).removeAll(unsubStripes);

        // adjust node's relayer info
        if (this.controller.isInCurrentView()) {
            this.relayerStripeMap.get(msg.getSender()).removeAll(unsubStripes);
        }
        logger.info("receive {} , relayerStripeMap: {}, sender's subscriberMap: {}", msg,
                this.relayerStripeMap.get(myId), this.subscriberMap.get(msg.getSender()));
    }

    public void acceptSubscribeMsgReceived(MZMessage msg) {
        HashSet<Integer> hs = this.subscribeMsgSentMap.get(msg.getSender());
        assert(hs != null):"hs is empty!!";
        
        // add a new relayer
        if (this.controller.isCurrentViewMember(msg.getSender())) {
            this.setRelayer(true);
            addRelayer(this.myId, this.zoneId, new ArrayList<>(hs));
        }

        // send unsubscribe msg to previous sender
        HashMap<Integer, Set<Integer>> unsubscribeMap = new HashMap<>();
        // update my subscribeMap
        if (subscribeMap.get(msg.getSender()) == null)
            subscribeMap.put(msg.getSender(), new HashSet<Integer>());
        for (int stripeId : hs) {
            Integer previousSender = this.stripeSenderMap.get(stripeId);
            if (previousSender != null && previousSender.intValue() != msg.getSender()) {
                if (unsubscribeMap.containsKey(previousSender.intValue()) == false)
                    unsubscribeMap.put(previousSender.intValue(), new HashSet<Integer>());
                unsubscribeMap.get(previousSender.intValue()).add(stripeId);

                // update subscribeMap
                assert(this.subscribeMap.containsKey(previousSender)):"Error in update this.subscribeMap";
                this.subscribeMap.get(previousSender).remove(stripeId);
                this.subscribeMap.get(msg.getSender()).add(stripeId);
            }

            // change stripe sender
            this.stripeSenderMap.put(stripeId, msg.getSender());
        }
        for (HashMap.Entry<Integer, Set<Integer>> entry : unsubscribeMap.entrySet()) 
            sendUnSubscribedMsgTo(entry.getKey(), entry.getValue());

        // remove sent subscribe history
        this.subscribeMsgSentMap.remove(msg.getSender());
        logger.info("acceptSubscribeMsgReceived received msg: {}, mysubscribeMap: {}, stripeSender: {}", msg.toString(), this.subscribeMap, this.stripeSenderMap);
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
                routingTable.put(msg.getMsgCreator(), new HashMap<>());
            if (routingTable.get(msg.getMsgCreator()).containsKey(msg.getSender()) == false)
                routingTable.get(msg.getMsgCreator()).put(msg.getSender(), latency + 1);
            long oldLatency = routingTable.get(msg.getMsgCreator()).get(msg.getSender());
            routingTable.get(msg.getMsgCreator()).put(msg.getSender(), Math.min(oldLatency, latency));
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

        // a relayer should subscribe stripes from a relayer to recude consensus nodes'
        // bandwidth burden.
        if (this.isRelayer() && this.myId < relayerId) {
            HashSet<Integer> hs = new HashSet<>();
            hs.addAll(this.relayerStripeMap.get(myId));
            hs.retainAll(relayedStripes);
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
        int relayerZone = this.controller.getStaticConf().getZoneId(relayerId);
        updateRelayer(relayerId, relayerZone, new ArrayList<Integer>(relayedStripes));

        // record the sender has send this msg to me.
        if (this.gossipedMsgSender.containsKey(msg) == false)
            this.gossipedMsgSender.put(msg, new HashSet<>());
        this.gossipedMsgSender.get(msg).add(msg.getSender());

        // forward to neighbors exclude those neighbors that send that msg to me
        ArrayList<Integer> neighbors = getNeighborNodes();
        neighbors.removeAll(gossipedMsgSender.get(msg));
        msg.setSender(myId);
        msg.setZoneId(zoneId);
        // Don't set timestap for relayer msg !!!!
        // msg.setTimeStamp(System.currentTimeMillis());
        multicastMsg(msg, neighbors);
        logger.info("relayerMsgReceived: forward [{}] to neighbors {}, zoneRelayerMap:{}, relayerStripeMap:{}, subscribeMap: {}, subscriberMap:{}",
                msg.toString(), neighbors, this.zoneRelayerMap, this.relayerStripeMap, this.subscribeMap, this.subscriberMap);
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
                        HashSet<Integer> stripes = relayerStripeMap.get(relayerId);
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
                    for (int stripe : relayerStripeMap.get(maxStripesnodeId)) {
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
                        HashSet<Integer> stripes = relayerStripeMap.get(nodeId);
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

    public void multicastMsg(MZMessage msg, ArrayList<Integer> neighbors) {
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
            ArrayList<Integer> neighbors = getNeighborNodes();
            this.multicastMsg(msg, neighbors);
            // broadcast every one minute.
            this.nextTimeBroadcastRelayer.set(now + 60000);
            logger.info("broadcastRelayerMsg: Multicast [{}] to neighbors {}, stripes I relayed: {}", msg.toString(),
                    neighbors, this.relayerStripeMap.get(myId));
        }
    }

    /**
     * periodically broadcast LATENCY_DETECT msg to neighbors.
     */
    public void broadcastLatencyDetectMsg() {
        if (isConsensusNode() == false) return;
        long now = System.currentTimeMillis();
        if (now < nextTimeBroadcastLatencyDetect.get())
            return;
        ArrayList<Object> nodeIdTimeStampList = new ArrayList<>();
        nodeIdTimeStampList.add(myId);
        nodeIdTimeStampList.add(System.currentTimeMillis());
        byte[] content = this.mzmMsgSrlzTool.seralizeLatencyDetect(nodeIdTimeStampList);
        MZMessage msg = createMZMessage(TOMUtil.LATENCY_DETECT, content);
        ArrayList<Integer> neighbors = getNeighborNodes();
        multicastMsg(msg, neighbors);
        nextTimeBroadcastLatencyDetect.set(now + 60000);
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
