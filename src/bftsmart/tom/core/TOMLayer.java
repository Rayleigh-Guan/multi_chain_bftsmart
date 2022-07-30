/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package bftsmart.tom.core;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignedObject;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;

import bftsmart.clientsmanagement.ClientsManager;
import bftsmart.clientsmanagement.RequestList;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.communication.client.RequestReceiver;
import bftsmart.consensus.Decision;
import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.multi_zone.*;
import bftsmart.multi_zone.RebuildProposeState;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.StateManager;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.leaderchange.RequestsTimer;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.RequestVerifier;
import bftsmart.tom.util.*;
import bftsmart.communication.SystemMessage;

import bftsmart.erasureCode.ReedSolomon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class implements the state machine replication protocol described in
 * Joao Sousa's 'From Byzantine Consensus to BFT state machine replication: a
 * latency-optimal transformation' (May 2012)
 * 
 * The synchronization phase described in the paper is implemented in the
 * Synchronizer class
 */
public final class TOMLayer extends Thread implements RequestReceiver {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public MZNodeMan mzNodeMan;

    // public Map<Integer, SystemMessage> MZBlockMap;
    // public Map<Integer, Long> MZBlockRecvTime;
    private MZBlockchainMan mzblockchainMan;

    private boolean doWork = true;
    // other components used by the TOMLayer (they are never changed)
    public ExecutionManager execManager; // Execution manager
    public Acceptor acceptor; // Acceptor role of the PaW algorithm
    private ServerCommunicationSystem communication; // Communication system between replicas
    // private OutOfContextMessageThread ot; // Thread which manages messages that
    // do not belong to the current consensus
    private DeliveryThread dt; // Thread which delivers total ordered messages to the appication
    public StateManager stateManager = null; // object which deals with the state transfer protocol

    // thread pool used to paralelise verification of requests contained in a batch
    private ExecutorService verifierExecutor = null;

    /**
     * Manage timers for pending requests
     */
    public RequestsTimer requestsTimer;
    /**
     * Store requests received but still not ordered
     */
    public ClientsManager clientsManager;
    /**
     * The id of the consensus being executed (or -1 if there is none)
     */
    private int inExecution = -1;
    private int lastExecuted = -1;
    private long lastTimeReceiveMzPropose = 0;
    private long lastTimeCreatePropose = 0;

    public MessageDigest md;
    private Signature engine;

    private ReentrantLock hashLock = new ReentrantLock();

    // the next two are used to generate non-deterministic data in a deterministic
    // way (by the leader)
    public BatchBuilder bb = new BatchBuilder(System.nanoTime());

    /* The locks and conditions used to wait upon creating a propose */
    private ReentrantLock leaderLock = new ReentrantLock();
    private Condition iAmLeader = leaderLock.newCondition();
    private ReentrantLock messagesLock = new ReentrantLock();
    private Condition haveMessages = messagesLock.newCondition();
    private ReentrantLock proposeLock = new ReentrantLock();
    private Condition canPropose = proposeLock.newCondition();

    private PrivateKey prk;
    public ServerViewController controller;

    private RequestVerifier verifier;

    private Synchronizer syncher;

    private multi_chain multiChain;
    private Thread packbundlethread;
    private Thread mznodeThread;
    private MzBatchBuilder mzbb;


    private MZInFlightMan mzInflightMan;
    private Map<Integer, Integer> failToRecoveryCntMap;     // block fail to recovery and its failed cnt

    /**
     * Creates a new instance of TOMulticastLayer
     *
     * @param manager    Execution manager
     * @param receiver   Object that receives requests from clients
     * @param recoverer
     * @param a          Acceptor role of the PaW algorithm
     * @param cs         Communication system between replicas
     * @param controller Reconfiguration Manager
     * @param verifier
     */
    public TOMLayer(ExecutionManager manager,
            ServiceReplica receiver,
            Recoverable recoverer,
            Acceptor a,
            ServerCommunicationSystem cs,
            ServerViewController controller,
            RequestVerifier verifier,
            MZNodeMan mzNodeMan
            ) {

        super("TOM Layer");

        this.execManager = manager;
        this.acceptor = a;
        this.communication = cs;
        this.controller = controller;
        // use either the same number of Netty workers threads if specified in the
        // configuration
        // or use a many as the number of cores available
        int nWorkers = this.controller.getStaticConf().getNumNettyWorkers();
        nWorkers = nWorkers > 0 ? nWorkers : Runtime.getRuntime().availableProcessors();
        this.verifierExecutor = Executors.newWorkStealingPool(nWorkers);

        // do not create a timer manager if the timeout is 0
        if (this.controller.getStaticConf().getRequestTimeout() == 0) {
            this.requestsTimer = null;
        } else {
            this.requestsTimer = new RequestsTimer(this, communication, this.controller); // Create requests timers
                                                                                          // manager (a thread)
        }

        try {
            this.md = TOMUtil.getHashEngine();
        } catch (Exception e) {
            logger.error("Failed to get message digest engine", e);
        }

        try {
            this.engine = TOMUtil.getSigEngine();
        } catch (Exception e) {
            logger.error("Failed to get signature engine", e);
        }

        this.prk = this.controller.getStaticConf().getPrivateKey();
        this.dt = new DeliveryThread(this, receiver, recoverer, this.controller); // Create delivery thread
        this.dt.start();
        this.stateManager = recoverer.getStateManager();
        stateManager.init(this, dt);

        this.verifier = (verifier != null) ? verifier : ((request) -> true); // By default, never validate requests

        // I have a verifier, now create clients manager
        this.clientsManager = new ClientsManager(this.controller, requestsTimer, this.verifier);

        this.syncher = new Synchronizer(this); // create synchronizer
        this.multiChain = new multi_chain(receiver.getId(), this.controller.getCurrentViewN(), 
            this.controller.getCurrentViewF(),  this.controller.getStaticConf().getUseSignatures()==1);

        this.mzbb = new MzBatchBuilder();

        this.mzNodeMan = mzNodeMan;
        if (this.controller.isInCurrentView())
            this.mzNodeMan.setConsensusNode(true);
        
        initPackBundleThread();

        initMZNodeThread();

        // store candidate block and blockchain
        this.mzblockchainMan = new MZBlockchainMan();

        this.mzInflightMan = new MZInFlightMan();

        this.failToRecoveryCntMap = new HashMap<>();
    }

    /**
     * Computes an hash for a TOM message
     *
     * @param data Data from which to generate the hash
     * @return Hash for the specified TOM message
     */
    public final byte[] computeHash(byte[] data) {
        byte[] ret = null;
        hashLock.lock();
        ret = md.digest(data);
        hashLock.unlock();
        return ret;
    }

    public SignedObject sign(Serializable obj) {
        try {
            return new SignedObject(obj, prk, engine);
        } catch (Exception e) {
            logger.error("Failed to sign object", e);
            return null;
        }
    }

    /**
     * Verifies the signature of a signed object
     *
     * @param so     Signed object to be verified
     * @param sender Replica id that supposedly signed this object
     * @return True if the signature is valid, false otherwise
     */
    public boolean verifySignature(SignedObject so, int sender) {
        try {
            return so.verify(controller.getStaticConf().getPublicKey(sender), engine);
        } catch (Exception e) {
            logger.error("Failed to verify object signature", e);
        }
        return false;
    }

    /**
     * Retrieve Communication system between replicas
     *
     * @return Communication system between replicas
     */
    public ServerCommunicationSystem getCommunication() {
        return this.communication;
    }

    public void imAmTheLeader() {
        leaderLock.lock();
        iAmLeader.signal();
        leaderLock.unlock();
    }

    /**
     * Sets which consensus was the last to be executed
     *
     * @param last ID of the consensus which was last to be executed
     */
    public void setLastExec(int last) {
        this.lastExecuted = last;
    }

    /**
     * Gets the ID of the consensus which was established as the last executed
     *
     * @return ID of the consensus which was established as the last executed
     */
    public int getLastExec() {
        return this.lastExecuted;
    }

    /**
     * Sets which consensus is being executed at the moment
     *
     * @param inEx ID of the consensus being executed at the moment
     */
    public void setInExec(int inEx) {
        proposeLock.lock();
        logger.debug("Modifying inExec from " + this.inExecution + " to " + inEx);
        this.inExecution = inEx;
        if (inEx == -1 && !isRetrievingState()) {
            canPropose.signalAll();
        }
        proposeLock.unlock();
    }

    /**
     * This method blocks until the PaW algorithm is finished
     */
    public void waitForPaxosToFinish() {
        proposeLock.lock();
        canPropose.awaitUninterruptibly();
        proposeLock.unlock();
    }

    /**
     * Gets the ID of the consensus currently beign executed
     *
     * @return ID of the consensus currently beign executed (if no consensus ir
     *         executing, -1 is returned)
     */
    public int getInExec() {
        return this.inExecution;
    }

    /**
     * This method is invoked by the communication system to deliver a request.
     * It assumes that the communication system delivers the message in FIFO
     * order.
     *
     * @param msg The request being received
     */
    @Override
    public void requestReceived(TOMMessage msg) {

        if (!doWork)
            return;

        // check if this request is valid and add it to the client' pending requests
        // list
        boolean readOnly = (msg.getReqType() == TOMMessageType.UNORDERED_REQUEST
                || msg.getReqType() == TOMMessageType.UNORDERED_HASHED_REQUEST);
        if (readOnly) {
            logger.debug("Received read-only TOMMessage from client " + msg.getSender() + " with sequence number "
                    + msg.getSequence() + " for session " + msg.getSession());

            dt.deliverUnordered(msg, syncher.getLCManager().getLastReg());
        } else {
            logger.debug("Received TOMMessage from client " + msg.getSender() + " with sequence number "
                    + msg.getSequence() + " for session " + msg.getSession());

            if (clientsManager.requestReceived(msg, true, communication)) {
                haveMessages();
            } else {
                logger.warn("The received TOMMessage " + msg + " was discarded.");
            }
        }
    }

    /**
     * Creates a value to be proposed to the acceptors. Invoked if this replica
     * is the leader
     *
     * @param dec Object that will eventually hold the decided value
     * @return A value to be proposed to the acceptors
     */
    public byte[] createPropose(Decision dec) {
        // Retrieve a set of pending requests from the clients manager
        RequestList pendingRequests = clientsManager.getPendingRequests();
        logger.debug("Node id: " + controller.getStaticConf().getProcessId() + " pack batch:" + pendingRequests);
        int numberOfMessages = pendingRequests.size(); // number of messages retrieved
        int numberOfNonces = this.controller.getStaticConf().getNumberOfNonces(); // ammount of nonces to be generated
        long timeStamp = System.currentTimeMillis();
        // for benchmarking
        if (dec.getConsensusId() > -1) { // if this is from the leader change, it doesnt matter
            dec.firstMessageProposed = pendingRequests.getFirst();
            dec.firstMessageProposed.consensusStartTime = timeStamp;
        }
        dec.batchSize = numberOfMessages;

        logger.debug("Creating a PROPOSE with " + numberOfMessages + " msgs");

        return bb.makeBatch(pendingRequests, numberOfNonces, timeStamp,
                controller.getStaticConf().getUseSignatures() == 1);
    }

    // our propose
    public byte[] createMzPropose(Decision dec) {
        long timeStamp = System.currentTimeMillis();
        logger.debug("Stage: createMzPropose --Node create a Mzpropose at:" + timeStamp);
        // List<Mz_BatchListItem> pendingBatch = multiChain.packList();
        List<Mz_BatchListItem> pendingBatch = multiChain.packListWithTip();
        if (pendingBatch.size() == 0) {
            logger.debug("Stage: createMzPropose --Node create a mzpropose faild,because packlist is null");
            return null;
        } else {
            logger.info("Stage: createMzPropose --Node create a mzpropose ,and success get a packlist,list size:"
                    + pendingBatch.size());
        }
        // logger.info("Stage: createMzPropose --Node create a Mzpropose at:" + timeStamp);
        RequestList requestnotsync = multiChain.getnotsyncRequestfromlist(pendingBatch);

        int numberOfItems = pendingBatch.size(); // number of messages retrieved
        int numberOfNonces = this.controller.getStaticConf().getNumberOfNonces(); // ammount of nonces to be generated
        // for benchmarking
        if (dec.getConsensusId() > -1) { // if this is from the leader change, it doesnt matter
            logger.debug("Mzpropose try to get first msg pendingbatch:" + pendingBatch);
            if (requestnotsync.size() == 0) {
                dec.firstMessageProposed = multiChain.getFirstRequest(pendingBatch);
                dec.firstMessageProposed.consensusStartTime = System.currentTimeMillis();
            } else {
                dec.firstMessageProposed = requestnotsync.get(0);
                dec.firstMessageProposed.consensusStartTime = System.currentTimeMillis();
            }
        }
        dec.batchSize = numberOfItems;

        logger.debug("Creating a MzPROPOSE with " + numberOfItems + " Items");
        Mz_Propose mzpropose = new Mz_Propose(dec.getConsensusId(),timeStamp, timeStamp, numberOfNonces, requestnotsync,pendingBatch);
        // return mzpb.makeMzPropose(pendingBatch, requestnotsync, numberOfNonces, System.currentTimeMillis(),
        //         controller.getStaticConf().getUseSignatures() == 1);
        return Mz_Propose.seralizeMZPropose(mzpropose, controller.getStaticConf().getUseSignatures() == 1);
    }

    /**
     * This is the main code for this thread. It basically waits until this
     * replica becomes the leader, and when so, proposes a value to the other
     * acceptors
     */
    @Override
    public void run() {
        logger.debug("Running."); // TODO: can't this be outside of the loop?
        
        if (this.controller.getStaticConf().getNetworkingMode() != TOMUtil.NM_CONSENSUS)
            this.mznodeThread.start();
        if (this.controller.isInCurrentView() ){
            int ds = this.controller.getStaticConf().getDataDisStrategy();
            if (ds == TOMUtil.DS_PREDIS_EC || ds == TOMUtil.DS_PREDIS_FULL) {
                // pack batch
                this.packbundlethread.start();
                // start predis propose
                startCreatePredisPropose();
            }
            else {
                // start basic propose
                startCreateBasicPropose();
            }
        }
    }

    /**
     * Called by the current consensus instance, to notify the TOM layer that
     * a value was decided
     *
     * @param dec The decision of the consensus
     */
    public void decided(Decision dec) {

        dec.setRegency(syncher.getLCManager().getLastReg());
        dec.setLeader(execManager.getCurrentLeader());

        this.dt.delivery(dec); // Sends the decision to the delivery thread
        // forward candidate block
        Epoch epoch = dec.getDecisionEpoch();

        int networkmode = this.controller.getStaticConf().getNetworkingMode();
        if (networkmode == TOMUtil.NM_CONSENSUS)
            return;
        SystemMessage block = this.mzNodeMan.getCandidateBlock(epoch.propValueHash);
        if (block == null) return;
        if (block instanceof MZBlock) {
            MZBlock mzblock = (MZBlock)(block);
            this.OnMZBlock(mzblock);
        }   
        else if (block instanceof ConsensusMessage) {
            ConsensusMessage ordinaryBlock = (ConsensusMessage)(block);
            this.OnBlock(ordinaryBlock);
        }
    }

    /**
     * Verify if the value being proposed for a epoch is valid. It verifies the
     * client signature of all batch requests.
     *
     * TODO: verify timestamps and nonces
     *
     * @param proposedValue      the value being proposed
     * @param addToClientManager add the requests to the client manager
     * @return Valid messages contained in the proposed value
     */
    public TOMMessage[] checkProposedValue(byte[] proposedValue, boolean addToClientManager) {

        try {

            logger.debug("Checking proposed value");
            logger.debug("Stage: node {} checkProposedValue --Checking proposed value",
                    this.controller.getStaticConf().getProcessId());
            BatchReader batchReader = new BatchReader(proposedValue,
                    this.controller.getStaticConf().getUseSignatures() == 1);

            TOMMessage[] requests = null;

            // deserialize the message
            // TODO: verify Timestamps and Nonces
            requests = batchReader.deserialiseRequests(this.controller);

            if (addToClientManager) {

                // use parallelization to validate the request
                final CountDownLatch latch = new CountDownLatch(requests.length);

                for (TOMMessage request : requests) {

                    verifierExecutor.submit(() -> {
                        try {

                            // notifies the client manager that this request was received and get
                            // the result of its validation
                            request.isValid = true;// clientsManager.requestReceived(request, false);
                            if (Thread.holdsLock(clientsManager.getClientsLock()))
                                clientsManager.getClientsLock().unlock();

                        } catch (Exception e) {

                            logger.error("Error while validating requests", e);
                            if (Thread.holdsLock(clientsManager.getClientsLock()))
                                clientsManager.getClientsLock().unlock();

                        }

                        latch.countDown();
                    });
                }

                latch.await();

                for (TOMMessage request : requests) {

                    if (request.isValid == false) {

                        logger.warn(
                                "Request {} could not be added to the pending messages queue of its respective client",
                                request);
                        return null;
                    }
                }
            }

            logger.debug("Successfully deserialized batch");
            return requests;

        } catch (Exception e) {
            logger.error("Failed to check proposed value", e);
            if (Thread.holdsLock(clientsManager.getClientsLock()))
                clientsManager.getClientsLock().unlock();
            return null;
        }
    }

    public void forwardRequestToLeader(TOMMessage request) {
        int leaderId = execManager.getCurrentLeader();
        if (this.controller.isCurrentViewMember(leaderId)) {
            logger.debug("Forwarding " + request + " to " + leaderId);
            communication.send(new int[] { leaderId },
                    new ForwardedMessage(this.controller.getStaticConf().getProcessId(), request));
        }
    }

    public boolean isRetrievingState() {
        // lockTimer.lock();
        boolean result = stateManager != null && stateManager.isRetrievingState();
        // lockTimer.unlock();

        return result;
    }

    public boolean isChangingLeader() {

        return requestsTimer != null && !requestsTimer.isEnabled();
    }

    public void setNoExec() {
        logger.debug("Modifying inExec from " + this.inExecution + " to " + -1);

        proposeLock.lock();
        this.inExecution = -1;
        // ot.addUpdate();
        canPropose.signalAll();
        proposeLock.unlock();
    }

    public void processOutOfContext() {
        for (int nextConsensus = getLastExec() + 1; execManager
                .receivedOutOfContextPropose(nextConsensus); nextConsensus = getLastExec() + 1) {
            execManager.processOutOfContextPropose(execManager.getConsensus(nextConsensus));
        }
    }

    public StateManager getStateManager() {
        return stateManager;
    }

    public Synchronizer getSynchronizer() {
        return syncher;
    }

    private void haveMessages() {
        messagesLock.lock();
        haveMessages.signal();
        messagesLock.unlock();
    }

    public DeliveryThread getDeliveryThread() {
        return dt;
    }

    public void shutdown() {
        this.doWork = false;
        imAmTheLeader();
        haveMessages();
        setNoExec();

        if (this.requestsTimer != null)
            this.requestsTimer.shutdown();
        if (this.clientsManager != null) {
            this.clientsManager.clear();
            this.clientsManager.getPendingRequests().clear();
        }
        if (this.dt != null)
            this.dt.shutdown();
        if (this.communication != null)
            this.communication.shutdown();
        if (this.packbundlethread != null)
            this.packbundlethread.interrupt();
    }

    public void OnMzBatch(ConsensusMessage msg) {
        // 拒绝来自自己的batch。避免重复存储
        int myId = this.controller.getStaticConf().getProcessId();
        MzBatchReader mzbatchReader = new MzBatchReader(msg.getValue(),
                this.controller.getStaticConf().getUseSignatures() == 1);
        Mz_Batch mzBatch = mzbatchReader.deserialisemsg();
        // logger.info("Node {} receive {} from ", myId, mzBatch.toString(), msg.getSender());
        int networkmode = this.controller.getStaticConf().getNetworkingMode();
        if (networkmode != TOMUtil.NM_CONSENSUS) {
            int dsBundleMode = this.controller.getStaticConf().getDisBundleToFullNodes();
            if (dsBundleMode == TOMUtil.DS_BUNDLE_EC) {
                MZStripeMessage[] mzStripeArray = this.generateStripe(msg.getValue(), mzBatch);
                for(MZStripeMessage stripeMsg: mzStripeArray)
                    this.multiChain.addStripeMsg(stripeMsg);
                this.mzNodeMan.addForwardData(mzStripeArray[myId]);
                // logger.info("Node {} add {} to forwardQueue", myId, mzStripeArray[myId]);
            }
            else if (dsBundleMode == TOMUtil.DS_BUNDLE_FULL) {
                long now = System.currentTimeMillis();
                int dataType = TOMUtil.DH_BUNDLEHASH;
                byte[] dataHash = msg.getHash();
                int[] appendix = new int[]{mzBatch.getNodeId(), mzBatch.getBatchId()};
                DataHashMessage datahashMsg = new DataHashMessage(myId, dataType, now, appendix, dataHash);
                // logger.info("Node {} add {} to forwardQueue", myId, datahashMsg);
                this.mzNodeMan.addForwardData(datahashMsg);
            }
        }
        if (myId != msg.getSender()) {
            // logger.info("Node {} receive bundle {}-{} from {}", myId, mzBatch.getNodeId(), mzBatch.getBatchId(), msg.getSender());
            if (this.controller.isInCurrentView())
                this.multiChain.add(mzBatch);
            else {
                this.multiChain.addBundle(mzBatch);
            }
        }
    }

    public void OnMZStripe(MZStripeMessage msg) {
        int myId = this.controller.getStaticConf().getProcessId();
        //int ds = this.controller.getStaticConf().getDataDisStrategy();
        int networkmode = this.controller.getStaticConf().getNetworkingMode();
        // only forward stripe from other node
        if (msg.getBatchChainId() == myId || msg.getSender() == myId)
           return;
        // if (false == this.controller.isInCurrentView())
        //     logger.info("Node {} receive a MZStripeMsg: {}", myId, msg.toString());
        // boolean addRes = this.multiChain.addStripeMsg(msg);
        // If I have already received this stripe, do not forward the stripe if ds is not DS_ENHANCED_FAB
        // if (addRes == false && (ds != TOMUtil.DS_RANDOM_ENHANCED_FAB)) 
        //     return;
        // if I am a consensus node and stripeId is myId,
        // then forward the message to other consensus nodes.
        MZStripeMessage forwardMsg = new MZStripeMessage(myId, msg.getBatchChainId(), msg.getHeight(),
                msg.getTotalLen(), msg.getStripeId(),msg.getValidDataLen(), msg.getStripe());
        if (this.controller.isInCurrentView() && msg.getStripeId() == myId) {
            int[] consensusNodes = this.controller.getCurrentViewAcceptors();
            for(int nodeId: consensusNodes){
                if (nodeId == myId || nodeId == msg.getSender())    continue;
                communication.send(new int[]{nodeId}, forwardMsg);
                // logger.info("Forward a MZStripeMsg: {} to {}", forwardMsg.toString(), nodeId);     
            }
        }
        if (networkmode == TOMUtil.NM_MULTI_ZONE) {
            // A non-consensus node always relays a stripe message
            // A consensus node only relays stripes whose stripeId equals to its id.
            if (this.controller.isInCurrentView()==false || msg.getStripeId() == myId ) {
                // Todo forward stripe to my subscriber
                this.mzNodeMan.addForwardData(forwardMsg);
            }
        }
        else if(networkmode == TOMUtil.NM_STAR) {
            // consensus nodes forward stripes whose stripeId equals to its id.
            if (this.controller.isInCurrentView() && msg.getStripeId() == myId ) {
                this.mzNodeMan.addForwardData(forwardMsg);
            }
        }
        else if (networkmode == TOMUtil.NM_RANDOM) {
            this.mzNodeMan.addForwardData(forwardMsg);
        }
        
        this.multiChain.addStripeMsg(msg);
    }

    public RebuildProposeState rebuildPropose(Mz_Propose mz_propose) {
        RebuildProposeState reply = this.multiChain.getsyncedRequestfromlist(mz_propose.bundleSliceList);
        if (reply.getState()){
            int numNotSyncReq = 0, numSyncReq = 0;
            numNotSyncReq = mz_propose.reqList.size();
            numSyncReq = reply.getlist().size();
            reply.reqList.addAll(0, mz_propose.reqList);
            int myId = this.controller.getStaticConf().getProcessId();
            boolean useSig = this.controller.getStaticConf().getUseSignatures() == 1;
            reply.batch = bb.makeBatch(reply.getlist(), mz_propose.numNounces, mz_propose.seed, mz_propose.timestamp, useSig);
            this.multiChain.setLastbatchlist(mz_propose.bundleSliceList);
            // logger.info("rebuildPropose: Node {} rebuild a mzpropose, {} full txes, {} batch txes, total {} txes", myId, numNotSyncReq, numSyncReq, reply.reqList.size());
        }
        return reply;
    }

    public void OnDataHash(DataHashMessage dh) {
        int type = dh.getType();
        int blockheight = dh.getAppendix()[0];
        int myId = this.controller.getStaticConf().getProcessId();
        int sender = dh.getSender();
        ArrayList<Long> record = new ArrayList<>();
        record.add(Long.valueOf(dh.getSender()));
        record.add(System.currentTimeMillis());
        // logger.info("Node {} receives {}, type {}", myId, dh.toString(), type);
        ArrayList<Integer> arr = new ArrayList<>();
        for(int ele : dh.getAppendix())
            arr.add(ele);
        if (type == TOMUtil.DH_BLODKHASH) {
            if (this.mzblockchainMan.containsBlock(arr.get(0)) == false &&  this.mzInflightMan.contains(arr) == false) {
                this.mzInflightMan.addMsgInflight(arr, record);
                dh.setType(TOMUtil.DH_GETPREDISBLOCK);
                dh.setSender(myId);
                this.communication.send(new int[]{sender}, dh);
                // logger.info("Node {} receives DH_BLODKHASH and send [{}] to {}",myId, dh,sender);
            }
            this.mzInflightMan.addDownloadSource(arr, sender);
            
        } else if (type == TOMUtil.DH_GETPREDISBLOCK) {
            if (this.mzblockchainMan.containsBlock(blockheight)) {
                SystemMessage msg = this.mzblockchainMan.getPredisBlock(blockheight);
                msg.setSender(myId);
                this.communication.send(new int[]{sender}, msg);
                logger.info("Node {} receives DH_GETPREDISBLOCK and send [{}] to {}",myId, msg, sender);
            }
            else {
                logger.warn("Node {} receives DH_GETPREDISBLOCK but can not find that blockheight: {}",myId, blockheight);
            }
        } else if (type == TOMUtil.DH_STRIPEHASH) {
            MZStripeMessage stripe = this.multiChain.getStripe(dh.getAppendix()[0], dh.getAppendix()[1],dh.getAppendix()[2]);
            // if this stipe is not in flight or not received, request it.
            if (this.mzInflightMan.contains(arr) == false && stripe == null ) {
                this.mzInflightMan.addMsgInflight(arr, record);
                dh.setType(TOMUtil.DH_GETSTRIPE);
                dh.setSender(myId);
                this.communication.send(new int[]{sender}, dh);
                // logger.info("Node {} receives DH_STRIPEHASH and send [{}] to {}",myId, dh.toString(), sender);
            }
            this.mzInflightMan.addDownloadSource(arr, sender);
        } else if (type == TOMUtil.DH_GETSTRIPE) {
            MZStripeMessage stripe = this.multiChain.getStripe(dh.getAppendix()[0], dh.getAppendix()[1], dh.getAppendix()[2]);
            if (stripe == null) {
                logger.info("Node {} cannot find stripe {}", myId, dh.getAppendix());
                return;
            }
            stripe.setSender(myId);
            this.communication.send(new int[]{sender}, stripe);
            logger.info("Node {} receives DH_GETSTRIPE and send [{}] to {}", myId, stripe.toString(), sender);
        }
        else if (type == TOMUtil.DH_BUNDLEHASH){
            int bundleChainId = dh.getAppendix()[0];
            int bundleHeight = dh.getAppendix()[1];
            Mz_Batch bundle = this.multiChain.getBundle(bundleChainId, bundleHeight);
            if (bundle == null && this.mzInflightMan.contains(arr) == false) {
                this.mzInflightMan.addMsgInflight(arr, record);
                dh.setType(TOMUtil.DH_GETBUNDLE);
                dh.setSender(myId);
                this.communication.send(new int[]{sender}, dh);
                // logger.info("Node {} receives DH_BUNDLEHASH and send [{}] to {}",myId, dh.toString(), sender);
            }
            this.mzInflightMan.addDownloadSource(arr, sender);
        }
        else if (type == TOMUtil.DH_GETBUNDLE) {
            int bundleChainId = dh.getAppendix()[0];
            int bundleHeight = dh.getAppendix()[1];
            Mz_Batch bundle = this.multiChain.getBundle(bundleChainId, bundleHeight);
            if (bundle == null) {
                logger.error("{} can not find bundle {}-{}", myId, bundleChainId, bundleHeight);
                return;
            }
            // logger.info("chain pool tip: {}", bundle.getChainPooltip());
            int useSig = this.controller.getStaticConf().getUseSignatures();
            byte[] seralizedBundle = this.mzbb.makeMzBatch(bundle, useSig == 1);
            MessageFactory messageFactory = new MessageFactory(myId);
            ConsensusMessage bundleMsg = messageFactory.createMzBatch(0, 0, seralizedBundle);
            this.communication.send(new int[]{sender}, bundleMsg);
            // logger.info("Node {} receives DH_GETBUNDLE and send bundle [{}-{}] to {}", myId, bundleChainId, bundleHeight, sender);
        }
        else {
            logger.info("Node {} receive a error type {}", myId, type);
        }
    }

    public void OnMZBlock(MZBlock block){
        int myId = this.controller.getStaticConf().getProcessId();
        boolean useSig = this.controller.getStaticConf().getUseSignatures() == 1;
        block.deseralizePropose(useSig);
        int blockHeight = block.getPropose().blockHeight;
        // if I am a consensus node, do not need to recovery block 
        // I just store and forward the block
        if (this.controller.isInCurrentView()) {
            if (this.mzblockchainMan.addValidBlock(block, blockHeight)) {
                block.setSender(myId);
                this.mzNodeMan.addForwardData(block);
            }
            return;
        }
        // if the first time receives
        // store the block and let recoveryBlock function to recovery the block
        // else just forward the block if it is DS_RANDOM_ENHANCED_FAB mode
        int ds = this.controller.getStaticConf().getDataDisStrategy();
        boolean addRes = this.mzblockchainMan.addPredisBlock(block);
        if (addRes == false && ds == TOMUtil.DS_RANDOM_ENHANCED_FAB) {
            block.setSender(myId);
            this.mzNodeMan.addForwardData(block);
        }
    }

    // a node receives a block only in random 
    public void OnBlock(ConsensusMessage block) {
        int blockHeight = block.getNumber();
        int myId = this.controller.getStaticConf().getProcessId();
        boolean addRes = this.mzblockchainMan.addValidBlock(block, blockHeight);
        // if I am a consensus node, do not need to recovery block 
        // I just store and forward the block
        if (this.controller.isInCurrentView()) {
            if (addRes) {
                block.setSender(myId);
                this.mzNodeMan.addForwardData(block);
            }
            return;
        }
        // if the first time receives
        // store the block and let recoveryBlock function to recovery the block
        // else just forward the block if it is DS_RANDOM_ENHANCED_FAB mode
        int ds = this.controller.getStaticConf().getDataDisStrategy();
        if (addRes == false && ds == TOMUtil.DS_RANDOM_ENHANCED_FAB) {
            block.setSender(myId);
            this.mzNodeMan.addForwardData(block);
        }
    }

    public void storeCandidateBlock(byte[] blockHash, ConsensusMessage msg) {
        int networkmode = this.controller.getStaticConf().getNetworkingMode();
        if (msg.getType() != MessageFactory.PROPOSE || networkmode == TOMUtil.NM_CONSENSUS)
            return;
        if (networkmode == TOMUtil.NM_STAR) {
            msg.setHash(blockHash);
            this.mzNodeMan.storeCandidateBlock(blockHash, msg);
        }
    }

    public void OnMzPropose(Epoch epoch, ConsensusMessage msg) {
        // MzProposeReader mzproposeReader = new MzProposeReader(msg.getValue(),
        //         controller.getStaticConf().getUseSignatures() == 1);
        logger.debug("OnMzPropose1 Nodeid: " + this.controller.getStaticConf().getProcessId()
                + " received mzpropose from: " + msg.getSender() + " --msg type: " + msg.getType());

        Mz_Propose mz_propose = Mz_Propose.deseralizeMZPropose(msg.getValue(), controller.getStaticConf().getUseSignatures() == 1);

        RebuildProposeState reply = this.multiChain.getsyncedRequestfromlist(mz_propose.bundleSliceList);
        if (!reply.getState())
            return;
        MessageFactory messageFactory = new MessageFactory(msg.getSender());

        this.multiChain.setLastbatchlist(mz_propose.bundleSliceList);

        RequestList totalreqlist = new RequestList();
        if (mz_propose.reqList.size() > 0) {
            totalreqlist.addAll(mz_propose.reqList);
        }
        if (reply.getlist().isEmpty() == false)
            totalreqlist.addAll(reply.getlist());
        this.acceptor.proposeReceived(epoch,
                messageFactory.createPropose(msg.getNumber(), msg.getEpoch(),
                        bb.makeBatch(totalreqlist, mz_propose.numNounces, mz_propose.seed, mz_propose.timestamp,
                                controller.getStaticConf().getUseSignatures() == 1)));
    }

    public void OnMzPropose(ConsensusMessage msg) {
        // MzProposeReader mzproposeReader = new MzProposeReader(msg.getValue(),
        //         controller.getStaticConf().getUseSignatures() == 1);
        int myId = this.controller.getStaticConf().getProcessId();
        Mz_Propose mz_propose = Mz_Propose.deseralizeMZPropose(msg.getValue(), controller.getStaticConf().getUseSignatures() == 1);
        logger.debug("Stage:OnMzPropose2 --Nodeid: " + myId + " received mzpropose from: " + msg.getSender()
        + " --msg type: " + msg.getType());
        // System.out.println("Stage: received Mzpropose at: " + System.currentTimeMillis());
        
        // record last tiem receive a propose
        long now = System.currentTimeMillis();
        long timediff = (now - this.lastTimeReceiveMzPropose);
        this.lastTimeReceiveMzPropose = now;

        RebuildProposeState reply = rebuildPropose(mz_propose);
        if (reply.getState() == false) {
            return;
        }
        // record the candidate block
        byte[] blockHash = computeHash(reply.batch);
        this.mzNodeMan.addMZCandidateBlock(blockHash, mz_propose, msg.getValue());
        // logger.info("Node {} receive a mzpropose from node {}, msg: [{}], at time {}, after {} ms, mz_propose size {}, tx size {}",
        // myId, msg.getSender(), mz_propose.toString(), now, timediff, msg.getValue().length, reply.batch.length);

        // convert the msg to a propose message 
        MessageFactory messageFactory = new MessageFactory(msg.getSender());
        this.acceptor.deliver(messageFactory.createPropose(msg.getNumber(), msg.getEpoch(), reply.batch));
    }

    public void updatepackedheight() {
        this.multiChain.updatePackagedHeight();
    }

    public MZStripeMessage[] generateStripe(byte[] batch, Mz_Batch mzBatch){
        int N = controller.getCurrentViewN();
        int F = controller.getCurrentViewF();
        int myId = controller.getStaticConf().getProcessId();
        byte[][] stripeArray = mzbb.prepareByteArray(batch, N-F, F);
        ReedSolomon rs = ReedSolomon.create(N-F, F);
        long encodeStart = System.nanoTime();
        rs.encodeParity(stripeArray, 0, stripeArray[0].length);
        long encodeTime = System.nanoTime() - encodeStart;
        final int batchLen = batch.length;
        // logger.info("Node {} encode a batch {}, length {} Bytes, uses {} ns", myId, mzbatch, batchLen, encodeTime);
        MZStripeMessage[] msgArray = new MZStripeMessage[N];
        
        // create MZStripeMessage and broadcast to other nodes
        assert(stripeArray.length > 0);
        int dataLen = stripeArray[0].length, totalLen = 0;
        for(int stripeId = 0; stripeId < N; ++stripeId) {
            if (totalLen + stripeArray[stripeId].length > batchLen)
                dataLen = batchLen - totalLen;
            totalLen += dataLen;
            msgArray[stripeId] = new MZStripeMessage(myId, mzBatch.getNodeId(), mzBatch.getBatchId(), batchLen, stripeId, dataLen, stripeArray[stripeId]);
        } 
        return msgArray;
    }

    public void initPackBundleThread(){
        this.packbundlethread = new Thread() {
            @Override
            public void run() {
                long lastsend = System.currentTimeMillis();
                int myId = controller.getStaticConf().getProcessId();
                int useSignature = controller.getStaticConf().getUseSignatures();
                MessageFactory messageFactory = new MessageFactory(myId);
                // until there are requests to be packed.
                messagesLock.lock();
                if (clientsManager.havePendingRequests() == false) {
                    haveMessages.awaitUninterruptibly();
                }
                messagesLock.unlock();
                while (!Thread.interrupted()) {
                    long interval = System.currentTimeMillis() - lastsend;
                    // todo multiChain.getMyGeneratedHeight()!=-1
                    // 是否有必要？以及对于空batch只是用来更新的batchid是否需要递增，以及是否需要存？
                    if ((interval > 5 && clientsManager.havePendingRequests()) || (interval > 500
                            && multiChain.getUpdateTipState() && multiChain.getMyGeneratedHeight() != -1)) {
                        RequestList reqlist = clientsManager.getPendingRequests();
                        // 无论 reqlist 是否为空，都更新 batchheight
                        // 1. 这样保证每个 batch 都是唯一的，不应该存在两个相同高度的batch,如果允许的话，拜占庭节点可以在同一个高度制造很多个不同的batch
                        // 2. 每个节点收到一个 stripe 后根据 stripe 对应的 batchchain编号、batch 高度，stripe编号存储并解码 batch，
                        //     同一个高度的两个batch 会导致 stripe 解码失败
                        // if (!reqlist.isEmpty()) {
                        //     multiChain.updateMyGeneratedHeight();
                        // }
                        multiChain.updateMyGeneratedHeight();
                        multiChain.setUpdateTipState(false);
                        Map<Integer, Integer> chainPoolTip = multiChain.getMyChainPoolTip();
                        // chainPoolTip.put(myId, multiChain.getMyGeneratedHeight()); 多余的操作
                        Mz_Batch mzbatch = new Mz_Batch(myId, multiChain.getMyGeneratedHeight(), reqlist, chainPoolTip);
                        multiChain.add(mzbatch);
                        byte[] batch = mzbb.makeMzBatch(myId, multiChain.getMyGeneratedHeight(), reqlist, useSignature == 1, chainPoolTip);
                        if (controller.getStaticConf().getDataDisStrategy() == TOMUtil.DS_PREDIS_EC) {
                            // use erasure code to encode stripe array
                            int N = controller.getCurrentViewN();
                            int F = controller.getCurrentViewF();
                            byte[][] stripeArray = mzbb.prepareByteArray(batch, N-F, F);

                            // for debug
                            // for (int i = 0; i < stripeArray.length; ++i) {
                            //     System.out.printf("stripeArray[%d]'s len: %d ", i ,stripeArray[i].length);
                            // }
                            // System.out.printf(" batch len: %d \n", batch.length);

                            ReedSolomon rs = ReedSolomon.create(N-F, F);
                            long encodeStart = System.nanoTime();
                            rs.encodeParity(stripeArray, 0, stripeArray[0].length);
                            long encodeTime = System.nanoTime() - encodeStart;
                            final int batchLen = batch.length;
                            // logger.info("Node {} encode a batch {}, length {} Bytes, uses {} ns", myId, mzbatch, batchLen, encodeTime);
                            MZStripeMessage[] msgArray = new MZStripeMessage[N];
                            
                            // create MZStripeMessage and broadcast to other nodes
                            assert(stripeArray.length > 0);
                            int dataLen = stripeArray[0].length, totalLen = 0;
                            for(int stripeId = 0; stripeId < N; ++stripeId) {
                                if (totalLen + stripeArray[stripeId].length > batchLen)
                                    dataLen = batchLen - totalLen;
                                totalLen += dataLen;
                                msgArray[stripeId] = new MZStripeMessage(myId, myId, multiChain.getMyGeneratedHeight(), batchLen, stripeId, dataLen, stripeArray[stripeId]);
                                multiChain.addStripeMsg(msgArray[stripeId]);
                                if (stripeId == myId) 
                                    continue;
                                // logger.info("Node {} Send a stripe to node {}, stripe: {}", myId, stripeId, msgArray[stripeId].toString());
                                communication.send(new int[]{stripeId}, msgArray[stripeId]);
                            }  
                            // broadcast my stripe to other consensus nodes.
                            assert(myId >= 0 && myId < N);
                            // logger.info("Node {} broadcasts stripe {} to all nodes", myId, msgArray[myId].toString());
                            communication.send(controller.getCurrentViewAcceptors(), msgArray[myId]);
                            
                            // forward the data to my subscriber in NON_CONSENSUS networking mode
                            if (controller.getStaticConf().getNetworkingMode() != TOMUtil.NM_CONSENSUS) {
                                mzNodeMan.addForwardData(msgArray[myId]);
                            }
                        }
                        else {
                            ConsensusMessage batchMessage = messageFactory.createMzBatch(0, 0, batch);
                            // System.out.println("Stage: packbatch try to send --Nodeid: " + myId + " create Mzbatch height:"
                            //         + (multiChain.getMyGeneratedHeight()) + " batch size: " + reqlist.size() + " at time: "
                            //         + System.currentTimeMillis());
                            communication.send(controller.getCurrentViewAcceptors(), batchMessage);
                            // System.out.println("Stage: packbatch have sended --Nodeid: " + myId + " create Mzbatch height:"
                            //         + (multiChain.getMyGeneratedHeight()) + " batch size: " + reqlist.size() + " at time: "
                            //         + System.currentTimeMillis());

                            // logger.info("Stage: packbatch --Nodeid: " + myId + " create Mzbatch height:"
                            //         + (multiChain.getMyGeneratedHeight()) + " batch size: " + reqlist.size() + " at time: "
                            //         + System.currentTimeMillis());
                            logger.debug("Batch Request: {}", reqlist.toString());
                            lastsend = System.currentTimeMillis();
                            
                            // forward the data to my subscriber
                            // if (controller.getStaticConf().getNetworkingMode() != TOMUtil.NM_CONSENSUS) {
                            //     mzNodeMan.addForwardData(batchMessage);
                            // }
                        }
                    }
                }
            }
        };
    }

        /**
     * This is the main code for this thread. It basically waits until this
     * replica becomes the leader, and when so, proposes a value to the other
     * acceptors
     */
    public void startCreateBasicPropose() {
        logger.debug("Running."); // TODO: can't this be outside of the loop?
        while (doWork) {

            // blocks until this replica learns to be the leader for the current epoch of the current consensus
            leaderLock.lock();
            logger.debug("Next leader for CID=" + (getLastExec() + 1) + ": " + execManager.getCurrentLeader());

            //******* EDUARDO BEGIN **************//
            if (execManager.getCurrentLeader() != this.controller.getStaticConf().getProcessId()) {
                iAmLeader.awaitUninterruptibly();
                //waitForPaxosToFinish();
            }
            //******* EDUARDO END **************//
            leaderLock.unlock();
            
            if (!doWork) break;

            // blocks until the current consensus finishes
            proposeLock.lock();

            if (getInExec() != -1) { //there is some consensus running
                logger.debug("Waiting for consensus " + getInExec() + " termination.");
                canPropose.awaitUninterruptibly();
            }
            proposeLock.unlock();
            
            if (!doWork) break;

            logger.debug("I'm the leader.");

            // blocks until there are requests to be processed/ordered
            messagesLock.lock();
            if (!clientsManager.havePendingRequests()) {
                haveMessages.awaitUninterruptibly();
            }
            messagesLock.unlock();
            
            if (!doWork) break;
            
            logger.debug("There are messages to be ordered.");

            logger.debug("I can try to propose.");

            if ((execManager.getCurrentLeader() == this.controller.getStaticConf().getProcessId()) && //I'm the leader
                    (clientsManager.havePendingRequests()) && //there are messages to be ordered
                    (getInExec() == -1)) { //there is no consensus in execution

                // Sets the current consensus
                int execId = getLastExec() + 1;
                setInExec(execId);

                Decision dec = execManager.getConsensus(execId).getDecision();

                // Bypass protocol if service is not replicated
                if (controller.getCurrentViewN() == 1) {

                    logger.debug("Only one replica, bypassing consensus.");
                    
                    byte[] value = createPropose(dec);

                    Consensus consensus = execManager.getConsensus(dec.getConsensusId());
                    Epoch epoch = consensus.getEpoch(0, controller);
                    epoch.propValue = value;
                    epoch.propValueHash = computeHash(value);
                    epoch.getConsensus().addWritten(value);
                    epoch.deserializedPropValue = checkProposedValue(value, true);
                    epoch.getConsensus().getDecision().firstMessageProposed = epoch.deserializedPropValue[0];
                    dec.setDecisionEpoch(epoch);

                    //System.out.println("ESTOU AQUI!");
                    dt.delivery(dec);
                    continue;

                }
                execManager.getProposer().startConsensus(execId, createPropose(dec));
            }
        }
        logger.info("TOMLayer stopped.");
    }

    /*
     * Start to create predis propose
     */
    public void startCreatePredisPropose(){
        int myId = this.controller.getStaticConf().getProcessId();
        while (doWork) {

            // blocks until this replica learns to be the leader for the current epoch of
            // the current consensus
            leaderLock.lock();
            logger.debug("Next leader for CID=" + (getLastExec() + 1) + ": " + execManager.getCurrentLeader());

            // ******* EDUARDO BEGIN **************//
            if (execManager.getCurrentLeader() != this.controller.getStaticConf().getProcessId()) {
                iAmLeader.awaitUninterruptibly();
                // waitForPaxosToFinish();
            }
            // ******* EDUARDO END **************//
            leaderLock.unlock();

            if (!doWork)
                break;

            // blocks until the current consensus finishes
            proposeLock.lock();
            logger.debug("Node %d judge if there is some consensus running...\n", myId);
            if (getInExec() != -1) { // there is some consensus running
                logger.debug("Waiting for consensus " + getInExec() + " termination.");
                canPropose.awaitUninterruptibly();
            }
            proposeLock.unlock();

            if (!doWork)
                break;

            logger.debug("I'm the leader.");

            if (!doWork)
                break;

            logger.debug("I can try to propose.");

            if ((execManager.getCurrentLeader() == this.controller.getStaticConf().getProcessId()) && // I'm the leader
            // (clientsManager.havePendingRequests()) && //there are messages to be ordered
                    (getInExec() == -1)) { // there is no consensus in execution

                // Sets the current consensus
                int execId = getLastExec() + 1;
                setInExec(execId);

                Decision dec = execManager.getConsensus(execId).getDecision();

                // Bypass protocol if service is not replicated
                if (controller.getCurrentViewN() == 1) {

                    logger.debug("Only one replica, bypassing consensus.");

                    byte[] value = createMzPropose(dec);
                    if (value == null)
                        continue;
                    Consensus consensus = execManager.getConsensus(dec.getConsensusId());
                    Epoch epoch = consensus.getEpoch(0, controller);
                    epoch.propValue = value;
                    epoch.propValueHash = computeHash(value);
                    epoch.getConsensus().addWritten(value);
                    epoch.deserializedPropValue = checkProposedValue(value, true);
                    epoch.getConsensus().getDecision().firstMessageProposed = epoch.deserializedPropValue[0];
                    dec.setDecisionEpoch(epoch);

                    // System.out.println("ESTOU AQUI!");
                    dt.delivery(dec);
                    continue;

                }
                byte[] value = createMzPropose(dec);
                if (value != null) {
                    long now = System.currentTimeMillis();
                    logger.info("Leader {} create mzpropose at time {}, interval: {}ms", myId, now,
                            now - lastTimeCreatePropose);
                    lastTimeCreatePropose = now;
                    execManager.getProposer().startConsensus(execId, value);
                }
                // set In exec to -1
                else {
                    // setInExec(-1);
                    setNoExec();
                    // try{
                    // Thread.sleep(2000);
                    // }
                    // catch (Exception e){
                    // logger.error("Node: {}, opps, error: {}", myId, e.toString());
                    // }
                }
            }
        }
        logger.info("Node %d oops, I jump out of run function\n", myId);
        logger.info("TOMLayer stopped."); 
    }

    public void requestTimeoutMsg(){
        long now = System.currentTimeMillis();
        int myId = this.controller.getStaticConf().getProcessId();
        long timeout = 10000;
        byte[] dataHash = null;
        ArrayList<ArrayList<Integer>> removedKeySet = new ArrayList<>();
        Set<ArrayList<Integer>> msgSet = this.mzInflightMan.getMsgInFlight();
        for (ArrayList<Integer> arr: msgSet) {
            ArrayList<Long> value = this.mzInflightMan.getInflightInfo(arr);
            int sender = Integer.parseInt(String.valueOf(value.get(0)));
            long ts = value.get(1);
            if (now - ts > timeout || ts == 0) {
                int[] appendix = new int[arr.size()];
                int type = TOMUtil.DH_GETSTRIPE;
                if (arr.size() == 1) {
                    type = TOMUtil.DH_GETPREDISBLOCK;
                    if (this.mzblockchainMan.containsBlock(arr.get(0))) {
                        removedKeySet.add(arr);
                        logger.info("Node {} received block {}, do not need to request", myId, arr.get(0));
                        continue;
                    }
                }
                else if (arr.size() == 2) {
                    type = TOMUtil.DH_GETBUNDLE;
                    if (this.multiChain.getBundle(arr.get(0), arr.get(1)) != null) {
                        removedKeySet.add(arr);
                        logger.info("Node {} received bundle {}-{}, do not need to request", myId, arr.get(0), arr.get(1));
                        continue;
                    }
                }
                else if (arr.size() == 3) {
                    type = TOMUtil.DH_GETSTRIPE;
                    // we have received its batch, do not need to request
                    if (this.multiChain.getStripe(arr.get(0), arr.get(1), arr.get(2))!= null) {
                        removedKeySet.add(arr);
                        // logger.info("Node {} do not need to request stripe [{},{},{}], lastheight: {}", myId, arr.get(0), arr.get(1), arr.get(2), lastBatchHeight);
                        continue;
                    } 
                }
                else {
                    int newSender = this.mzInflightMan.getADownloadSource(arr);
                    if (newSender != -1)
                        sender = newSender;
                    for(int i = 0; i < arr.size(); ++i)
                        appendix[i] = arr.get(i);
                    DataHashMessage dh = new DataHashMessage(myId, type, now, appendix, dataHash);
                    this.communication.send(new int[]{sender}, dh);
                    value.set(1, now);
                    logger.info("requestTimeoutMsg node {} request [{}] from node {} again", myId, dh.toString(), sender);
                }

            }
        }
        for (ArrayList<Integer> ele: removedKeySet)
            this.mzInflightMan.removeMsgSet(ele);
    }

    /**
     * This function recovery block from a Mz_propose and forward the block to other nodes.
     */
    public void recoveryBlock() {
        int myId = this.controller.getStaticConf().getProcessId();
        int waitCnt = 2;
        Set<Integer> blockToRecoverySet = this.mzblockchainMan.getBlockNotRebuild();
        for(int blockheight: blockToRecoverySet) {
            MZBlock block = (MZBlock)(this.mzblockchainMan.getPredisBlock(blockheight));
            final boolean useSig = this.controller.getStaticConf().getUseSignatures()==1;
            Mz_Propose mzpropose = block.deseralizePropose(useSig);
            RebuildProposeState reply = rebuildPropose(mzpropose);
            final int digit = 16;
            final String blockHashStr = MZNodeMan.bytesToHex(block.getBlockHash(), digit);
            if (reply.getState() == false) {
                if (this.failToRecoveryCntMap.containsKey(mzpropose.blockHeight) == false)
                    this.failToRecoveryCntMap.put(mzpropose.blockHeight, 0);
                int cnt = this.failToRecoveryCntMap.get(mzpropose.blockHeight);
                this.failToRecoveryCntMap.put(mzpropose.blockHeight, cnt + 1);
                ArrayList<Long> senderTime = new ArrayList<>();
                senderTime.add(Long.valueOf(block.getSender()));
                senderTime.add(Long.valueOf(0));
                if (cnt == waitCnt) {
                    logger.info("Node {} cannot recovery block {}, fail_cnt: {}, will request from {}", myId, mzpropose.blockHeight, cnt, block.getSender());
                    for(int batchchainId: reply.missingBatchMap.keySet()) {
                        int startHeight = reply.missingBatchMap.get(batchchainId).get(0);
                        int endHeight = reply.missingBatchMap.get(batchchainId).get(1);
                        logger.info("Node {} cannot recovery [batchid: {},startHeight: {}, endHeight:{}], my tip: {}", myId, batchchainId, startHeight, endHeight, this.multiChain.getLastBatchHeight(batchchainId));
                        while (startHeight <= endHeight) {
                            int dsBundleToFullNodesMode = this.controller.getStaticConf().getDisBundleToFullNodes();
                            if (dsBundleToFullNodesMode == TOMUtil.DS_BUNDLE_EC) {
                                for(int i = 0; i < this.controller.getCurrentViewN(); ++i) {
                                    if (this.multiChain.getStripe(batchchainId, startHeight, i) == null) {
                                        ArrayList<Integer> appendix = new ArrayList<>(3);
                                        appendix.add(batchchainId);
                                        appendix.add(startHeight);
                                        appendix.add(i);
                                        this.mzInflightMan.addMsgInflight(appendix, senderTime);
                                        // logger.info("request stripe [{}] from {}", appendix, senderTime);
                                    }
                                }
                            }
                            else if (dsBundleToFullNodesMode == TOMUtil.DS_BUNDLE_FULL){
                                if (this.multiChain.getBundle(batchchainId, startHeight) == null){
                                    ArrayList<Integer> appendix = new ArrayList<>(2);
                                    appendix.add(batchchainId);
                                    appendix.add(startHeight);
                                    this.mzInflightMan.addMsgInflight(appendix, senderTime);
                                    // logger.info("request bundle [{}] from {}", appendix, senderTime);
                                }
                            }
                            ++startHeight;
                        }
                    }
                }
                break;
            }
            
            logger.debug("Node {} recovery a block {} from {}, propose:{}, batch size: {}, propose size {}", myId, blockHashStr, block.getSender(), mzpropose.toString(), reply.batch.length, block.getBlockContent().length);
            byte[] computedBlockHash = computeHash(reply.batch);
            final String computedBlockHashStr = MZNodeMan.bytesToHex(computedBlockHash, digit);
            if (Arrays.equals(computedBlockHash, block.getBlockHash())) {
                long now = System.currentTimeMillis();
                long waittingTime = now - this.mzblockchainMan.getBlockRecvTime(blockheight);
                logger.info("Node {} successfully recovery a new block {}, blockheight: {}, sender: {}, orignal tx size: {}, Mzblock size {}, use {} ms, waitting {} ms",
                    myId, blockHashStr, mzpropose.blockHeight, block.getSender(), reply.batch.length, block.getBlockContent().length, now - mzpropose.timestamp, waittingTime);
                
                
                // store the block
                this.mzblockchainMan.removePredisBlock(blockheight);
                this.mzblockchainMan.addSuccessfullyRecoveredBlock(block);
                this.failToRecoveryCntMap.remove(blockheight);
                // forward MZBlock when I can recovery the block
                MZBlock forwardBlock = new MZBlock(myId, block.getBlockHash(), block.getPropose(), block.getBlockContent());
                this.mzNodeMan.addForwardData(forwardBlock);
            }
            else {
                logger.info("Node {} failed recovery a new block {}, blockheight: {}, sender: {}, computedHash: {}",myId, blockHashStr, mzpropose.blockHeight, mzpropose.blockHeight, block.getSender(),computedBlockHashStr);
                break;
            }
        }
    }

    public void initMZNodeThread(){
        this.mznodeThread = new Thread() {
            @Override 
            public void run(){
                int myId = controller.getStaticConf().getProcessId();
                logger.info("Node {} starts MZNodeThread", myId);
                
                // ask relayNodes
                int networkMode = controller.getStaticConf().getNetworkingMode();
                if (networkMode == TOMUtil.NM_MULTI_ZONE && controller.isCurrentViewMember(myId) == false) {
                    mzNodeMan.askRelayNodes();
                    mzNodeMan.adjustFromReceivedRelayNodeMsgs();   
                }
                int ds = controller.getStaticConf().getDataDisStrategy();
                while(doWork) {
                    if (networkMode == TOMUtil.NM_MULTI_ZONE) {
                        // subscribe stripe 
                       
                        // if I am a relayer
                        mzNodeMan.boradcastRelayerAliveMsg();
                       
                        // if I am a consensus node.
                        // mzNodeMan.broadcastLatencyDetectMsg();
                        
                        mzNodeMan.broadcastHeartbeatMsg();

                        mzNodeMan.detectNodes();
                        // Todo change routing table

                        // Todo subscribe data from other nodes.
                    }
                    // a node needs to recovery block in non-orginal mode.
                    if (ds != TOMUtil.DS_ORIGINAL && controller.isInCurrentView()==false) {
                        // recovery block
                        recoveryBlock();   
                    }

                    if (networkMode != TOMUtil.NM_STAR && networkMode != TOMUtil.NM_CONSENSUS) {
                        requestTimeoutMsg();
                    }

                    // a node only forward data to other nodes in non-consensus mode.
                    if (networkMode != TOMUtil.NM_CONSENSUS) {
                        // forward message
                        mzNodeMan.ForwardDataToSubscriber();
                    }
                }
            logger.info("TOMLayer stopped.");
            }
        };      
    }
}
