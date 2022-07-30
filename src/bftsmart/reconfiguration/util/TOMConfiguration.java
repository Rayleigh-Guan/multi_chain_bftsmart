/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.reconfiguration.util;

import bftsmart.tom.util.KeyLoader;
import bftsmart.tom.util.TOMUtil;

import java.security.Provider;
import java.util.StringTokenizer;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TOMConfiguration extends Configuration {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    protected int n;
    protected int f;
    protected int requestTimeout;
    protected int tomPeriod;
    protected int paxosHighMark;
    protected int revivalHighMark;
    protected int timeoutHighMark;
    protected int replyVerificationTime;
    protected int maxBatchSize;
    protected int numberOfNonces;
    protected int inQueueSize;
    protected int outQueueSize;
    protected boolean shutdownHookEnabled;
    protected boolean useSenderThread;
    private int numNIOThreads;
    private int useMACs;
    private int useSignatures;
    private boolean stateTransferEnabled;
    private int checkpointPeriod;
    private int globalCheckpointPeriod;
    private int useControlFlow;
    private int[] initialView;
    private int ttpId;
    private boolean isToLog;
    private boolean syncLog;
    private boolean parallelLog;
    private boolean logToDisk;
    private boolean isToWriteCkpsToDisk;
    private boolean syncCkp;
    private boolean isBFT;
    private int numRepliers;
    private int numNettyWorkers;
    private boolean sameBatchSize;
    private String bindAddress;

    private int dataDisStrategy;                // data distribution strategy
    private int networkmode;                    // networkmode mode
    private int nMaximumSubscriber;             // maximum neighbors in multi_zone or random networking mode
    private int heartbeatInterval;              // heartbeat interval(milliseconds) to notify my existence.
    private int nRandomNeighbor;                // average node to connect in random network topology
    private int minConWithConseususNode;
    private int minConWithRelayerNode;

    private int fabricTTLDirect;
    private int fabricTTL;
    private int fabricFout;

    private int dsInRandommode;

    private String dataDisStrategyStr;
    private String networkingModeStr;
    private String dsInRandommodeStr;

    private int dsBundleToFullNodesMode;
    private String dsBundleToFullNodesModeStr;
    
    /** Creates a new instance of TOMConfiguration */
    public TOMConfiguration(int processId, KeyLoader loader) {
        super(processId, loader);
    }

    /** Creates a new instance of TOMConfiguration */
    public TOMConfiguration(int processId, String configHome, KeyLoader loader) {
        super(processId, configHome, loader);
    }

    @Override
    protected void init() {
        super.init();
        try {
            n = Integer.parseInt(configs.remove("system.servers.num").toString());
            String s = (String) configs.remove("system.servers.f");
            if (s == null) {
                f = (int) Math.ceil((n - 1) / 3);
            } else {
                f = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.shutdownhook");
            shutdownHookEnabled = (s != null) ? Boolean.parseBoolean(s) : false;

            s = (String) configs.remove("system.totalordermulticast.period");
            if (s == null) {
                tomPeriod = n * 5;
            } else {
                tomPeriod = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.totalordermulticast.timeout");
            if (s == null) {
                requestTimeout = 10000;
            } else {
                requestTimeout = Integer.parseInt(s);
                if (requestTimeout < 0) {
                    requestTimeout = 0;
                }
            }

            s = (String) configs.remove("system.totalordermulticast.highMark");
            if (s == null) {
                paxosHighMark = 10000;
            } else {
                paxosHighMark = Integer.parseInt(s);
                if (paxosHighMark < 10) {
                    paxosHighMark = 10;
                }
            }

            s = (String) configs.remove("system.totalordermulticast.revival_highMark");
            if (s == null) {
                revivalHighMark = 10;
            } else {
                revivalHighMark = Integer.parseInt(s);
                if (revivalHighMark < 1) {
                    revivalHighMark = 1;
                }
            }

            s = (String) configs.remove("system.totalordermulticast.timeout_highMark");
            if (s == null) {
                timeoutHighMark = 100;
            } else {
                timeoutHighMark = Integer.parseInt(s);
                if (timeoutHighMark < 1) {
                    timeoutHighMark = 1;
                }
            }

            s = (String) configs.remove("system.totalordermulticast.maxbatchsize");
            if (s == null) {
                maxBatchSize = 100;
            } else {
                maxBatchSize = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.totalordermulticast.replayVerificationTime");
            if (s == null) {
                replyVerificationTime = 0;
            } else {
                replyVerificationTime = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.totalordermulticast.nonces");
            if (s == null) {
                numberOfNonces = 0;
            } else {
                numberOfNonces = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.communication.useSenderThread");
            if (s == null) {
                useSenderThread = false;
            } else {
                useSenderThread = Boolean.parseBoolean(s);
            }

            s = (String) configs.remove("system.communication.numNIOThreads");
            if (s == null) {
                numNIOThreads = 2;
            } else {
                numNIOThreads = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.communication.useMACs");
            if (s == null) {
                useMACs = 0;
            } else {
                useMACs = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.communication.useSignatures");
            if (s == null) {
                useSignatures = 0;
            } else {
                useSignatures = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.totalordermulticast.state_transfer");
            if (s == null) {
                stateTransferEnabled = false;
            } else {
                stateTransferEnabled = Boolean.parseBoolean(s);
            }

            s = (String) configs.remove("system.totalordermulticast.checkpoint_period");
            if (s == null) {
                checkpointPeriod = 1;
            } else {
                checkpointPeriod = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.communication.useControlFlow");
            if (s == null) {
                useControlFlow = 0;
            } else {
                useControlFlow = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.initial.view");
            if (s == null) {
                initialView = new int[n];
                for (int i = 0; i < n; i++) {
                    initialView[i] = i;
                }
            } else {
                StringTokenizer str = new StringTokenizer(s, ",");
                initialView = new int[str.countTokens()];
                for (int i = 0; i < initialView.length; i++) {
                    initialView[i] = Integer.parseInt(str.nextToken());
                }
            }

            s = (String) configs.remove("system.ttp.id");
            if (s == null) {
                ttpId = -1;
            } else {
                ttpId = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.communication.inQueueSize");
            if (s == null) {
                inQueueSize = 1000;
            } else {

                inQueueSize = Integer.parseInt(s);
                if (inQueueSize < 1) {
                    inQueueSize = 1000;
                }

            }

            s = (String) configs.remove("system.communication.outQueueSize");
            if (s == null) {
                outQueueSize = 1000;
            } else {
                outQueueSize = Integer.parseInt(s);
                if (outQueueSize < 1) {
                    outQueueSize = 1000;
                }
            }

            s = (String) configs.remove("system.totalordermulticast.log");
            if (s != null) {
                isToLog = Boolean.parseBoolean(s);
            } else {
                isToLog = false;
            }

            s = (String) configs
                    .remove("system.totalordermulticast.log_parallel");
            if (s != null) {
                parallelLog = Boolean.parseBoolean(s);
            } else {
                parallelLog = false;
            }

            s = (String) configs
                    .remove("system.totalordermulticast.log_to_disk");
            if (s != null) {
                logToDisk = Boolean.parseBoolean(s);
            } else {
                logToDisk = false;
            }

            s = (String) configs
                    .remove("system.totalordermulticast.sync_log");
            if (s != null) {
                syncLog = Boolean.parseBoolean(s);
            } else {
                syncLog = false;
            }

            s = (String) configs
                    .remove("system.totalordermulticast.checkpoint_to_disk");
            if (s == null) {
                isToWriteCkpsToDisk = false;
            } else {
                isToWriteCkpsToDisk = Boolean.parseBoolean(s);
            }

            s = (String) configs
                    .remove("system.totalordermulticast.sync_ckp");
            if (s == null) {
                syncCkp = false;
            } else {
                syncCkp = Boolean.parseBoolean(s);
            }

            s = (String) configs.remove("system.totalordermulticast.global_checkpoint_period");
            if (s == null) {
                globalCheckpointPeriod = 1;
            } else {
                globalCheckpointPeriod = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.bft");
            isBFT = (s != null) ? Boolean.parseBoolean(s) : true;

            s = (String) configs.remove("system.numrepliers");
            if (s == null) {
                numRepliers = 0;
            } else {
                numRepliers = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.numnettyworkers");
            if (s == null) {
                numNettyWorkers = 0;
            } else {
                numNettyWorkers = Integer.parseInt(s);
            }

            s = (String) configs.remove("system.communication.bindaddress");

            Pattern pattern = Pattern
                    .compile("^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

            if (s == null || !pattern.matcher(s).matches()) {
                bindAddress = "";
            } else {
                bindAddress = s;
            }

            s = (String) configs.remove("system.samebatchsize");
            if (s != null) {
                sameBatchSize = Boolean.parseBoolean(s);
            } else {
                sameBatchSize = false;
            }
            
            // configure parameters for multi-zone
            s = (String) configs.remove("system.qc");
            if (s == null){
                minConWithConseususNode = Integer.parseInt(s);
            }
            else {
                minConWithConseususNode = n;
            }

            s = (String) configs.remove("system.qr");
            if (s == null){
                minConWithRelayerNode = Integer.parseInt(s);
            }
            else {
                minConWithRelayerNode = n;
            }
            
            s = (String) configs.remove("system.ds");
            // System.out.println("parsed ds: "+s);
            if (s == null) {
                dataDisStrategy = TOMUtil.DS_ORIGINAL;
                dataDisStrategyStr = "ORGINAL";
            }
            else if (s.equals("predis_ec")) {
                dataDisStrategy = TOMUtil.DS_PREDIS_EC;
                dataDisStrategyStr = "PREDIS_EC";
            }
            else if (s.equals("predis_full")) {
                dataDisStrategy = TOMUtil.DS_PREDIS_FULL;
                dataDisStrategyStr = "PREDIS_FULL";
            }

            s = (String) configs.remove("system.networkmode");
            // System.out.println("parsed networkmode: "+s);
            if (s == null || s.equals("consensus")) {
                networkmode = TOMUtil.NM_CONSENSUS;
                networkingModeStr = "CONSENSUS";
            }
            else if (s.equals("multi_zone") ) {
                networkmode = TOMUtil.NM_MULTI_ZONE;
                networkingModeStr = "MULTI_ZONE";
            } 
            else if (s.equals("random")) {
                networkmode = TOMUtil.NM_RANDOM;
                networkingModeStr = "RANDOM";
            } 
            else if (s.equals("star")) {
                networkmode = TOMUtil.NM_STAR;
                networkingModeStr = "STAR";
            }
            
            s = (String) configs.remove("system.ds_bundle");
            if (s == null || s.equals("erasure_coding")) {
                dsBundleToFullNodesMode = TOMUtil.DS_BUNDLE_EC;
                dsBundleToFullNodesModeStr = "erasure_coding";
            }
            else {
                dsBundleToFullNodesMode = TOMUtil.DS_BUNDLE_FULL;
                dsBundleToFullNodesModeStr = "full";
            }
            
            s = (String) configs.remove("system.ds_in_random");
            // System.out.println("parsed ds_in_random: "+s);
            if (s == null) {
                dsInRandommode = TOMUtil.DS_RANDOM_ENHANCED_FAB;
                dsInRandommodeStr = "DS_RANDOM_ENHANCED_FAB";
            }
            else if (s.equals("enchanced_fabric") ) {
                dsInRandommode = TOMUtil.DS_RANDOM_ENHANCED_FAB;
                dsInRandommodeStr = "DS_RANDOM_ENHANCED_FAB";
            } 
            else if (s.equals("BTC")) {
                dsInRandommode = TOMUtil.DS_RANDOM_BTC;
                dsInRandommodeStr = "DS_RANDOM_BTC";
            } 
            else if (s.equals("ETH")) {
                dsInRandommode = TOMUtil.DS_RANDOM_ETH;
                dsInRandommodeStr = "DS_RANDOM_ETH";
            }

            s = (String) configs.remove("system.maximumSubscriber");
            if (s == null)
                nMaximumSubscriber = 24;
            else
                nMaximumSubscriber = Integer.parseInt(s);
            
            s = (String) configs.remove("System.heartbeatInterval");
            if (s == null)
                heartbeatInterval = 30000;
            else
                heartbeatInterval = Integer.parseInt(s);

            s = (String) configs.remove("system.randoNneighbor");
            if (s == null)
                nRandomNeighbor = 12;
            else
                nRandomNeighbor  = Integer.parseInt(s);
            
            s = (String) configs.remove("system.fabricTTLDirect");
            if (s == null)
                fabricTTLDirect = 2;
            else
                fabricTTLDirect = Integer.parseInt(s);

            s = (String) configs.remove("system.fabricTTL");
            if (s == null)
                fabricTTL = 9;
            else
                fabricTTL = Integer.parseInt(s);

            s = (String) configs.remove("system.fabricFout");
            if (s == null)
                fabricFout = 4;
            else
                fabricFout = Integer.parseInt(s);

            logger = LoggerFactory.getLogger(this.getClass());
            logger.info("Qc(Quorum connections to consensus node) = " + minConWithConseususNode);
            logger.info("Qr(Quorum connections to relay node) = " + minConWithRelayerNode );
            logger.info("Consensus data distribution strategy is "+ dataDisStrategyStr);
            logger.info("Networking mode is " + networkingModeStr);
            logger.info("Data dissemination strategy is " + dsInRandommodeStr);
            logger.info("Data distribute to full nodes is " + dsBundleToFullNodesModeStr);
            logger.info("Maximum subscriber is "+ nMaximumSubscriber);
            logger.info("Random connect neighbor is " + nRandomNeighbor);
            logger.info("Initialview: {}", initialView);
            
        } catch (Exception e) {
            logger.error("Could not parse system configuration file", e);
        }

    }

    public String getViewStoreClass() {
        String s = (String) configs.remove("view.storage.handler");
        if (s == null) {
            return "bftsmart.reconfiguration.views.DefaultViewStorage";
        } else {
            return s;
        }

    }

    public boolean isTheTTP() {
        return (this.getTTPId() == this.getProcessId());
    }

    public final int[] getInitialView() {
        return this.initialView;
    }

    public int getTTPId() {
        return ttpId;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public int getReplyVerificationTime() {
        return replyVerificationTime;
    }

    public int getN() {
        return n;
    }

    public int getF() {
        return f;
    }

    public int getPaxosHighMark() {
        return paxosHighMark;
    }

    public int getRevivalHighMark() {
        return revivalHighMark;
    }

    public int getTimeoutHighMark() {
        return timeoutHighMark;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public boolean isShutdownHookEnabled() {
        return shutdownHookEnabled;
    }

    public boolean isStateTransferEnabled() {
        return stateTransferEnabled;
    }

    public int getInQueueSize() {
        return inQueueSize;
    }

    public int getOutQueueSize() {
        return outQueueSize;
    }

    public boolean isUseSenderThread() {
        return useSenderThread;
    }

    /**
     * *
     */
    public int getNumberOfNIOThreads() {
        return numNIOThreads;
    }

    /** * @return the numberOfNonces */
    public int getNumberOfNonces() {
        return numberOfNonces;
    }

    /**
     * Indicates if signatures should be used (1) or not (0) to authenticate client
     * requests
     */
    public int getUseSignatures() {
        return useSignatures;
    }

    /**
     * Indicates if MACs should be used (1) or not (0) to authenticate client-server
     * and server-server messages
     */
    public int getUseMACs() {
        return useMACs;
    }

    /**
     * Indicates the checkpoint period used when fetching the state from the
     * application
     */
    public int getCheckpointPeriod() {
        return checkpointPeriod;
    }

    public boolean isToWriteCkpsToDisk() {
        return isToWriteCkpsToDisk;
    }

    public boolean isToWriteSyncCkp() {
        return syncCkp;
    }

    public boolean isToLog() {
        return isToLog;
    }

    public boolean isToWriteSyncLog() {
        return syncLog;
    }

    public boolean logToDisk() {
        return logToDisk;
    }

    public boolean isToLogParallel() {
        // TODO Auto-generated method stub
        return parallelLog;
    }

    /**
     * Indicates the checkpoint period used when fetching the state from the
     * application
     */
    public int getGlobalCheckpointPeriod() {
        return globalCheckpointPeriod;
    }

    /**
     * Indicates if a simple control flow mechanism should be used to avoid an
     * overflow of client requests
     */
    public int getUseControlFlow() {
        return useControlFlow;
    }

    public boolean isBFT() {

        return this.isBFT;
    }

    public int getNumRepliers() {
        return numRepliers;
    }

    public int getNumNettyWorkers() {
        return numNettyWorkers;
    }

    public boolean getSameBatchSize() {
        return sameBatchSize;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public int getDataDisStrategy() {
        return dataDisStrategy;
    }

    public int getMinConWithConsensusNode(){
        return minConWithConseususNode;
    }

    public int getMinConWithRelayerNode(){
        return minConWithRelayerNode;
    }

    public int getMaximumSubscriber(){
        return nMaximumSubscriber;
    }

    public int getHeartbeatInterval(){
        return heartbeatInterval;
    }

    public int getNRandomNeighbor(){
        return nRandomNeighbor;
    }

    public int getNetworkingMode(){
        return networkmode;
    }

    public int getDataDisForRandom(){
        return dsInRandommode;
    }

    public String getDataDisForRandomStr(){
        return dsInRandommodeStr;
    }

    public String getNetworkmodeStr(){
        return networkingModeStr;
    }

    public int getDisBundleToFullNodes(){
        return dsBundleToFullNodesMode;
    }

    public String getDisBundleToFullNodesStr(){
        return dsBundleToFullNodesModeStr;
    }

    public int getFabricTTLDirect(){
        return fabricTTLDirect;
    }

    public int getFabricTTL(){
        return fabricTTL;
    }

    public int getFabricFout(){
        return fabricFout;
    }
}
