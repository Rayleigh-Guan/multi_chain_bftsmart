package bftsmart.multi_zone;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.ArrayList;

import bftsmart.communication.SystemMessage;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MZBlockchainMan {
    private Map<Integer, SystemMessage> blockchain;
    public Map<Integer, SystemMessage> mzBlockMap;
    public Map<Integer, Long> mzBlockRecvTime;
    private ReentrantLock blockLock;
    private Logger logger;                                

    public MZBlockchainMan(){
        this.mzBlockMap = new TreeMap<Integer, SystemMessage>();
        this.mzBlockRecvTime = new HashMap<Integer, Long>();
        this.blockchain = new TreeMap<Integer, SystemMessage>();
        this.blockLock = new ReentrantLock();
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    public boolean addPredisBlock(MZBlock block) {
        int blockHeight = block.getPropose().blockHeight;
        boolean res = false;
        if (this.containsBlock(blockHeight) == false) {
            this.blockLock.lock();
            this.mzBlockMap.put(blockHeight, block);
            this.mzBlockRecvTime.put(blockHeight, System.currentTimeMillis());
            res = true;
            this.blockLock.unlock();
        }
        return res;
    }

    public boolean addValidBlock(SystemMessage block, int blockHeight) {
        boolean res = false;
        this.blockLock.lock();
        if (this.blockchain.containsKey(blockHeight) == false) {
            long now = System.currentTimeMillis();
            this.blockchain.put(blockHeight, block);
            this.mzBlockRecvTime.put(blockHeight, now);
            res = true;
        }
        this.blockLock.unlock();
        return res;
    }

    public MZBlock getPredisBlock(int heihgt) {
        MZBlock block = null;
        this.blockLock.lock();
        if (this.containsBlock(heihgt)) {
            MZBlock preBlock = (MZBlock)this.mzBlockMap.get(heihgt);
            if (preBlock == null)
                preBlock = (MZBlock)this.blockchain.get(heihgt);
            block = preBlock.clone();
        }
        this.blockLock.unlock();
        return block;
    } 

    public void removePredisBlock(int height) {
        this.blockLock.lock();
        this.mzBlockMap.remove(height);
        this.blockLock.unlock();
    }

    public void removePredisBlock(ArrayList<Integer> heightSet) {
        this.blockLock.lock();
        for(int height: heightSet)
            this.mzBlockMap.remove(height);
        this.blockLock.unlock();
    }

    public void addSuccessfullyRecoveredBlock(MZBlock block) {
        int blockHeight = block.getPropose().blockHeight;
        this.blockchain.put(blockHeight, block);
    }

   
    public boolean containsBlock(int height) {
        this.blockLock.lock();
        boolean res = this.mzBlockMap.containsKey(height) || this.blockchain.containsKey(height);
        this.blockLock.unlock();
        return res;
    }

    public long getBlockRecvTime(int height) {
        long recvTime = 0;
        this.blockLock.lock();
        if (this.containsBlock(height))
            recvTime = this.mzBlockRecvTime.get(height);
        this.blockLock.unlock();
        return recvTime;
    }

    public Set<Integer> getBlockNotRebuild(){
        Set<Integer> blockHeightSet = new TreeSet<>();
        this.blockLock.lock();
        blockHeightSet.addAll(this.mzBlockMap.keySet());
        this.blockLock.unlock();
        return blockHeightSet;
    }
}
