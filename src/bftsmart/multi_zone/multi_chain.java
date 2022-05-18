package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;
import bftsmart.tom.core.messages.TOMMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.lang.Math;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicBoolean;

public class multi_chain {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private int replica_num=4;

    private List<Mz_Batch>[] ChainPool = new ArrayList[this.replica_num];
    private int[] nTxArray;
    private int[] PackagedHeight=new int[this.replica_num];
    private AtomicBoolean multicastTip;
    private int MyGeneratedHeight;
    private int NodeID;
    private int NPackedTx;
    private ReentrantLock mzlock = new ReentrantLock();
    private List<Mz_BatchListItem> lastbatchlist=new ArrayList<>();

    public multi_chain(Integer nodeid)
    {
        multicastTip = new AtomicBoolean(false);
        nTxArray = new int[this.replica_num];
        for (int i = 0; i < this.replica_num; i++) {
            this.ChainPool[i]=new ArrayList<>();
            this.PackagedHeight[i] = -1;
        }
        this.NodeID = nodeid;
        this.MyGeneratedHeight = -1;
        NPackedTx = 0;
    }

    public void setUpdateTipState(boolean b) {
        multicastTip.set(b);
    }

    public boolean getUpdateTipState() {
        return multicastTip.get();
    }

    public void add(Mz_Batch value){
        this.mzlock.lock();
        int nodeid = value.NodeId;
        if (!value.Req.isEmpty()){
            this.ChainPool[nodeid].add(value);
        }
        // received a new batch, we need to update our tip.
        if (nodeid != this.NodeID && !value.Req.isEmpty()) {
            setUpdateTipState(true);
        }
        nTxArray[nodeid] += value.Req.size();
        logger.info("--Nodeid: {}, received Batch from {}, batchId: {}, request num:{}, current batch len: {}, tipArray: {}, total received Tx: {}, should I update my Tip: {}", 
            this.NodeID, nodeid, value.BatchId, value.Req.size(), this.ChainPool[nodeid].size(), value.chainPooltip.toString(), nTxArray[nodeid], getUpdateTipState());
        logger.debug("--Nodeid: {}, received Batch from {}, requests: {}",  this.NodeID, nodeid, value.BatchId, value.Req);
        this.mzlock.unlock();
    }


    public List<Mz_BatchListItem> packList(){
        this.mzlock.lock();
        List<Mz_BatchListItem> list=new ArrayList<>();
        for (int i=0;i<this.replica_num;i++)
        {
            if (this.ChainPool[i].size()==0)
                continue;
            int batchtip=this.ChainPool[i].get(this.ChainPool[i].size()-1).BatchId;
            int hi=this.PackagedHeight[i];
            if (batchtip>hi)
            {
                if (batchtip>hi+6){
                    int ed = hi+1;
                    if (batchtip > hi+25)
                        ed = Math.max(ed, batchtip-25);
                    Mz_BatchListItem temp=new Mz_BatchListItem(i,hi+1,ed,1);
                    list.add(temp);
                }else
                {
                    Mz_BatchListItem temp=new Mz_BatchListItem(i,hi+1,hi+1,0);
                    list.add(temp);
                }
            }
        }
        this.mzlock.unlock();
//        logger.debug("Node try to packlist:"+list.toString());
        return list;
    }

    public List<Mz_BatchListItem> packListWithTip(){
        List<Map<Integer,Integer>> chainTipArray = getReplicaChainPoolTip();
        logger.debug("Stage packListWithTip: --start time: {}", System.currentTimeMillis());
        this.mzlock.lock();
        List<Mz_BatchListItem> list = new ArrayList<>();
        // int quorum = (replica_num/3)*2;
        int quorum = replica_num;
        for (int i=0; i<this.replica_num; i++)
        {
            int batchLen = this.ChainPool[i].size();
            if (batchLen <= 0)    continue;
            // find slowest node for a batch chain
            int startHeight = this.PackagedHeight[i]+1;
            int localTip = this.ChainPool[i].get(batchLen-1).BatchId;
            int endHeight = localTip;
            // Leader node skip to pack a list it falls behind
            int useBatchhash = 0;
            if (endHeight >= startHeight) {
                int nNotFallBehind = 0;
                for (Map<Integer, Integer> integerIntegerMap : chainTipArray) {
                    if (!integerIntegerMap.containsKey(i)) continue;
                    int tmpTip = integerIntegerMap.get(i);
                    if (tmpTip < startHeight) continue;
                    ++nNotFallBehind;
                    if (tmpTip < endHeight) endHeight = tmpTip;
                }
                // at least 2/3 replicas can catch up.
                if (nNotFallBehind >= quorum)
                    useBatchhash = 1;
                else
                    endHeight = startHeight;
                Mz_BatchListItem temp = new Mz_BatchListItem(i, startHeight, endHeight, useBatchhash);
                logger.info("Node {} packListWithTip for chain {} , startHeight: {}, endHeight:{}, tip: {}, usebatch: {}", NodeID, i, startHeight, endHeight, localTip, useBatchhash);
                list.add(temp);
            }
        }
        for (int i=0; i<this.replica_num; i++)
            logger.info("Node {}'s tip Array: {}", i, i, chainTipArray.get(i).toString());
        
        this.mzlock.unlock();
        // check the block before returned
        checkBlock(list); //这是由于空batch所带来的弊端
        logger.debug("Stage packListWithTip: --end time: "+System.currentTimeMillis());
        return list;
    }

    public void checkBlock(List<Mz_BatchListItem> list) {
        // check if the number of txes is zero
        int nRequests = 0;
        for(Mz_BatchListItem item: list) {
            for (int i = item.StartHeight; i <= item.EndHeight; ++i) {
                nRequests += ChainPool[item.NodeId].get(i).Req.size();
            }
        }
        if (nRequests == 0) {
            list.clear();
            logger.debug("There are no TXes in block, clear the list, list size: {}\n", list.size());
        }
            
    }

    public List<Map<Integer,Integer>> getReplicaChainPoolTip(){
        this.mzlock.lock();
        List<Map<Integer,Integer>> chainTipArray = new ArrayList<Map<Integer,Integer>>();
        for (int i = 0; i < this.replica_num; i++) {
            int len = this.ChainPool[i].size();
            if (len <= 0)   
                chainTipArray.add(new HashMap<Integer,Integer>());
            else
                chainTipArray.add(ChainPool[i].get(len-1).chainPooltip);
        }
        this.mzlock.unlock();
        // update newest chainTip for myself.
        // todo 是否有必要？感觉这一步很多余，之前都在锁里
        chainTipArray.set(this.NodeID, getMyChainPoolTip());
        return chainTipArray;
    }


    public Map<Integer,Integer> getMyChainPoolTip() {
        this.mzlock.lock();
        Map<Integer,Integer> chainPoolTip = new HashMap<Integer,Integer>();
        for (int i = 0; i < this.replica_num; i++) {
            int len = this.ChainPool[i].size();
            if (len <= 0) {
                chainPoolTip.put(i, -1);
            }
            else {
                int tipHeight = ChainPool[i].get(len-1).BatchId;
                chainPoolTip.put(i, tipHeight);
            }

        }
        this.mzlock.unlock();
        return chainPoolTip;
    }

    // TODO: to fix IndexOutOfBoundsException in line 198
    public getsync_reply getsyncedRequestfromlist(List<Mz_BatchListItem> rev){

        getsync_reply reply=new getsync_reply();
        logger.debug("Stage: getsyncedRequestfromlist try to getRequestfromlist");
        this.mzlock.lock();
        for (Mz_BatchListItem mz_batchListItem : rev) {
            int st = mz_batchListItem.StartHeight;
            int ed = mz_batchListItem.EndHeight;
            int nd = mz_batchListItem.NodeId;
            int uf = mz_batchListItem.usedful;
            logger.debug("Stage: getsyncedRequestfromlist --Nodeid :"+nd+" StartHeight: "+st+" EndHeight: "+ed+" usebatch "+uf);
            if (uf == 0)
                continue;
            if (ed>this.ChainPool[nd].get(this.ChainPool[nd].size()-1).BatchId)
            {
                reply.setOk(false);
                logger.debug("Stage: getsyncedRequestfromlist --ed>len --ed:"+ed+" --len: "+this.ChainPool[nd].get(this.ChainPool[nd].size()-1).BatchId);
                return reply;
            }
            for (int j = st; j <= ed; j++) {
                reply.list.addAll(this.ChainPool[nd].get(j).Req);
                logger.debug("Stage: getsyncedRequestfromlist --Height: "+j+" req: "+this.ChainPool[nd].get(j).Req);
            }
        }
        this.mzlock.unlock();
        // logger.debug("Stage: getsyncedRequestfromlist --totoal reqlist"+reqlist);
        logger.debug("Stage: getsyncedRequestfromlist --total syncedRequesrequest size: {}, request: {}", reply.getlist().size(), reply.getlist().toString());
        NPackedTx += reply.list.size();
        if (!reply.list.isEmpty()) {
            logger.info("Node {} packed {} batched request, NPackedTx: {}", NodeID, reply.list.size(), NPackedTx);
        }
        return reply;
    }

    public RequestList getnotsyncRequestfromlist(List<Mz_BatchListItem> rev){

        RequestList reqlist=new RequestList();
        logger.debug("Stage: getnotsyncRequestfromlist try to getnotsyncRequestfromlist");
        this.mzlock.lock();
        for (Mz_BatchListItem mz_batchListItem : rev) {
            int st = mz_batchListItem.StartHeight;
            int ed = mz_batchListItem.EndHeight;
            int nd = mz_batchListItem.NodeId;
            int uf = mz_batchListItem.usedful;
            logger.debug("Stage: getnotsyncRequestfromlist --Nodeid: "+nd+" StartHeight: "+st+" EndHeight: "+ed+" usebatch "+uf);
            if (uf == 1)
                continue;
            for (int j = st; j <= ed; j++) {
                reqlist.addAll(this.ChainPool[nd].get(j).Req);
                logger.debug("Stage: getnotsyncRequestfromlist --Height: "+j+" req: "+this.ChainPool[nd].get(j).Req);
            }
        }
        this.mzlock.unlock();
        // logger.debug("Stage: getnotsyncRequestfromlist --totoal reqlist"+reqlist);
        logger.debug("Stage: getnotsyncRequestfromlist --total notsyncRequest size: %d, request: %s\n", reqlist.size(), reqlist.toString());
        NPackedTx += reqlist.size();
        if (!reqlist.isEmpty()) {
            logger.info("Node {} packed {} full requests ,NPackedTx: {}", NodeID, reqlist.size(), NPackedTx);
        }
        return reqlist;
    }

    public void updatePackagedHeight(){
        this.mzlock.lock();
        for (Mz_BatchListItem mz_batchListItem : this.lastbatchlist) {
            int nodeid_temp = mz_batchListItem.NodeId;
            if (mz_batchListItem.EndHeight > this.PackagedHeight[nodeid_temp])
                this.PackagedHeight[nodeid_temp] = mz_batchListItem.EndHeight;
        }
        this.mzlock.unlock();
    }

    public int getMyGeneratedHeight(){
        return this.MyGeneratedHeight;
    }
    public void updateMyGeneratedHeight(){
        this.mzlock.lock();
        this.MyGeneratedHeight += 1;
        this.mzlock.unlock();
    }
    public TOMMessage getFirstRequest(List<Mz_BatchListItem> list){
        for(Mz_BatchListItem item: list) {
            for (int i = item.StartHeight; i <= item.EndHeight; ++i) {
                if (ChainPool[item.NodeId].get(i).Req.size() > 0)
                    return ChainPool[item.NodeId].get(i).Req.getFirst();
                
            }
        }
        return null;
    }

    public void setLastbatchlist(List<Mz_BatchListItem> lastbatchlist) {
        this.lastbatchlist = lastbatchlist;
    }
}
