package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;
import bftsmart.tom.core.messages.TOMMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.lang.Math;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
public class multi_chain {
    private int replica_num=4;

    private List<Mz_Batch>[] ChainPool = new ArrayList[this.replica_num];
    private int[] nTxArray;
    private int[] PackagedHeight=new int[this.replica_num];

    private int MyGeneratedHeight;
    private int NodeID;
    private int NPackedTx;
    private ReentrantLock mzlock = new ReentrantLock();
    private List<Mz_BatchListItem> lastbatchlist=new ArrayList<>();

    public multi_chain(Integer nodeid)
    {
        nTxArray = new int[this.replica_num];
        for (int i = 0; i < this.replica_num; i++) {
            this.ChainPool[i]=new ArrayList<>();
            this.PackagedHeight[i] = -1;
        }
        this.NodeID = nodeid;
        this.MyGeneratedHeight = -1;
        NPackedTx = 0;
    }

    public void add(Mz_Batch value){
        this.mzlock.lock();
        int nodeid = value.NodeId;
        this.ChainPool[nodeid].add(value);
//        if (nodeid==this.NodeID)
//            this.MyGeneratedHeight=this.MyGeneratedHeight+1;
        nTxArray[nodeid] += value.Req.size();
        System.out.printf("Stage: addbatch --Nodeid: %d, received Batch: %d from %d, len: %d, req: %s, tipArray: %s, total received Tx: %d\n", 
            this.NodeID, value.BatchId, nodeid, this.ChainPool[nodeid].size(), value.Req.toString(),value.chainPooltip.toString(), nTxArray[nodeid]);
        // System.out.println("Stage: addbatch --Nodeid: "+this.NodeID+" received Batch "+value.BatchId+" from "+value.NodeId+" --batch:"+value.Req+" --len"+this.ChainPool[nodeid].size()+" tipArray: "+value.chainPooltip.toString());
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
//        System.out.println("Node try to packlist:"+list.toString());
        return list;
    }

    public List<Mz_BatchListItem> packListWithTip(){
        List<Map<Integer,Integer>> chainTipArray = getReplicaChainPoolTip();
        this.mzlock.lock();
        List<Mz_BatchListItem> list = new ArrayList<>();
        // int quorum = (replica_num/3)*2;
        int quorum = replica_num;
        for (int i=0; i<this.replica_num; i++)
        {
            int batchLen = this.ChainPool[i].size();
            if (batchLen <= 0)    continue;
            System.out.printf("For batch chain %d, node %d's tip Array: %s\n", i, i, chainTipArray.get(i).toString());
            // find slowest node for a batch chain
            int startHeight = this.PackagedHeight[i]+1;
            int localTip = this.ChainPool[i].get(batchLen-1).BatchId;
            int endHeight = localTip;
            // Leader node skip to pack a list it falls behind
            int useBatchhash = 0;
            if (endHeight >= startHeight) {
                int nNotFallBehind = 0;
                for (int j = 0; j < chainTipArray.size(); ++j) {
                    if (chainTipArray.get(j).containsKey(i) == false)   continue;
                    int tmpTip = chainTipArray.get(j).get(i);
                    if (tmpTip < startHeight) continue;
                    ++nNotFallBehind;
                    if (tmpTip < endHeight) endHeight = tmpTip; 
                }
                // at least 2/3 replicas can catch up.
                if (endHeight >= startHeight && nNotFallBehind >= quorum)
                    useBatchhash = 1;
                else
                    endHeight = startHeight;
                Mz_BatchListItem temp = new Mz_BatchListItem(i, startHeight, endHeight, useBatchhash);
                System.out.printf("Node %d packListWithTip for chain %d , startHeight: %d, endHeight:%d, tip: %d, usebatch: %d\n", NodeID, i, startHeight, endHeight, localTip, useBatchhash);
                list.add(temp);
            }
        }
        this.mzlock.unlock();
        return list;
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

    public RequestList getsyncedRequestfromlist(List<Mz_BatchListItem> rev){
        
        RequestList reqlist=new RequestList();
        System.out.println("Stage: getsyncedRequestfromlist try to getRequestfromlist");
        this.mzlock.lock();
        for (Mz_BatchListItem mz_batchListItem : rev) {
            int st = mz_batchListItem.StartHeight;
            int ed = mz_batchListItem.EndHeight;
            int nd = mz_batchListItem.NodeId;
            int uf = mz_batchListItem.usedful;
            System.out.println("Stage: getsyncedRequestfromlist --Node id :"+nd+" StartHeight: "+st+" EndHeight: "+ed+" usebatch "+uf);
            if (uf == 0)
                continue;
            for (int j = st; j <= ed; j++) {
                reqlist.addAll(this.ChainPool[nd].get(j).Req);
                System.out.println("Stage: getsyncedRequestfromlist --Height: "+j+" req: "+this.ChainPool[nd].get(j).Req);
            }
        }
        this.mzlock.unlock();
        // System.out.println("Stage: getsyncedRequestfromlist --totoal reqlist"+reqlist);
        System.out.printf("Stage: getsyncedRequestfromlist --total syncedRequesrequest size: %d, request: %s\n", reqlist.size(), reqlist.toString());
        NPackedTx += reqlist.size();
        if (reqlist.isEmpty() == false) {
            System.out.printf("Node %d packed tx number: %d\n", NodeID, NPackedTx);
        }
        return reqlist;
    }

    public RequestList getnotsyncRequestfromlist(List<Mz_BatchListItem> rev){

        RequestList reqlist=new RequestList();
        System.out.println("Stage: getnotsyncRequestfromlist try to getnotsyncRequestfromlist");
        this.mzlock.lock();
        for (Mz_BatchListItem mz_batchListItem : rev) {
            int st = mz_batchListItem.StartHeight;
            int ed = mz_batchListItem.EndHeight;
            int nd = mz_batchListItem.NodeId;
            int uf = mz_batchListItem.usedful;
            System.out.println("Stage: getnotsyncRequestfromlist --Node id: "+nd+" StartHeight: "+st+" EndHeight: "+ed+" usebatch "+uf);
            if (uf == 1)
                continue;
            for (int j = st; j <= ed; j++) {
                reqlist.addAll(this.ChainPool[nd].get(j).Req);
                System.out.println("Stage: getnotsyncRequestfromlist --Height: "+j+" req: "+this.ChainPool[nd].get(j).Req);
            }
        }
        this.mzlock.unlock();
        // System.out.println("Stage: getnotsyncRequestfromlist --totoal reqlist"+reqlist);
        System.out.printf("Stage: getnotsyncRequestfromlist --total notsyncRequest size: %d, request: %s\n", reqlist.size(), reqlist.toString());
        NPackedTx += reqlist.size();
        if (!reqlist.isEmpty()) {
            System.out.printf("Node %d packed tx number: %d\n", NodeID, NPackedTx);
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
    public TOMMessage getfistMsg(int nodeid, int height){
        return this.ChainPool[nodeid].get(height-1).Req.getFirst();
    }

    public void setLastbatchlist(List<Mz_BatchListItem> lastbatchlist) {
        this.lastbatchlist = lastbatchlist;
    }
}
