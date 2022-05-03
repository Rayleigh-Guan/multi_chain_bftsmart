package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;
import bftsmart.tom.core.messages.TOMMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
public class multi_chain {
    private int replica_num=4;

    private List<Mz_Batch> ChainPool[] = new ArrayList[this.replica_num];
    private int[] PackagedHeight=new int[this.replica_num];

    private int MyGeneratedHeight;
    private int NodeID;

    private ReentrantLock mzlock = new ReentrantLock();
    private List<Mz_BatchListItem> lastbatchlist=new ArrayList<>();

    public multi_chain(Integer nodeid)
    {
        for (int i = 0; i < this.replica_num; i++) {
            this.ChainPool[i]=new ArrayList<>();
            this.PackagedHeight[i]=0;
        }
        this.NodeID=nodeid;
        this.MyGeneratedHeight=0;
    }

    public void add(Mz_Batch value){
        this.mzlock.lock();
        int nodeid = value.NodeId;
        this.ChainPool[nodeid].add(value);
//        if (nodeid==this.NodeID)
//            this.MyGeneratedHeight=this.MyGeneratedHeight+1;
        System.out.println("Nodeid: "+this.NodeID+" received "+value.BatchId+" from "+value.NodeId+" --batch:"+value.Req);
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
            if (batchtip>hi+2)
            {
                Mz_BatchListItem temp=new Mz_BatchListItem(hi+1,hi+1,i);
                list.add(temp);
            }
        }
        this.mzlock.unlock();
        return list;
    }

    public RequestList getRequestfromlist(List<Mz_BatchListItem> rev){
        int revsize=rev.size();
        RequestList reqlist=new RequestList();

        this.mzlock.lock();
        for (Mz_BatchListItem mz_batchListItem : rev) {
            Integer st = mz_batchListItem.StartHeight;
            Integer ed = mz_batchListItem.EndHeight;
            Integer nd = mz_batchListItem.NodeId;
            for (int j = st - 1; j < ed; j++) {
                reqlist.addAll(this.ChainPool[nd].get(j).Req);
            }
        }
        this.mzlock.unlock();

        return reqlist;
    }

    public void updatePackagedHeight(){
        this.mzlock.lock();
        for (Mz_BatchListItem mz_batchListItem : this.lastbatchlist) {
            Integer nodeid_temp = mz_batchListItem.NodeId;
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
        this.MyGeneratedHeight=this.MyGeneratedHeight+1;
        this.mzlock.unlock();
    }
    public TOMMessage getfistMsg(int nodeid, int height){
        return this.ChainPool[nodeid].get(height-1).Req.getFirst();
    }

    public void setLastbatchlist(List<Mz_BatchListItem> lastbatchlist) {
        this.lastbatchlist = lastbatchlist;
    }
}
