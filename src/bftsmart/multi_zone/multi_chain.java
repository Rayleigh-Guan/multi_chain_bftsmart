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
    private int NPackedTx;
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
        NPackedTx = 0;
    }

    public void add(Mz_Batch value){
        this.mzlock.lock();
        int nodeid = value.NodeId;
        this.ChainPool[nodeid].add(value);
//        if (nodeid==this.NodeID)
//            this.MyGeneratedHeight=this.MyGeneratedHeight+1;
        System.out.println("Stage: addbatch --Nodeid: "+this.NodeID+" received "+value.BatchId+" from "+value.NodeId+" --batch:"+value.Req+" --len"+this.ChainPool[nodeid].size());
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
                    Mz_BatchListItem temp=new Mz_BatchListItem(i,hi+1,hi+1,1);
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

    public RequestList getsyncedRequestfromlist(List<Mz_BatchListItem> rev){
        
        RequestList reqlist=new RequestList();
        System.out.println("Stage: getsyncedRequestfromlist try to getRequestfromlist");
        this.mzlock.lock();
        for (Mz_BatchListItem mz_batchListItem : rev) {
            int st = mz_batchListItem.StartHeight;
            int ed = mz_batchListItem.EndHeight;
            int nd = mz_batchListItem.NodeId;
            int uf=mz_batchListItem.usedful;
            System.out.println("Stage: getsyncedRequestfromlist --Node id :"+nd+" StartHeight: "+st+" EndHeight"+ed+" usebatch"+uf);
            if (uf == 0)
                continue;
            for (int j = st - 1; j < ed; j++) {
                reqlist.addAll(this.ChainPool[nd].get(j).Req);
                System.out.println("Stage: getsyncedRequestfromlist --Height: "+j+" req: "+this.ChainPool[nd].get(j).Req);
            }
        }
        this.mzlock.unlock();
        // System.out.println("Stage: getsyncedRequestfromlist --totoal reqlist"+reqlist);
        System.out.printf("Stage:getsyncedRequestfromlist --total request size: %d, request: %s\n", reqlist.size(), reqlist.toString());
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
            System.out.println("Stage: getnotsyncRequestfromlist --Node id :"+nd+" StartHeight: "+st+" EndHeight"+ed+" usebatch"+uf);
            if (uf == 1)
                continue;
            for (int j = st - 1; j < ed; j++) {
                reqlist.addAll(this.ChainPool[nd].get(j).Req);
                System.out.println("Stage: getnotsyncRequestfromlist --Height: "+j+" req: "+this.ChainPool[nd].get(j).Req);
            }
        }
        this.mzlock.unlock();
        // System.out.println("Stage: getnotsyncRequestfromlist --totoal reqlist"+reqlist);
        System.out.printf("Stage:getnotsyncRequestfromlist --total request size: %d, request: %s\n", reqlist.size(), reqlist.toString());
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
