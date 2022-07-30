package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;

import java.util.HashMap;
import java.util.Map;

public class Mz_Batch implements Cloneable{
    Integer NodeId;
    Integer BatchId;
    RequestList Req;
    Map<Integer, Integer> chainPooltip;// It is an array like {NodeId, Tip, NodeId, Tip...}

    public Mz_Batch(Integer nodeid, Integer batchid, RequestList req, Map<Integer, Integer> tipMap) {
        this.NodeId = nodeid;
        this.BatchId = batchid;
        this.Req = req;
        chainPooltip = tipMap;
    }

    public int getNodeId() {
        return NodeId;
    }

    public int getBatchId(){
        return BatchId;
    }

    public RequestList getReq(){
        return Req;
    }

    public Map<Integer, Integer> getChainPooltip(){
        return chainPooltip;
    }

    @Override
    public String toString(){
        return String.format("NodeId:%d, BatchId:%d,Req:%s,chainPooltip:%s", NodeId, BatchId, Req.toString(), chainPooltip.toString());
    }

    @Override
    public Mz_Batch clone(){
        Map<Integer, Integer> tipMap = new HashMap<>();
        tipMap.putAll(chainPooltip);
        Mz_Batch batch = new Mz_Batch(this.NodeId, this.BatchId, this.Req.clone(), tipMap);
        return batch;
    }
}
