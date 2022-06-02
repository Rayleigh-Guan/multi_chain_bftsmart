package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;
import java.util.Map;

public class Mz_Batch {
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
}
