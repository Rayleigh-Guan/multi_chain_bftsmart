package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;

public class Mz_Batch {
    Integer NodeId;
    Integer BatchId;
    RequestList Req;

    public Mz_Batch(Integer nodeid,Integer batchid,RequestList req)
    {
        this.NodeId=nodeid;
        this.BatchId=batchid;
        this.Req=req;
    }
}

