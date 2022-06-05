package bftsmart.demo.microbenchmarks;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.util.Storage;
import bftsmart.tom.AsynchServiceProxy;


import java.util.Arrays;
import java.util.HashMap;



public class AsyncReplyListener implements ReplyListener {
    private int replies ;
    private boolean verbose;
    private HashMap<Integer, Long> reqSendTime;
    private HashMap<Integer, Long> reqCompletedSendime;
	private HashMap<Integer, Long> replyRecvTime;
    private int id; 
    private AsynchServiceProxy serviceProxy;        

    public AsyncReplyListener(int clientid, AsynchServiceProxy service, boolean vb) {
        super();
        id = clientid;
        reqSendTime = new HashMap<>();
        reqCompletedSendime=new HashMap<>();
        replyRecvTime = new HashMap<>();
        serviceProxy = service;
        replies = 0;
        verbose = vb;
    }
    public void sotreCompletetime(int reqId)
    {
        if (reqCompletedSendime.containsKey(reqId) == false) 
        reqCompletedSendime.put(reqId, System.currentTimeMillis());
    }
    public void storeRequest(int reqId) {
        if (reqSendTime.containsKey(reqId) == false) 
            reqSendTime.put(reqId, System.currentTimeMillis());
    }

    public void storeReply(int reqId) {
        if (replyRecvTime.containsKey(reqId) == false)
            replyRecvTime.put(reqId, System.currentTimeMillis());
    }

    public int getNumberofReply(){
        return replyRecvTime.size();
    }

    public int getNumberofRequest(){
        return reqSendTime.size();
    }

    public boolean receiveAllReply(int nReq){
        return replyRecvTime.size() >= nReq;
    }

    public void printStaticsInfo() {
        int numberOfOps = replyRecvTime.size();
        Storage st = new Storage(numberOfOps);
        Storage stforsend = new Storage(numberOfOps);
        for (HashMap.Entry<Integer, Long> entry : replyRecvTime.entrySet()) {
            long recvTime = entry.getValue();
            int reqId = entry.getKey();
            if (reqSendTime.containsKey(reqId)==false)
                System.out.printf("Error: %d received reply %d, but have not corresponding request\n", this.id, entry.getKey());
            else
            {
                st.store(recvTime - reqSendTime.get(reqId));
                stforsend.store(reqCompletedSendime.get(reqId)-reqSendTime.get(reqId));
                System.out.println(this.id+" "+reqId+" "+reqSendTime.get(reqId)+" "+reqCompletedSendime.get(reqId)+" "+recvTime);
            }
                
        }
            
        System.out.println(this.id + "// Total send " + reqSendTime.size() +" cmds, receives "+ replyRecvTime.size());
        System.out.println(this.id + " // Average time for " + numberOfOps + " executions (-10%) = "
        + st.getAverage(true) + " ms ");
        System.out.println(this.id + " // Average time for " + numberOfOps + " complete send (-10%) = "
                + stforsend.getAverage(true) + " ms ");
        System.out.println(this.id + " // Standard desviation for " + numberOfOps + " executions (-10%) = "
                + st.getDP(true) + " ms ");
        System.out.println(this.id + " // Average time for " + numberOfOps + " executions (all samples) = "
                + st.getAverage(false) + " ms ");
        System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2
                + " executions (all samples) = " + st.getDP(false)  + " ms ");
        System.out.println(this.id + " // Maximum time for " + numberOfOps + " executions (all samples) = "
                + st.getMax(false) + " ms ");
    }
    
    @Override
    public void reset() {

        if (verbose) System.out.println("[RequestContext] The proxy is re-issuing the request to the replicas");
        replies = 0;
    }

    @Override
    public void replyReceived(RequestContext context, TOMMessage reply) {
        StringBuilder builder = new StringBuilder();
        builder.append("[RequestContext] id: " + context.getReqId() + " type: " + context.getRequestType());
        builder.append("[TOMMessage reply] sender id: " + reply.getSender() + " Hash content: " + Arrays.toString(reply.getContent()));
        if (verbose) System.out.println(builder.toString());

        replies++;
        storeReply(reply.getSequence());
        if (verbose) System.out.println("[RequestContext] clean request context id: " + context.getReqId());
        serviceProxy.cleanAsynchRequest(context.getOperationId());

        // double q = Math.ceil((double) (serviceProxy.getViewManager().getCurrentViewN() + serviceProxy.getViewManager().getCurrentViewF() + 1) / 2.0);

        // if (replies >= q) {
        //     if (verbose) System.out.println("[RequestContext] clean request context id: " + context.getReqId());
        //     serviceProxy.cleanAsynchRequest(context.getOperationId());
        // }
    }
}
