package bftsmart.multi_zone;

import bftsmart.erasureCode.ReedSolomon;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class StripeMessageCache {

    // the MSZstripeMessages a node received at a height.
    private Map<Integer, ArrayList<MZStripeMessage>> receivedStripeMap;
    private Map<Integer, Boolean> alreadyDecoded;
    private int quorum;
    private int totalNum;

    public StripeMessageCache(int quorum, int totalNum){
        this.receivedStripeMap = new HashMap<>();
        this.alreadyDecoded = new HashMap<>();
        assert(quorum > 0);
        assert(totalNum > quorum);
        assert(totalNum <= 256);
        this.quorum = quorum;
        this.totalNum = totalNum;
    }

    public int getQuorum(){
        return this.quorum;
    }

    public int getReceivedStripeNum(int height) {
        if (receivedStripeMap.containsKey(height) == false)
            return -1;
        return receivedStripeMap.get(height).size();
    }

    public boolean addStripe(MZStripeMessage msg) {
        if (this.receivedStripeMap.containsKey(msg.getHeight()) == false)
            this.receivedStripeMap.put(msg.getHeight(), new ArrayList<MZStripeMessage>());
        ArrayList<MZStripeMessage> stripeList = this.receivedStripeMap.get(msg.getHeight());
        boolean addRes = true;
        for(MZStripeMessage stripe: stripeList){
            if (stripe.getBatchChainId() == msg.getBatchChainId() &&
                stripe.getHeight() == msg.getHeight() &&
                stripe.getStripeId() == msg.getStripeId()
            ) {
                addRes = false;
                break;
            }
        }
        if (addRes) 
            stripeList.add(msg);
        return addRes;     
    }

    public MZStripeMessage getStripe(int height, int stripeId) {
        if (this.receivedStripeMap.containsKey(height) == false)
            return null;
        for(MZStripeMessage msg: this.receivedStripeMap.get(height)) {
            if (msg.getStripeId() == stripeId)
                return msg;
        }
        return null;
    }

    public boolean oldMsg(MZStripeMessage msg) {
        // never received this message, its not a old message.
        if (this.alreadyDecoded.containsKey(msg.getHeight()) == false)
            return false;
        return this.alreadyDecoded.get(msg.getHeight()) == true;
    }

    public boolean receiveQuorum(int height) {
        if (this.receivedStripeMap.get(height) == null || this.receivedStripeMap.get(height).size() < quorum)
            return false;
        return true;
    }

    public byte[] decodeMsg(int height) {
        assert (receiveQuorum(height) == true);
        ReedSolomon codec = ReedSolomon.create(this.quorum, this.totalNum - this.quorum);
        ArrayList<MZStripeMessage> stripeList = this.receivedStripeMap.get(height);
        assert(stripeList != null && stripeList.size() > 0);
        byte[][] shards = new byte[totalNum][stripeList.get(0).getStripe().length];
        boolean[] shardPresent = new boolean[this.totalNum];
        for (MZStripeMessage stripeMsg: stripeList) {
            shardPresent[stripeMsg.getStripeId()] = true;
            shards[stripeMsg.getStripeId()] = stripeMsg.getStripe();
        }
       
        boolean needDecode = false;
        for(int i = 0; i < this.quorum; ++i) {
            if (shardPresent[i] == false) {
                needDecode = true;
                break;
            }
        }
        if (needDecode) {
            final int shardLength = shards[0].length;
            codec.decodeMissing(shards, shardPresent, 0, shardLength);
        }
        // System.out.printf("Merge byteArray \n");
        byte[] batch = mergeByteArray(shards, this.receivedStripeMap.get(height).get(0).getTotalLen());
        // mark this stripe msg as alredy received
        alreadyDecoded.put(height, true);
        if (height >= 20 + (receivedStripeMap.keySet().stream().findFirst().get()))
            this.receivedStripeMap.remove(height);
        return batch;
    }

    private byte[] mergeByteArray(byte[][] shards, int len){ 
        byte[] batch = new byte[len];
        int i = 0, curLen = 0;
        int copyLen = shards[i].length;
        while (i < this.quorum) {
            if (curLen + shards[i].length > len)
                copyLen = len - curLen;
            System.arraycopy(shards[i], 0, batch, curLen, copyLen);
            curLen += copyLen;
            ++i;
        }  
        return batch;  
    }
}
