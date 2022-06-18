package bftsmart.multi_zone;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import bftsmart.clientsmanagement.RequestList;

public class RebuildProposeState {
    public boolean state;
    public RequestList reqList;
    public Map<Integer, ArrayList<Integer>> missingBatchMap;
    public byte[] batch;

    RebuildProposeState() {
        this.state = true;
        this.reqList = new RequestList();
        missingBatchMap = new HashMap<>();
        batch = null;
    }

    public void setOk(boolean input) {
        this.state = input;
    }

    public void setList(RequestList input) {
        this.reqList = input;
    }

    public boolean getState() {
        return this.state;
    }

    public RequestList getlist() {
        return this.reqList;
    }

    public Map<Integer, ArrayList<Integer>> getMissingBatchMap(){
        return this.missingBatchMap;
    }

    public void addMissingBatch(int BatchId, int startStripe, int endStripe) {
        if (this.missingBatchMap.containsKey(BatchId) == false)
            this.missingBatchMap.put(BatchId, new ArrayList<>());
        this.missingBatchMap.get(BatchId).add(startStripe);
        this.missingBatchMap.get(BatchId).add(endStripe);
    }

}
