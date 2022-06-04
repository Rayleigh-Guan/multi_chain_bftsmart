package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;
import bftsmart.tom.core.messages.TOMMessage;

import java.util.ArrayList;
import java.util.List;

public class Mz_Propose {
    public long timestamp;
    public int numNounces;
    public long seed;
    public int numBatchlistItems;
    public List<Mz_BatchListItem> list = new ArrayList<>();
    public int numofnotsyncreq;
    public RequestList notsyncreq;

    @Override
    public String toString(){
        String notsyncreqStr = (notsyncreq == null) ? "|":notsyncreq.toString();
        String listStr = (list == null) ? "|":list.toString();
        return String.format("timestamp:%d_nounce:%d_seed:%d_numBatchlistItems:%d_list:%s_numofnotsyncreq:%d_notsyncreq%s", 
            timestamp, numNounces, seed, numBatchlistItems, listStr, numofnotsyncreq, notsyncreqStr);
    }
}
