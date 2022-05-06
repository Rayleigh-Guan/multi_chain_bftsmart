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
    public List<Mz_BatchListItem> list=new ArrayList<>();
    public int numofnotsyncreq;
    public RequestList notsyncreq;

}
