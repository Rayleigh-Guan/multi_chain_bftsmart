package bftsmart.multi_zone;

import java.util.ArrayList;
import java.util.List;

public class Mz_Propose {
    public long timestamp;
    public int numNounces;
    public long seed;
    public int numBatchlistItems;
    public List<Mz_BatchListItem> list=new ArrayList<>();

}
