package bftsmart.multi_zone;

public class Mz_BatchListItem {
    public int NodeId;
    public int StartHeight;
    public int EndHeight;


    public Mz_BatchListItem(int ni,int sh,int eh){
        this.NodeId=ni;
        this.StartHeight=sh;
        this.EndHeight=eh;

    }
}
