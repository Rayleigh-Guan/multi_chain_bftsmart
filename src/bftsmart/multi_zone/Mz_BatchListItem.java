package bftsmart.multi_zone;

public class Mz_BatchListItem {

    public int NodeId;
    public int StartHeight;
    public int EndHeight;
    public int usedful;// 0 can not use batch 1 can use batch

    public Mz_BatchListItem(int ni, int sh, int eh, int uf) {
        this.NodeId = ni;
        this.StartHeight = sh;
        this.EndHeight = eh;
        this.usedful = uf;
    }
}
