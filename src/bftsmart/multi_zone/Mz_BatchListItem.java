package bftsmart.multi_zone;

public class Mz_BatchListItem implements Cloneable{

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

    @Override
    public String toString(){
        return String.format("NodeId:%d_StartHeight:%d_EndHeight:%d_usedful:%d", NodeId, StartHeight,EndHeight,usedful);
    }

    @Override
    public Mz_BatchListItem clone(){
        Mz_BatchListItem cloneItem = new Mz_BatchListItem(this.NodeId, this.StartHeight,this.EndHeight,this.usedful);
        return cloneItem;
    }
}
