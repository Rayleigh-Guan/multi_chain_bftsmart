package bftsmart.multi_zone;

public class Mz_BatchListItem {
    public Integer StartHeight;
    public Integer EndHeight;
    public Integer NodeId;

    public Mz_BatchListItem(Integer sh,Integer eh,Integer ni){
        this.StartHeight=sh;
        this.EndHeight=eh;
        this.NodeId=ni;
    }
}
