package bftsmart.multi_zone;

public class Mz_BatchListItem {
    public Integer NodeId;
    public Integer StartHeight;
    public Integer EndHeight;


    public Mz_BatchListItem(Integer ni,Integer sh,Integer eh){
        this.NodeId=ni;
        this.StartHeight=sh;
        this.EndHeight=eh;

    }
}
