package bftsmart.tom.util;

import bftsmart.multi_zone.Mz_BatchListItem;
import bftsmart.tom.core.messages.TOMMessage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

public class MzProposeBuilder {
    private Random rnd;

    public MzProposeBuilder(long seed){
        rnd = new Random(seed);
    }
    public byte[] makeMzPropose(List<Mz_BatchListItem> msgs, int numNounces, long timestamp) {
        System.out.println("Stage: makeMzPropose --Node make a Mzpropose to byte");
        int numBatchlistItems=msgs.size();
        int size = 16 + //timestamp 8, nonces 4, numBatchlistItems 4
                (numNounces > 0 ? 8 : 0) + //seed if needed
                (4*3*numBatchlistItems);// Items length
        System.out.println("Stage: makeMzPropose --Mzpropose size: "+size);
        ByteBuffer MzProposeBuffer = ByteBuffer.allocate(size);
        MzProposeBuffer.putLong(timestamp);
        System.out.println("Stage: makeMzPropose --Mzpropose timestamp: "+timestamp);
        MzProposeBuffer.putInt(numNounces);
        System.out.println("Stage: makeMzPropose --Mzpropose numNounces: "+numNounces);
        if(numNounces>0){
            MzProposeBuffer.putLong(rnd.nextLong());
        }

        MzProposeBuffer.putInt(numBatchlistItems);
        for (Mz_BatchListItem msg : msgs) {
            MzProposeBuffer.putInt(msg.NodeId);
            MzProposeBuffer.putInt(msg.StartHeight);
            MzProposeBuffer.putInt(msg.EndHeight);
            System.out.println("Stage: makeMzPropose --Mzpropose BatchlistItem: --nd: "+msg.NodeId+" --st: "+msg.StartHeight+" --ed: "+msg.EndHeight);
        }

        // return the MzPropose
        return MzProposeBuffer.array();
    }
}
