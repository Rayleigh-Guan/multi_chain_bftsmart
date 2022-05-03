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
        int numBatchlistItems=msgs.size();
        int size = 24 + //timestamp 8, nonces 4, numBatchlistItems 4
                (numNounces > 0 ? 8 : 0) + //seed if needed
                (Integer.BYTES * 3*numBatchlistItems);// Items length

        ByteBuffer MzProposeBuffer = ByteBuffer.allocate(size);
        MzProposeBuffer.putLong(timestamp);
        MzProposeBuffer.putInt(numNounces);
        if(numNounces>0){
            MzProposeBuffer.putLong(rnd.nextLong());
        }

        MzProposeBuffer.putInt(numBatchlistItems);

        for (int i = 0; i < numBatchlistItems; i++) {
            MzProposeBuffer.putInt(msgs.get(0).NodeId);
            MzProposeBuffer.putInt(msgs.get(0).StartHeight);
            MzProposeBuffer.putInt(msgs.get(0).EndHeight);
        }

        // return the MzPropose
        return MzProposeBuffer.array();
    }
}
