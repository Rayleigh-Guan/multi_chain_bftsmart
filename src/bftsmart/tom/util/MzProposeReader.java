package bftsmart.tom.util;

import bftsmart.multi_zone.Mz_BatchListItem;
import bftsmart.multi_zone.Mz_Propose;

import java.nio.ByteBuffer;

public class MzProposeReader {
    private ByteBuffer MzProposeBuffer;

    /** wrap buffer */
    public MzProposeReader(byte[] batch) {
        MzProposeBuffer = ByteBuffer.wrap(batch);
    }

    public Mz_Propose deserialisemsg() {
        Mz_Propose mz_propose=new Mz_Propose();
        mz_propose.timestamp=MzProposeBuffer.getLong();
        mz_propose.numNounces=MzProposeBuffer.getInt();

        if(mz_propose.numNounces > 0){
            mz_propose.seed = MzProposeBuffer.getLong();
        }
        else mz_propose.numNounces = 0;

        mz_propose.numBatchlistItems=MzProposeBuffer.getInt();

        for (int i=0;i< mz_propose.numBatchlistItems;i++)
        {
            mz_propose.list.add(new Mz_BatchListItem(MzProposeBuffer.getInt(),MzProposeBuffer.getInt(),MzProposeBuffer.getInt()));
        }
        return mz_propose;
    }
}
