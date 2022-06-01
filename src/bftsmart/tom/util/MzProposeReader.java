package bftsmart.tom.util;

import bftsmart.clientsmanagement.RequestList;
import bftsmart.multi_zone.Mz_BatchListItem;
import bftsmart.multi_zone.Mz_Propose;
import bftsmart.tom.core.messages.TOMMessage;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

public class MzProposeReader {
    private ByteBuffer MzProposeBuffer;
    private boolean useSignatures;

    /** wrap buffer */
    public MzProposeReader(byte[] batch,boolean useSignatures) {
        MzProposeBuffer = ByteBuffer.wrap(batch);
        useSignatures=useSignatures;
    }

    public Mz_Propose deserialisemsg() {
        Mz_Propose mz_propose=new Mz_Propose();

        mz_propose.timestamp=MzProposeBuffer.getLong();

        mz_propose.numNounces=MzProposeBuffer.getInt();

        System.out.println("Stage: MzProposedeserialisemsg --timestamp: "+mz_propose.timestamp+"\n--numNounces: "+mz_propose.numNounces);
        if(mz_propose.numNounces > 0){
            mz_propose.seed = MzProposeBuffer.getLong();
        }
        else mz_propose.numNounces = 0;


        mz_propose.numofnotsyncreq=MzProposeBuffer.getInt();
        if (mz_propose.numofnotsyncreq>0) {
            RequestList Req=new RequestList();;
            for (int i = 0; i < mz_propose.numofnotsyncreq; i++) {
                //read the message and its signature from the batch
                int messageSize = MzProposeBuffer.getInt();

                byte[] message = new byte[messageSize];
                MzProposeBuffer.get(message);

                byte[] signature = null;

                if (useSignatures) {

                    int sigSize = MzProposeBuffer.getInt();

                    if (sigSize > 0) {
                        signature = new byte[sigSize];
                        MzProposeBuffer.get(signature);
                    }
                }
                Long recp=MzProposeBuffer.getLong();
                try {
                    DataInputStream ois = new DataInputStream(new ByteArrayInputStream(message));
                    TOMMessage tm = new TOMMessage();
                    tm.rExternal(ois);
                    ois.close();
                    tm.serializedMessage = message;
                    tm.serializedMessageSignature = signature;
                    tm.receptionTime=recp;
                    Req.add(tm);

                } catch (Exception e) {
                    LoggerFactory.getLogger(this.getClass()).error("Failed to deserialize Mzbatch", e);
                }
            }
            mz_propose.notsyncreq=Req;
        }
        mz_propose.numBatchlistItems=MzProposeBuffer.getInt();
        System.out.println("Stage: MzProposedeserialisemsg --numBatchlistItems: "+mz_propose.numBatchlistItems);

        for (int i=0;i< mz_propose.numBatchlistItems;i++)
        {
            Mz_BatchListItem batchListItem=new Mz_BatchListItem(MzProposeBuffer.getInt(),MzProposeBuffer.getInt(),MzProposeBuffer.getInt(),MzProposeBuffer.getInt());
            System.out.println("Stage: MzProposedeserialisemsg --batchListItem: node:"+batchListItem.NodeId+" start height: "+batchListItem.StartHeight+" endheight: "+ batchListItem.EndHeight+" usebatch: "+batchListItem.usedful);
            mz_propose.list.add(batchListItem);
        }
        return mz_propose;
    }
}
