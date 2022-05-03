package bftsmart.tom.util;

import bftsmart.clientsmanagement.RequestList;
import bftsmart.multi_zone.Mz_Batch;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.messages.TOMMessage;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Random;

public class MzBatchReader
{
    private ByteBuffer MzBatchBuffer;
    private boolean useSignatures;
    public MzBatchReader(byte[] batch, boolean useSignatures) {
        this.MzBatchBuffer = ByteBuffer.wrap(batch);
        this.useSignatures = useSignatures;
    }
    public Mz_Batch deserialisemsg() {

        //obtain the timestamps to be delivered to the application

        int id=MzBatchBuffer.getInt();
        int height=MzBatchBuffer.getInt();
        int numberOfMessages = MzBatchBuffer.getInt();

        RequestList Req=new RequestList();

        for (int i = 0; i < numberOfMessages; i++) {
            //read the message and its signature from the batch
            int messageSize = MzBatchBuffer.getInt();

            byte[] message = new byte[messageSize];
            MzBatchBuffer.get(message);

            byte[] signature = null;

            if (useSignatures) {

                int sigSize = MzBatchBuffer.getInt();

                if (sigSize > 0) {
                    signature = new byte[sigSize];
                    MzBatchBuffer.get(signature);
                }
            }
            try {
                DataInputStream ois = new DataInputStream(new ByteArrayInputStream(message));
                TOMMessage tm = new TOMMessage();
                tm.rExternal(ois);
                tm.serializedMessage = message;
                tm.serializedMessageSignature = signature;
                Req.add(tm);

            } catch (Exception e) {
                LoggerFactory.getLogger(this.getClass()).error("Failed to deserialize Mzbatch",e);
            }
        }
        return new Mz_Batch(id,height,Req);
    }
}
