package bftsmart.tom.util;

import bftsmart.tom.core.messages.TOMMessage;

import java.nio.ByteBuffer;
import java.util.List;

public class MzBatchBuilder {
    private void putMessage(ByteBuffer MzBatchBuffer, byte[] message, boolean addSig, byte[] signature) {
        MzBatchBuffer.putInt(message.length);
        MzBatchBuffer.put(message);

        if (addSig) {
            if(signature != null) {
                MzBatchBuffer.putInt(signature.length);
                MzBatchBuffer.put(signature);
            } else {
                MzBatchBuffer.putInt(0);
            }
        }
    }
    public byte[] makeMzBatch(int id,Integer height,List<TOMMessage> msgs, boolean useSignatures) {
        int numMsgs = msgs.size();
        int totalMessageSize = 0; //total size of the messages being batched

        byte[][] messages = new byte[numMsgs][]; //bytes of the message (or its hash)
        byte[][] signatures = new byte[numMsgs][]; //bytes of the message (or its hash)
        int i = 0;

        for (TOMMessage msg : msgs) {
            messages[i] = msg.serializedMessage;
            signatures[i] = msg.serializedMessageSignature;
            totalMessageSize += messages[i].length;
            i++;
        }

        // return the batch
        return createMzBatch(id,height,numMsgs, totalMessageSize,messages, signatures,useSignatures);
    }
    private byte[] createMzBatch(int id,Integer height,int numberOfMessages, int totalMessagesSize,byte[][] messages, byte[][] signatures,boolean useSignatures) {

        int sigsSize = 0;
        if (useSignatures) {

            sigsSize = Integer.BYTES * numberOfMessages;

            for (byte[] sig : signatures) {

                sigsSize += sig.length;
            }
        }
        int size = 12 + //id 4, height 4, nummessages 4
                (Integer.BYTES * numberOfMessages) + // messages length size
                sigsSize + // signatures size
                totalMessagesSize; //size of all msges

        ByteBuffer MzBacthBuffer = ByteBuffer.allocate(size);
        MzBacthBuffer.putInt(id);
        MzBacthBuffer.putInt(height);
        MzBacthBuffer.putInt(numberOfMessages);
        for (int i = 0; i < numberOfMessages; i++) {
            putMessage(MzBacthBuffer,messages[i], useSignatures, signatures[i]);
        }

        return MzBacthBuffer.array();
    }

}
