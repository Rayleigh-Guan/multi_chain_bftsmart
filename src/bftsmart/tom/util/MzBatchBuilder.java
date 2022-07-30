package bftsmart.tom.util;

import bftsmart.multi_zone.Mz_Batch;
import bftsmart.tom.core.messages.TOMMessage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

public class MzBatchBuilder {
    private void putMessage(ByteBuffer MzBatchBuffer, byte[] message, boolean addSig, byte[] signature, Long recp) {
        MzBatchBuffer.putInt(message.length);
        MzBatchBuffer.put(message);

        if (addSig) {
            if (signature != null) {
                MzBatchBuffer.putInt(signature.length);
                MzBatchBuffer.put(signature);
            } else {
                MzBatchBuffer.putInt(0);
            }
        }
        MzBatchBuffer.putLong(recp);
    }

    public byte[] makeMzBatch(Mz_Batch bundle, boolean useSignatures) {
        return makeMzBatch(bundle.getNodeId(), bundle.getBatchId(), bundle.getReq(), useSignatures, bundle.getChainPooltip());
    }

    public byte[] makeMzBatch(int id, int height, List<TOMMessage> msgs, boolean useSignatures,
            Map<Integer, Integer> chainPoolTip) {
        int numMsgs = msgs.size();
        int totalMessageSize = 0; // total size of the messages being batched

        byte[][] messages = new byte[numMsgs][]; // bytes of the message (or its hash)
        byte[][] signatures = new byte[numMsgs][]; // bytes of the message (or its hash)
        Long[] recptime = new Long[numMsgs];
        int i = 0;

        for (TOMMessage msg : msgs) {
            messages[i] = msg.serializedMessage;
            signatures[i] = msg.serializedMessageSignature;
            recptime[i] = msg.receptionTime;
            totalMessageSize += messages[i].length;
            i++;
        }

        // return the batch
        return createMzBatch(id, height, numMsgs, totalMessageSize, messages, signatures, useSignatures, chainPoolTip,
                recptime);
    }

    private byte[] createMzBatch(int id, int height, int numberOfMessages, int totalMessagesSize, byte[][] messages,
            byte[][] signatures, boolean useSignatures, Map<Integer, Integer> chainPoolTip, Long[] recptime) {

        int sigsSize = 0;
        if (useSignatures) {

            sigsSize = Integer.BYTES * numberOfMessages;

            for (byte[] sig : signatures) {

                sigsSize += sig.length;
            }
        }
        int size = 12 + // id 4, height 4, nummessages 4
                (Integer.BYTES * numberOfMessages) + // messages length size
                (Long.BYTES * numberOfMessages) +
                sigsSize + // signatures size
                Integer.BYTES + // the number of <key,value> pair of chainPool Tip
                Integer.BYTES * chainPoolTip.size() * 2 + // chainPoolTip size
                totalMessagesSize; // size of all msges

        ByteBuffer MzBacthBuffer = ByteBuffer.allocate(size);
        MzBacthBuffer.putInt(id);
        MzBacthBuffer.putInt(height);
        MzBacthBuffer.putInt(numberOfMessages);
        for (int i = 0; i < numberOfMessages; i++) {
            putMessage(MzBacthBuffer, messages[i], useSignatures, signatures[i], recptime[i]);
        }

        // put batchTip into array
        MzBacthBuffer.putInt(chainPoolTip.size());
        for (Integer key : chainPoolTip.keySet()) {
            MzBacthBuffer.putInt(key);
            MzBacthBuffer.putInt(chainPoolTip.get(key));
        }

        return MzBacthBuffer.array();
    }

    /**
     * 
     * @param bytes batch array
     * @param k the number stripes to divide
     * @param m the number of extra stripes to recovery
     * @return a squre of byte array.
     */
    public byte[][] prepareByteArray(byte[] bytes, int k, int m) {
        assert(k <= bytes.length );
        final int arrayLen = (int) Math.ceil(bytes.length/((double) k));
        byte[][] result = new byte[k+m][arrayLen];
        int from, to;
        for (int i = 0; i < k; i++) {
            from = i * arrayLen;
            to = from + arrayLen;
            result[i] = Arrays.copyOfRange(bytes, from, to);
        }
        return result;
    }
}
