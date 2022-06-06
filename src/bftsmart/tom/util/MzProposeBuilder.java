// package bftsmart.tom.util;

// import bftsmart.clientsmanagement.RequestList;
// import bftsmart.multi_zone.Mz_BatchListItem;
// import bftsmart.tom.core.messages.TOMMessage;

// import java.nio.ByteBuffer;
// import java.util.List;
// import java.util.Random;

// public class MzProposeBuilder {
//     private Random rnd;

//     public MzProposeBuilder(long seed) {
//         rnd = new Random(seed);
//     }

//     private void putMessage(ByteBuffer MzproposalBuffer, byte[] message, boolean addSig, byte[] signature, Long recp) {
//         MzproposalBuffer.putInt(message.length);
//         MzproposalBuffer.put(message);

//         if (addSig) {
//             if (signature != null) {
//                 MzproposalBuffer.putInt(signature.length);
//                 MzproposalBuffer.put(signature);
//             } else {
//                 MzproposalBuffer.putInt(0);
//             }
//         }
//         MzproposalBuffer.putLong(recp);
//     }

//     public byte[] createMzBatch(List<Mz_BatchListItem> msgs, int numNounces, long timestamp, int numberOfreq,
//             int totalreqSize,
//             boolean useSignatures, byte[][] messages, byte[][] signatures, Long[] recptime) {
//         System.out.println("Stage: makeMzPropose --Node make a Mzpropose to byte");

//         int sigsSize = 0;

//         if (useSignatures) {

//             sigsSize = Integer.BYTES * numberOfreq;

//             for (byte[] sig : signatures) {

//                 sigsSize += sig.length;
//             }
//         }

//         int numBatchlistItems = msgs.size();

//         int size = 20 + // timestamp 8, nonces 4, notsyncrequestnum 4,numBatchlistItems 4
//                 (numNounces > 0 ? 8 : 0) + // seed if needed
//                 (Integer.BYTES * numberOfreq) + // messages length
//                 (Long.BYTES * numberOfreq) +
//                 sigsSize + // signatures size
//                 totalreqSize +
//                 (4 * 4 * numBatchlistItems);// Items length

//         System.out.println("Stage: makeMzPropose --Mzpropose size: " + size);

//         ByteBuffer MzProposeBuffer = ByteBuffer.allocate(size);

//         MzProposeBuffer.putLong(timestamp);
//         System.out.println("Stage: makeMzPropose --Mzpropose timestamp: " + timestamp);

//         MzProposeBuffer.putInt(numNounces);
//         System.out.println("Stage: makeMzPropose --Mzpropose numNounces: " + numNounces);
//         if (numNounces > 0) {
//             MzProposeBuffer.putLong(rnd.nextLong());
//         }

//         MzProposeBuffer.putInt(numberOfreq);
//         for (int i = 0; i < numberOfreq; i++) {
//             putMessage(MzProposeBuffer, messages[i], useSignatures, signatures[i], recptime[i]);
//         }

//         MzProposeBuffer.putInt(numBatchlistItems);
//         for (Mz_BatchListItem msg : msgs) {
//             MzProposeBuffer.putInt(msg.NodeId);
//             MzProposeBuffer.putInt(msg.StartHeight);
//             MzProposeBuffer.putInt(msg.EndHeight);
//             MzProposeBuffer.putInt(msg.usedful);
//             System.out.println("Stage: makeMzPropose --Mzpropose BatchlistItem: --nd: " + msg.NodeId + " --st: "
//                     + msg.StartHeight + " --ed: " + msg.EndHeight + " --uf: " + msg.usedful);
//         }

//         // return the MzPropose
//         return MzProposeBuffer.array();
//     }

//     public byte[] createMzBatch(List<Mz_BatchListItem> msgs, int numNounces, long timestamp) {
//         System.out.println("Stage: makeMzPropose --Node make a Mzpropose to byte");
//         int numBatchlistItems = msgs ==null ? 0 : msgs.size();
//         int size = 20 + // timestamp 8, nonces 4, notsyncrequestnum 4,numBatchlistItems 4
//                 (numNounces > 0 ? 8 : 0) + // seed if needed
//                 (4 * 4 * numBatchlistItems);// Items length

//         System.out.println("Stage: makeMzPropose --Mzpropose size: " + size);
//         ByteBuffer MzProposeBuffer = ByteBuffer.allocate(size);

//         MzProposeBuffer.putLong(timestamp);
//         System.out.println("Stage: makeMzPropose --Mzpropose timestamp: " + timestamp);

//         MzProposeBuffer.putInt(numNounces);
//         System.out.println("Stage: makeMzPropose --Mzpropose numNounces: " + numNounces);
//         if (numNounces > 0) {
//             MzProposeBuffer.putLong(rnd.nextLong());
//         }

//         MzProposeBuffer.putInt(0);// notsyncrequestnum

//         System.out.println("Stage: makeMzPropose --Mzpropose notsyncrequestnum: " + 0);
//         MzProposeBuffer.putInt(numBatchlistItems);
//         if (msgs != null) {
//             for (Mz_BatchListItem msg : msgs) {
//                 MzProposeBuffer.putInt(msg.NodeId);
//                 MzProposeBuffer.putInt(msg.StartHeight);
//                 MzProposeBuffer.putInt(msg.EndHeight);
//                 MzProposeBuffer.putInt(msg.usedful);
//                 System.out.println("Stage: makeMzPropose --Mzpropose BatchlistItem: --nd: " + msg.NodeId + " --st: "
//                         + msg.StartHeight + " --ed: " + msg.EndHeight + " --uf: " + msg.usedful);
//             }
//         }
//         // return the MzPropose
//         return MzProposeBuffer.array();
//     }

//     public byte[] makeMzPropose(List<Mz_BatchListItem> msgs, List<TOMMessage> list, int numNounces, long timestamp,
//             boolean useSignature) {

//         int numnotsyncreq = list==null ? 0 : list.size();
//         if (numnotsyncreq == 0)
//             return createMzBatch(msgs, numNounces, timestamp);

//         byte[][] messages = new byte[numnotsyncreq][]; // bytes of the message (or its hash)
//         byte[][] signatures = new byte[numnotsyncreq][]; // bytes of the message (or its hash)
//         Long[] recptime = new Long[numnotsyncreq];
//         // Fill the array of bytes for the messages/signatures being batched
//         int i = 0;
//         int totalreqSize = 0; // total size of the req being batched
//         for (TOMMessage req : list) {
//             messages[i] = req.serializedMessage;
//             signatures[i] = req.serializedMessageSignature;
//             recptime[i] = req.receptionTime;
//             totalreqSize += messages[i].length;
//             i++;
//         }

//         // return the batch
//         return createMzBatch(msgs, numNounces, timestamp, numnotsyncreq, totalreqSize, useSignature, messages,
//                 signatures, recptime);

//     }
// }
