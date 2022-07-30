package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;
import bftsmart.tom.core.messages.TOMMessage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;

public class Mz_Propose implements Cloneable{
    public int blockHeight;
    public long timestamp;
    public long seed;
    public int numNounces;
    public RequestList reqList;
    public List<Mz_BatchListItem> bundleSliceList;

    public Mz_Propose(){
        reqList = null;
        bundleSliceList = null;
    }

    public Mz_Propose(int h, long ts, long seed, int numNonces, RequestList reqList, List<Mz_BatchListItem> bundleSliceList){
        this.blockHeight = h;
        this.timestamp = ts;
        this.seed = seed;
        this.numNounces = numNonces;
        this.reqList = reqList;
        this.bundleSliceList = bundleSliceList;
    }

    @Override
    public Mz_Propose clone(){
        List<Mz_BatchListItem> tmpList = new ArrayList<>();
        for(Mz_BatchListItem item: this.bundleSliceList)
            tmpList.add(item.clone());
        Mz_Propose clonePropose = new Mz_Propose(this.blockHeight, this.timestamp, this.seed, this.numNounces, this.reqList.clone(), tmpList);
        return clonePropose;
    }

    @Override
    public String toString(){
        String reqListStr = (reqList == null) ? "|":reqList.toString();
        String bundleListStr = (bundleSliceList == null) ? "|":bundleSliceList.toString();
        return String.format("blockheight:%d_timestamp:%d_nounce:%d_seed:%d_bundleSliceList:%s_reqList:%s", 
            blockHeight, timestamp, numNounces, seed, bundleListStr, reqListStr);
    }

    public static byte[] seralizeMZPropose(Mz_Propose mzpropose, boolean useSig){
        

        // blockHeight, timestamp, seed, numNonces, reqList's length, signature's length, bundleSliceList's length
        int prefixSize = 4 + 8 + 8 + 4 + 4 + 4 + 4; 
        int reqLen = (mzpropose.reqList != null ? mzpropose.reqList.size() : 0);
        // compute request and its signature size
        int reqSize = 0, sigSize = 0;
        for (int i = 0; i < reqLen; ++i) {
            TOMMessage req = mzpropose.reqList.get(i);
            reqSize += req.serializedMessage != null ? req.serializedMessage.length : 0;
            sigSize += req.serializedMessageSignature != null ? req.serializedMessageSignature.length : 0;
        }
        int receptiontimeSize = reqLen * Long.BYTES;
        int reqLenSize = reqLen * Integer.BYTES;
        int bundleSliceLen = (mzpropose.bundleSliceList != null ? mzpropose.bundleSliceList.size() : 0);
        int totalSize = prefixSize + reqLenSize + reqSize + receptiontimeSize + bundleSliceLen * Integer.BYTES * 4;
        if (useSig)
            totalSize += sigSize + reqLen * Integer.BYTES;
        // System.out.println("Stage: makeMzPropose --Mzpropose size: " + totalSize);
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(mzpropose.blockHeight);   // blockHeight
        buffer.putLong(mzpropose.timestamp);    // timestamp
        buffer.putLong(mzpropose.seed);         // seed
        buffer.putInt(mzpropose.numNounces);    // numNonces
        buffer.putInt(reqLen);                  // reqList's length
        buffer.putInt((useSig ? reqLen: 0));    // signature's length        
        buffer.putInt(bundleSliceLen);          // bundleSliceList's length

        // System.out.println("Stage: makeMzPropose --Mzpropose timestamp: " + mzpropose.timestamp + "block height: " + mzpropose.blockHeight);

        // seralize requests with its signature and reception time
        for(int i = 0; i < reqLen; ++i) {
            TOMMessage req = mzpropose.reqList.get(i);
            buffer.putInt(req.serializedMessage.length);
            buffer.put(req.serializedMessage);
            if (useSig) {
                buffer.putInt(req.serializedMessageSignature.length);
                buffer.put(req.serializedMessageSignature);
            }
            buffer.putLong(req.receptionTime);
        }  
        
        // seralize bundleslices
        for (int i = 0; i < bundleSliceLen; ++i) {
            Mz_BatchListItem msg = mzpropose.bundleSliceList.get(i);
            buffer.putInt(msg.NodeId);
            buffer.putInt(msg.StartHeight);
            buffer.putInt(msg.EndHeight);
            buffer.putInt(msg.usedful);
            // System.out.println("Stage: makeMzPropose --Mzpropose BatchlistItem: --nd: " + msg.NodeId + " --st: "
            //         + msg.StartHeight + " --ed: " + msg.EndHeight + " --uf: " + msg.usedful);
        }
        return buffer.array();
    }

    public static Mz_Propose deseralizeMZPropose(byte[] batch, boolean useSig) {
        ByteBuffer buff = ByteBuffer.wrap(batch);
        Mz_Propose mzpropose = new Mz_Propose();
        // deseralize prefix
        mzpropose.blockHeight = buff.getInt();
        mzpropose.timestamp = buff.getLong();
        mzpropose.seed = buff.getLong();
        mzpropose.numNounces = buff.getInt();
        int reqLen = buff.getInt();
        int sigLen = buff.getInt();
        int bundleSliceLen = buff.getInt();


        // System.out.printf("Stage: MzProposedeserialisemsg-- blockheight: %d, timestamp: %d, --numNounces: %d\n",mzpropose.blockHeight, mzpropose.timestamp , mzpropose.numNounces);
        
        // deseralize requests with its signature and reception time
        mzpropose.reqList = new RequestList();
        while(reqLen > 0) {
            int reqSize = buff.getInt();
            byte[] req = new byte[reqSize];
            buff.get(req);
            int sigSize = 0;
            byte[] sig = null;
            if (sigLen > 0){
                sigSize = buff.getInt();
                sig = new byte[sigSize];
                buff.get(sig);
            }
            long receptime = buff.getLong();

            try {
                DataInputStream ois = new DataInputStream(new ByteArrayInputStream(req));
                TOMMessage tm = new TOMMessage();
                tm.rExternal(ois);
                ois.close();
                tm.serializedMessage = req;
                tm.serializedMessageSignature = sig;
                tm.receptionTime = receptime;
                mzpropose.reqList.add(tm);
            } catch (Exception e) {
                System.out.println("Failed to deserialize MZpropose"+ e);
            }
            --reqLen;
        }

        // System.out.println("Stage: MzProposedeserialisemsg --bundleSliceList: ");
        
        // deseralize bundleSlice
        mzpropose.bundleSliceList = new ArrayList<>();
        while (bundleSliceLen > 0) {
            Mz_BatchListItem bundleSlice = new Mz_BatchListItem(buff.getInt(), buff.getInt(),buff.getInt(), buff.getInt());
            mzpropose.bundleSliceList.add(bundleSlice);

            // System.out.println("Stage: MzProposedeserialisemsg --bundleSlice: node:" + bundleSlice.NodeId
            // + " start height: " + bundleSlice.StartHeight + " endheight: " + bundleSlice.EndHeight
            // + " usebatch: " + bundleSlice.usedful);
            
            --bundleSliceLen;
        }

        return mzpropose;
    }

}
