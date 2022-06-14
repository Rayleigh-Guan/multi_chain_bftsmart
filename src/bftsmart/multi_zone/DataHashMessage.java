package bftsmart.multi_zone;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import bftsmart.communication.SystemMessage;
import bftsmart.tom.util.TOMUtil;
import java.util.Arrays;


// This denotes a message of data hash.
public class DataHashMessage extends SystemMessage{
    private int type;
    private long ts;
    private int[] appendix;
    private byte[] dataHash;
    
    
    public DataHashMessage() {

    }

    /**
     * Constructor
     * 
     * @param from      replica that creates this message
     * @param type      type of the message
     * @param appendix  extra data info
     * @param ts        timestamp of leader change and synchronization
     * @param hash      the data hash
     */
    public DataHashMessage(int from, int type, long ts, int[] appendix, byte[] hash) {
        super(from);
        this.type = type;
        this.ts = ts;
        this.appendix = appendix;
        this.dataHash = hash;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(type);
        out.writeLong(ts);
        int len = this.appendix == null ? 0 : this.appendix.length;
        out.writeInt(len);
        for(int i = 0; i < len; ++i)
            out.writeInt(this.appendix[i]);
        if (dataHash == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(dataHash.length);
            out.write(dataHash);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.type = in.readInt();
        this.ts = in.readLong();
        int len = in.readInt();
        if (len > 0){
            this.appendix = new int[len];
            for (int i = 0; i < len; ++i)
                this.appendix[i] = in.readInt();
        }
        int toRead = in.readInt();
        if (toRead != -1) {
            this.dataHash = new byte[toRead];
            do {
                toRead -= in.read(this.dataHash, this.dataHash.length - toRead, toRead);
            } while (toRead > 0);
        }
    }

        /**
     * Get type of message
     * 
     * @return type of message
     */
    public int getType() {
        return this.type;
    }


    public void setType(int t){
        this.type = t;
    }

    public void setTimeStamp(long ts){
        this.ts = ts;
    }

    public long getTimeStamp() {
        return this.ts;
    }

    public int[] getAppendix() {
        return this.appendix;
    }

    public void setAppendix1(int[] a) {
        this.appendix = a;
    }

    /**
     * Obter data of the message
     * 
     * @return data of the message
     */
    public byte[] getDataHash() {
        return this.dataHash;
    }

    public void setValue(byte[] value){
        this.dataHash = value;
    }

    public String getVerboseType(){
        if (this.type == TOMUtil.DH_BLODKHASH)
            return "DH_BLODKHASH";
        else if (this.type == TOMUtil.DH_GETBLOCK)
            return "DH_GETBLOCK";
        else if (this.type == TOMUtil.DH_STRIPEHASH)
            return "DH_STRIPEHASH";
        else if (this.type == TOMUtil.DH_GETSTRIPE)
            return "DH_GETSTRIPE";
        else
            return "NULL";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataHashMessage msg = (DataHashMessage) o;
        return Arrays.equals(this.dataHash, msg.dataHash) && this.appendix == msg.appendix;
    }
    
    @Override
    public String toString() {
        return String.format("sender:%d, type:%s, timestamp:%d, hash:%s", getSender(), getVerboseType(), getTimeStamp(),MZNodeMan.bytesToHex(dataHash, 8));
    }
}
