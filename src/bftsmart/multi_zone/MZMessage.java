package bftsmart.multi_zone;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import bftsmart.communication.SystemMessage;
import bftsmart.tom.util.TOMUtil;

import java.util.Objects;

/**
 * Message used for multi_zone network construction and data dissemination
 */
public class MZMessage extends SystemMessage{
    
    private int type;                   // message type
    private int zoneId;                 // zoneId of sender
    private long ts;                    // timestampe
    private int msgCreator;             // msgcreator
    private byte[] value = null;        // data


    /**
     * Creates a MZMessage message. Not used. 
     * This default constructor can't be removed!!!
     */
    public MZMessage(){

    }

    /**
     * Constructor
     * 
     * @param from    replica that creates this message
     * @param type    type of the message
     * @param ts      timestamp of leader change and synchronization
     * @param payload dada that comes with the message
     * @param value the content
     */
    public MZMessage(int from, int type, long ts, int zoneId, byte[] value) {
        super(from);
        this.type = type;
        this.ts = ts;
        this.zoneId = zoneId;
        this.value = value;
        this.msgCreator = from;
    }

    /**
     * Constructor
     * 
     * @param from    replica that creates this message
     * @param type    type of the message 
     * @param ts      timestamp of leader change and synchronization
     * @param payload dada that comes with the message
     */
    public MZMessage(int from, int type, long ts, int zoneId) {
        
        this(from, type, ts, zoneId, null);
        
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(msgCreator);
        out.writeInt(type);
        out.writeInt(zoneId);
        out.writeLong(ts);
        if (value == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(value.length);
            out.write(value);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        msgCreator = in.readInt();
        type = in.readInt();
        zoneId = in.readInt();
        ts = in.readLong();
        int toRead = in.readInt();
        if (toRead != -1) {
            value = new byte[toRead];
            do {
                toRead -= in.read(value, value.length - toRead, toRead);
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

    public void setZoneId(int z) {
        this.zoneId = z;
    }

    public void setTimeStamp(long ts){
        this.ts = ts;
    }

    /**
     * Get zoneId of message
     * 
     * @return zoneId of message
     */
    public int getZoneId(){
        return this.zoneId;
    }

    /**
     * Get timestamp of leader change and synchronization
     * 
     * @return timestamp of leader change and synchronization
     */
    public long getTimeStamp() {
        return this.ts;
    }

    /**
     * Obter data of the message
     * 
     * @return data of the message
     */
    public byte[] getValue() {
        return this.value;
    }

    public void setValue(byte[] value){
        this.value = value;
    }

    public int getMsgCreator(){
        return this.msgCreator;
    }

    
    public void setSender(int id){
        this.sender = id;
    }


    @Override
    public int hashCode() {
        return Objects.hash(msgCreator, type, ts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MZMessage msg = (MZMessage) o;
        return msgCreator == msg.msgCreator && Objects.equals(type, msg.type) && Objects.equals(this.ts, msg.ts);
    }

    public String getVerboseType(){
        if (getType() == TOMUtil.GET_RELAY_NODE)
            return "GET_RELAY_NODE";
        else if (getType() == TOMUtil.RELAY_NODES)
            return "RELAY_NODES";
        else if (getType() == TOMUtil.SUBSCRIBE)
            return "SUBSCRIBE";
        else if (getType() == TOMUtil.ACCEPT_SUBSCRIBE)
            return "ACCEPT_SUBSCRIBE";
        else if (getType() == TOMUtil.UNSUBSCRIBE)
            return "UNSUBSCRIBE";
        else if (getType() == TOMUtil.STRIPE)
            return "STRIPE";
        else if (getType() == TOMUtil.RELAYER)
            return "RELAYER";
        else if (getType() == TOMUtil.LATENCY_DETECT)
            return "LATENCY_DETECT";
        else if (getType() == TOMUtil.INV)
            return "INV";
        return null;
    }

    @Override
    public String toString() {
        return String.format("creator:%d, sender:%d, type:%s, timestamp:%d, zoneId:%d",getMsgCreator(), getSender(), getVerboseType(), getTimeStamp(), getZoneId());
    }
}