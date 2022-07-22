package bftsmart.multi_zone;

import bftsmart.communication.SystemMessage;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import bftsmart.tom.util.TOMUtil;

import java.util.Objects;

/**
 * Message used to denote a erasure encoding message
 * ______
 * |    |<-----
 * ------
 * 
 */

public class MZStripeMessage extends SystemMessage{

    private int batchChainId;       // batchChainId is the producer's ID. 
    private int batchHeight;
    private int totalLen;           // total length of all stripes' data
    private int stripeId;           // stripeId
    private int dataLen;            // the valid data length
    private byte[] stripe;

    public MZStripeMessage(){

    }

    public MZStripeMessage(int from, int batchChainId, int batchHeight, int totalLen, int stripeId, int dataLen, byte[] stripe) {
        super(from);
        this.batchChainId = batchChainId;
        this.batchHeight = batchHeight;
        this.totalLen = totalLen;
        this.stripeId = stripeId;
        this.dataLen = dataLen;
        this.stripe = stripe;
        assert(dataLen <= stripe.length);
    }

    public int getHeight(){
        return this.batchHeight;
    }

    public int getBatchChainId(){
        return this.batchChainId;
    }

    public int getStripeId(){
        return this.stripeId;
    }

    public int getValidDataLen(){
        return this.dataLen;
    }

    public int getTotalLen(){
        return this.totalLen;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(this.batchChainId);
        out.writeInt(this.batchHeight);
        out.writeInt(this.totalLen);
        out.writeInt(this.stripeId);
        out.writeInt(this.dataLen);
        if(stripe == null) {
            out.writeInt(-1);
        }else {
            out.writeInt(this.stripe.length);
            out.write(this.stripe);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.batchChainId = in.readInt();
        this.batchHeight = in.readInt();
        this.totalLen = in.readInt();
        this.stripeId = in.readInt();
        this.dataLen = in.readInt();
        int toRead = in.readInt();
        if (toRead != -1) {
            stripe = new byte[toRead];
            do {
                toRead -= in.read(stripe, stripe.length - toRead, toRead);
            } while (toRead > 0);
        }
    }

    public byte[] getStripe(){
        return this.stripe;
    }

    public int getType(){
        return TOMUtil.STRIPE;
    }

    public String getVerboseType(){
        return "STRIPE";
    }

    @Override
    public int hashCode(){
        return Objects.hash(batchChainId, batchHeight, stripeId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MZStripeMessage msg = (MZStripeMessage) o;
        return this.batchChainId == msg.batchChainId && 
                this.batchHeight == msg.batchHeight &&
                this.stripeId == msg.stripeId &&
        Objects.equals(stripe, msg.stripe) ;
    }

    @Override
    public String toString(){
        return String.format("type=STRIPE_BatchChain_%d_height_%d_stripeId_%d_totalLen_%d_stripeLen_%d_validDataLen_%d_Sender_%d",
            this.batchChainId, this.batchHeight, this.stripeId, this.totalLen, this.stripe.length, this.dataLen,this.getSender());
    }
    
}
