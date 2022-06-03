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
    private int stripeId;           // stripeId
    private byte[] stripe;


    public MZStripeMessage(){

    }

    public MZStripeMessage(int from, int batchChainId, int batchHeight, int stripeId, byte[] stripe) {
        super(from);
        this.batchChainId = batchChainId;
        this.batchHeight = batchHeight;
        this.stripeId = stripeId;
        this.stripe = stripe;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(batchChainId);
        out.writeInt(this.batchHeight);
        out.writeInt(this.stripeId);
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
        this.stripeId = in.readInt();
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
        return String.format("BatchChain: %d, height: %d, stripeId: %d, stripeLen: %d\n",
                this.batchChainId, this.batchHeight, this.stripeId, this.stripe.length);
    }
    
}
