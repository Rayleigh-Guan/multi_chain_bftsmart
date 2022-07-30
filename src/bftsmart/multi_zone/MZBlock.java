package bftsmart.multi_zone;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import bftsmart.communication.SystemMessage;




public class MZBlock extends SystemMessage implements Cloneable{
    // ecah proof is HashMap<Integer, byte[]> macVector = new HashMap<>();
    // private Set<ConsensusMessage> proof;
    
    private byte[] blockHash ;                  // block hash
    private byte[] blockContent;                // seralized Mz_Propose
    private Mz_Propose propose;                 // Mz_Propose to rebuild original block
    
    public MZBlock() {
        this.propose = null;
        this.blockHash = null;
        this.blockContent = null;
    }

    public MZBlock(int from, byte[] blockHash, Mz_Propose propose, byte[] seralizedPropose) {
        super(from);
        this.blockHash = blockHash;
        this.propose = propose;
        this.blockContent = seralizedPropose;
    }

    @Override
    public MZBlock clone(){
        Mz_Propose tmpPropose = null;
        if (this.propose != null)
            tmpPropose = this.propose.clone();
        MZBlock blk = new MZBlock(this.getSender(), this.blockHash.clone(), tmpPropose, this.blockContent.clone());
        return blk;
    }


    public byte[] getBlockHash(){
        return this.blockHash;
    }

    public int getBlockHeight(){
        int height = -2;
        if (this.propose != null) {
            height = this.propose.blockHeight;
        }
        return height;
    }


    public byte[] getBlockContent(){
        return this.blockContent;
    }

    public String getBlockHashStr(int digit) {
        String str = new String(this.blockHash);
        digit = Math.min(digit, this.blockHash.length);
        return str.substring(0, digit);
    }

    public Mz_Propose deseralizePropose(boolean useSig){
        if (this.propose == null){
            this.propose = Mz_Propose.deseralizeMZPropose(this.blockContent, useSig);
        }
        return this.propose;
    }

    public Mz_Propose getPropose(){
        return this.propose;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        if (blockHash ==  null)
            out.writeInt(-1);
        else {
            out.writeInt(blockHash.length);
            out.write(blockHash);
        }

        if (blockContent == null)
            out.writeInt(-1);
        else{
            out.writeInt(blockContent.length);
            out.write(blockContent);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        int toRead = in.readInt();
        if (toRead != -1) {
            this.blockHash = new byte[toRead];
            do {
                toRead -= in.read(this.blockHash, this.blockHash.length - toRead, toRead);
            } while (toRead > 0);
        }
        toRead = in.readInt();
        if (toRead != -1) {
            this.blockContent = new byte[toRead];
            do {
                toRead -= in.read(this.blockContent, this.blockContent.length - toRead, toRead);
            } while (toRead > 0);
        }
    }

    @Override
    public String toString(){
        return String.format("blockHash: %s, propose: %s", MZNodeMan.bytesToHex(blockHash, 16), propose!=null ? propose.toString():"[EMPTY PROPOSE]");
    }
}
