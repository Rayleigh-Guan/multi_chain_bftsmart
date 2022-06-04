package bftsmart.multi_zone;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import bftsmart.communication.SystemMessage;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.tom.util.MzProposeReader;



public class MZBlock extends SystemMessage{
    // ecah proof is HashMap<Integer, byte[]> macVector = new HashMap<>();
    // private Set<ConsensusMessage> proof;
    
    private byte[] blockHash ;
    private byte[] blockContent;
    private Mz_Propose propose;
    
    public MZBlock(int from, byte[] blockHash, byte[] blockContent) {
        super(from);
        this.blockHash = blockHash;
        this.blockContent = blockContent;
        this.propose = null;
    }

    public MZBlock(int from, byte[] blockHash, Mz_Propose propose) {
        super(from);
        this.blockHash = blockHash;
        this.propose = propose;
        this.blockContent = null;
    }

    public byte[] getBlockHash(){
        return this.blockHash;
    }

    public Mz_Propose deseralizePropose(boolean useSig){
        if (this.propose == null){
            MzProposeReader mzproposeReader = new MzProposeReader(blockContent, useSig);
            this.propose = mzproposeReader.deserialisemsg();    
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
}
