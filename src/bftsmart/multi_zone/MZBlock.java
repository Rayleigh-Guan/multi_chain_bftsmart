package bftsmart.multi_zone;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import bftsmart.communication.SystemMessage;
import bftsmart.consensus.messages.ConsensusMessage;



public class MZBlock extends SystemMessage{
    // ecah proof is HashMap<Integer, byte[]> macVector = new HashMap<>();
    private Set<ConsensusMessage> proof;
    private ConsensusMessage mzpropose;
    private byte[] value = null;
    
    public MZBlock(Set<ConsensusMessage> proof, ConsensusMessage mzpropose) {
        // this.proof = proof;
        // this.mzpropose = mzpropose;
        // MzProposeReader mzproposeReader = new MzProposeReader(msg.getValue(),controller.getStaticConf().getUseSignatures() == 1);
        // Mz_Propose mz_propose = mzproposeReader.deserialisemsg();
    }

    // public byte[] seralizeBlock(){
    //     if (value != null) return value;

    // }

    public byte[] getValue(){
        return value;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        if (value ==  null){
            out.writeInt(-1);
        }
        else {
            out.writeInt(value.length);
            out.write(value);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        int toRead  = in.readInt();
        if (toRead != -1) {
            value = new byte[toRead];
            do {
                toRead -= in.read(value, value.length - toRead, toRead);
            } while (toRead > 0);
        }
    }
}
