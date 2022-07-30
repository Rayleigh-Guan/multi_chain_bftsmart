
package bftsmart.multi_zone;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MZMessageSeralizeTool{
    private Random rnd;

    public MZMessageSeralizeTool(long seed){
        rnd = new Random(seed);
    }

    /**
     * seralize SUBSCRIBE message
     * @param subStripes
     * @return
     */
    public byte[] seralizeSubscribe(Set<Integer> subStripes) {
        assert(subStripes != null);
        int nStripes = subStripes != null ? subStripes.size(): 0;
        int size = (nStripes + 1) * Integer.BYTES;
        ByteBuffer buff = ByteBuffer.allocate(size);
        buff.putInt(nStripes);
        if (subStripes != null && !subStripes.isEmpty()) {
            for (Integer stripe: subStripes)
                buff.putInt(stripe);
        }
        return buff.array();
    }

    /**
     * deseralize SUBSCRIBE message
     * @param data
     * @return
     */
    public  ArrayList<Integer> deseralizeSubscribe(byte[] data) {
        assert(data != null);
        ByteBuffer buff = ByteBuffer.wrap(data);
        ArrayList<Integer> subStripes = new ArrayList<>();
        int nStripes = buff.getInt();
        while (nStripes > 0) {
            subStripes.add(buff.getInt());
            nStripes -= 1;
        }
        return subStripes;
    }


    /**
     * seralize message RELAY_NODES
     * relayerNum RelayerId1 StripeNum Stripe1 Stripe2... , RelayerId2 StripeNum2 ...
     */
    public byte[] seralizeRelayNodes(Set<Integer> relayerSet, Map<Integer, HashSet<Integer>> relayStripeMap) {
        int nRelayers = relayerSet!=null? relayerSet.size():0;
        int num = 1 + nRelayers + nRelayers;  // relayerNum RelayerId1 StripeNum1, RelayerId2, StripeNum2
        int stripeNum = 0;
        if (relayerSet!= null) {
            for (Integer relayerId: relayerSet) {
                assert(relayStripeMap.containsKey(relayerId) == true):"Error in seralizeRelayNodes";
                stripeNum += relayStripeMap.get(relayerId).size();
            }
        }
        ByteBuffer buff = ByteBuffer.allocate((num + stripeNum)*Integer.BYTES);
        buff.putInt(nRelayers);
        if (nRelayers!=0 && stripeNum !=0) {
            for (Integer relayerId: relayerSet){
                buff.putInt(relayerId);
                buff.putInt(relayStripeMap.get(relayerId).size());
                for (Integer stripeId : relayStripeMap.get(relayerId)) {
                    buff.putInt(stripeId);
                }
            }
        }
        return buff.array();
    }

    /**
     * deseralize RELAY_NODES message
     * @param data
     * @return
     */
    public HashMap<Integer, HashSet<Integer>> deseralizeRelayNodes(byte[] data){
        assert(data != null);
        HashMap<Integer, HashSet<Integer>> relayStripeMap = new HashMap<>();
        ByteBuffer buff = ByteBuffer.wrap(data);
        int nRelayers = buff.getInt();
        while (nRelayers > 0) {
            int relayerId = buff.getInt();
            relayStripeMap.put(relayerId, new HashSet<Integer>());
            int stripeNum = buff.getInt();
            while (stripeNum > 0) {
                relayStripeMap.get(relayerId).add(buff.getInt());
                stripeNum -= 1;
            }
            nRelayers -= 1;
        }
        return relayStripeMap;
    }

    /**
     * seralize message RELAYER
     * @param stripes
     * @return
     */
    public byte[] seralizeRelayer(Set<Integer> stripes) {
        if (stripes == null)
            stripes = new HashSet<>();
        int size = stripes.size() + 1;
        ByteBuffer buff = ByteBuffer.allocate(size * Integer.BYTES);
        buff.putInt(stripes.size());
        for (Integer stripeId: stripes)
            buff.putInt(stripeId);
        return buff.array();
    }

    /**
     * deseralize message RELAYER
     * @param data
     * @return
     */
    public HashSet<Integer> deseralizeRelayer(byte[] data){
        assert(data!=null);
        ByteBuffer buff = ByteBuffer.wrap(data);
        HashSet<Integer> stripes = new HashSet<>();
        int nStripe = buff.getInt();
        while (nStripe > 0) {
            stripes.add(buff.getInt());
            nStripe -= 1;
        }
        return stripes;
    }

    public byte[] seralizeLatencyDetect(ArrayList<Object> data) {
        assert(data != null):"data is null";
        int size = Integer.BYTES  + Integer.BYTES * data.size()/2 + Long.BYTES*data.size()/2;
        ByteBuffer buff = ByteBuffer.allocate(size);
        buff.putInt(data.size());
        int i = 0;
        while (i < data.size()) {
            buff.putInt((int)data.get(i++));
            buff.putLong((long)data.get(i++));
        }
        return buff.array();
    }

    public ArrayList<Object> deseraliceLatencyDetect(byte[] data){
        ArrayList<Object> content = new ArrayList<>();
        ByteBuffer buff = ByteBuffer.wrap(data);
        int len = buff.getInt();
        while(len > 0) {
            content.add(buff.getInt());
            --len;
            content.add(buff.getLong());
            --len;
        }
        return content;
    }

    public byte[] seralizeRejectSubscribe(ArrayList<Integer> neighbors, ArrayList<Integer> stripeList) {
        int neighborLen = neighbors == null ? 0: neighbors.size();
        int stripeLen = stripeList == null ? 0: stripeList.size();
        int size = Integer.BYTES * (neighborLen + stripeLen + 2);
        ByteBuffer buff = ByteBuffer.allocate(size);
        buff.putInt(neighborLen);
        int i = 0, j = 0;
        while (i < neighborLen) {
            buff.putInt(neighbors.get(i++));
        }
        buff.putInt(stripeLen);
        while (j < stripeLen){
            buff.putInt(stripeList.get(j++));
        }
        return buff.array();
    }

    public ArrayList<Integer> deseralizeRejectSubscribe(byte[] data, ArrayList<Integer> neighbors, ArrayList<Integer> stripeList){
        
        ByteBuffer buff = ByteBuffer.wrap(data);
        int neighborLen = buff.getInt();
        while(neighborLen > 0) {
            neighbors.add(buff.getInt());
            --neighborLen;
        }
        int stripeLen = buff.getInt();
        while(stripeLen > 0){
            stripeList.add(buff.getInt());
            --stripeLen;
        }

        return neighbors;
    }
}