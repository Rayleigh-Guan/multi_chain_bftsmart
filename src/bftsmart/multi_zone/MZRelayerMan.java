package bftsmart.multi_zone;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MZRelayerMan {
    private int myId;
    private int myZoneId;
    private Logger logger;
    private Map<Integer, HashSet<Integer>> zoneRelayerMap;
    private Map<Integer, HashSet<Integer>> relayerStripeMap;
    private ReentrantLock relayerLock;

    public MZRelayerMan(int nodeId, int zoneId) {
        this.myId = nodeId;
        this.myZoneId = zoneId;
        this.logger = LoggerFactory.getLogger(this.getClass());
        this.zoneRelayerMap = new HashMap<>();
        this.relayerStripeMap = new HashMap<>();
        this.relayerLock = new ReentrantLock();
    }

    public HashSet<Integer> getAllRelayers(){
        this.relayerLock.lock();
        HashSet<Integer> res = new HashSet<>();
        res.addAll(this.zoneRelayerMap.keySet());
        this.relayerLock.unlock();
        return res;
    }

    public HashSet<Integer> getZoneRelayers(int zoneId) {
        this.relayerLock.lock();
        HashSet<Integer> res = new HashSet<>();
        if (this.zoneRelayerMap.containsKey(zoneId))
            res.addAll(this.zoneRelayerMap.get(zoneId));
        this.relayerLock.unlock();
        return res;
    }

    boolean isRelayer(int nodeId) {
        this.relayerLock.lock();
        boolean res = this.relayerStripeMap.containsKey(nodeId) ;
        this.relayerLock.unlock();
        return res;
    }

    public HashSet<Integer> getRelayerStripe(int nodeId){
        HashSet<Integer> res = new HashSet<>();
        if (isRelayer(nodeId)) {
            this.relayerLock.lock();
            res.addAll(this.relayerStripeMap.get(nodeId)) ;
            this.relayerLock.unlock();
        }
        return res;
    }

    public int getNumOfRelayedStripes(int nodeId) {
        this.relayerLock.lock();
        int res = 0;
        if (isRelayer(nodeId)) 
            res = this.relayerStripeMap.get(nodeId).size();
        this.relayerLock.unlock();
        return res;
    }

    public int getRelayerNums(int zoneId) {
        this.relayerLock.lock();
        int cnt = 0;
        if (this.zoneRelayerMap.containsKey(zoneId))
            cnt = this.zoneRelayerMap.get(zoneId).size();
        this.relayerLock.unlock();
        return cnt;
    }

    public void addNewRelayer(int nodeId, int zoneId, ArrayList<Integer> stripes) {
        if (stripes == null || stripes.isEmpty())
            return;
        this.relayerLock.lock();
        if (isRelayer(nodeId))
            this.removeRelayer(nodeId, zoneId);
        if (this.zoneRelayerMap.containsKey(zoneId) == false)
            this.zoneRelayerMap.put(zoneId, new HashSet<>());
        this.zoneRelayerMap.get(zoneId).add(nodeId);
        this.relayerStripeMap.put(nodeId, new HashSet<>(stripes));
        this.relayerLock.unlock();
    }

    public void addRelayerStripes(int nodeId, int zoneId, ArrayList<Integer> stripes){
        if (stripes == null || stripes.isEmpty())
            return;
        this.relayerLock.lock();
        if (isRelayer(nodeId) == false)
            addNewRelayer(nodeId, zoneId, stripes);
        else
            this.relayerStripeMap.get(nodeId).addAll(stripes);
        this.relayerLock.unlock();
    }

    public boolean removeRelayer(int nodeId, int zoneId) {
        boolean res = false;
        if (isRelayer(nodeId)){
            this.relayerLock.lock();
            this.relayerStripeMap.remove(nodeId);
            if (this.zoneRelayerMap.containsKey(zoneId)){
                this.zoneRelayerMap.get(zoneId).remove(nodeId);
            }
            this.relayerLock.unlock();
            res = true;
        }
        return res;
    }

    public void deleteRelayerStripe(int nodeId, int zoneId, ArrayList<Integer> stripes){
        boolean res = isRelayer(nodeId);
        if(res == false)
            return;
        this.relayerLock.lock();
        this.relayerStripeMap.get(nodeId).removeAll(stripes);
        this.relayerCheck(nodeId, zoneId);
        this.relayerLock.unlock();
    }

    private void relayerCheck(int nodeId, int zoneId){
        this.relayerLock.lock();
        if (this.relayerStripeMap.containsKey(nodeId) == false || 
            this.relayerStripeMap.get(nodeId).isEmpty())
            this.removeRelayer(nodeId, zoneId);
        this.relayerLock.unlock();
    }

    public byte[] seralizeRelayer(MZMessageSeralizeTool mzmMsgSrlzTool, int zoneId) {
        this.relayerLock.lock();
        HashSet<Integer> relayerSet = this.getZoneRelayers(zoneId);
        byte[] content = mzmMsgSrlzTool.seralizeRelayNodes(relayerSet, this.relayerStripeMap);
        this.relayerLock.unlock();
        return content;
    }

    public HashSet<Integer> findStripeWithNoRelayer(int n){
        this.relayerLock.lock();
        HashSet<Integer> noRelayerStripeSet = new HashSet<>();
        for(int i = 0; i < n; ++i) 
            noRelayerStripeSet.add(i);

        HashSet<Integer> hs = new HashSet<Integer>();
        HashSet<Integer> myZoneRelayers = this.getZoneRelayers(this.myZoneId);
        for(Integer node: myZoneRelayers) 
            hs.addAll(this.getRelayerStripe(node));
        noRelayerStripeSet.removeAll(hs);
        this.relayerLock.unlock();
        return noRelayerStripeSet;
    }
}
