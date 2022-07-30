package bftsmart.multi_zone;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantLock;

public class MZInFlightMan {
    private Map<ArrayList<Integer>, ArrayList<Long>> msgInFlightMap;
    private ReentrantLock inFlightLock;

    private Map<ArrayList<Integer>, ArrayList<Integer>> downloadSourceMap;
    private ReentrantLock downloadLock;

    public MZInFlightMan(){
        this.msgInFlightMap = new HashMap<>();
        this.inFlightLock = new ReentrantLock();
        this.downloadSourceMap = new HashMap<>();
        this.downloadLock = new ReentrantLock();
    }

    public boolean contains(ArrayList<Integer> arr) {
        boolean res = false;
        this.inFlightLock.lock();
        if (this.msgInFlightMap.containsKey(arr)) {
            res = true;
        }
        this.inFlightLock.unlock();
        return res;
    }

    public ArrayList<Long> getInflightInfo(ArrayList<Integer> arr) {
        ArrayList<Long> res = new ArrayList<>();
        this.inFlightLock.lock();
        if (this.msgInFlightMap.containsKey(arr)) {
            res.addAll(this.msgInFlightMap.get(arr));
        }
        this.inFlightLock.unlock();
        return res;
    }

    public void addMsgInflight(ArrayList<Integer> arr, ArrayList<Long> record) {
        this.inFlightLock.lock();
        this.msgInFlightMap.put(arr, record);
        this.inFlightLock.unlock();
        int source = Integer.valueOf(String.valueOf(record.get(0)));
        this.addDownloadSource(arr, source);
    }

    public Set<ArrayList<Integer>> getMsgInFlight(){
        this.inFlightLock.lock();
        HashSet<ArrayList<Integer>> msgSet = new HashSet<>();
        msgSet.addAll(this.msgInFlightMap.keySet());
        this.inFlightLock.unlock();
        return msgSet;
    }

    public void removeMsgSet(ArrayList<Integer> arr) {
        this.inFlightLock.lock();
        this.msgInFlightMap.remove(arr);
        this.inFlightLock.unlock();
    }

    public void addDownloadSource(ArrayList<Integer> arr, int nodeId) {
        this.downloadLock.lock();
        if (this.downloadSourceMap.containsKey(arr) == false)
            this.downloadSourceMap.put(arr, new ArrayList<Integer>());
        ArrayList<Integer> sourceList = this.downloadSourceMap.get(arr);
        if (sourceList.contains(nodeId) == false)
            sourceList.add(nodeId);
        this.downloadLock.unlock();
    }

    public int getADownloadSource(ArrayList<Integer> arr) {
        ArrayList<Long> remoteSender = this.getInflightInfo(arr);
        int res = -1;
        if (remoteSender.isEmpty() == false)
            res = Integer.valueOf(String.valueOf(remoteSender.get(0)));
        this.downloadLock.lock();
        ArrayList<Integer> candidateList = new ArrayList<>();
        if (this.downloadSourceMap.containsKey(arr)) {
            candidateList.addAll(this.downloadSourceMap.get(arr));
        }
        this.downloadLock.unlock();
        if (candidateList.isEmpty() == false)
            Collections.shuffle(candidateList);
            res = candidateList.get(0);
        return res;
    }
}
