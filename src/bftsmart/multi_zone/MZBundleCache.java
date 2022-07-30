package bftsmart.multi_zone;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;


public class MZBundleCache {
    private Map<Integer, Mz_Batch> bundleMap;    // batchId -> batch msg
    private ReentrantLock bundleLock;

    public MZBundleCache(){
        this.bundleMap = new TreeMap<>();
        this.bundleLock = new ReentrantLock();
    }

    public void addBundle(Mz_Batch batch) {
        this.bundleLock.lock();
        bundleMap.put(batch.getBatchId(), batch);
        this.bundleLock.unlock();
    }

    public boolean contains(int height) {
        this.bundleLock.lock();
        boolean res = this.bundleMap.containsKey(height);
        this.bundleLock.unlock();
        return res;
    }

    public Mz_Batch getAndRemove(int height) {
        this.bundleLock.lock();
        Mz_Batch bundle = null;
        if (this.bundleMap.containsKey(height))
            bundle = this.bundleMap.get(height);
        this.bundleMap.remove(height);
        this.bundleLock.unlock();
        return bundle;
    }

    public Mz_Batch getBundle(int height) {
        this.bundleLock.lock();
        Mz_Batch bundle = null;
        if (this.bundleMap.containsKey(height))
            bundle = this.bundleMap.get(height).clone();
        this.bundleLock.unlock();
        return bundle;
    }

    public int getMinimumBundleHeight() {
        this.bundleLock.lock();
        int height = -2;
        if (this.bundleMap.isEmpty() == false) {
            height = this.bundleMap.keySet().iterator().next();
        }
        this.bundleLock.unlock();
        return height;
    }


}
