package bftsmart.multi_zone;

import java.util.Map;

import java.util.HashMap;
import java.util.HashSet;


import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MZSubscribeMan {
    public int noSender;
    private int myId;
    private Logger logger;
    private ReentrantLock subscribeLock;

    private Map<Integer, HashSet<Integer>> subscribeMap;                // nodeId --> stripeId I subscribed
    private Map<Integer, Integer> stripeSenderMap;                      // stripeId --> nodeId send to me

    private Map<Integer, HashSet<Integer>> subscribeMsgSentMap;         // nodeId --> subscribed stripe I send to
    private Map<Integer, ArrayList<Long>> stripeSubedNodeAndTimeMap; // stripeId --> node I subscribed to and time
    private ReentrantLock msgLock;

    private Map<Integer, HashSet<Integer>> stripeAvailableNode;         // stripeId --> node that can forwad the stripe
    private ReentrantLock downloaderLock;


    public MZSubscribeMan(int nodeId) {
        this.myId = nodeId;
        this.subscribeMap = new HashMap<>();
        this.stripeSenderMap = new HashMap<>();
        this.subscribeLock = new ReentrantLock();

        this.subscribeMsgSentMap = new HashMap<>();
        this.stripeSubedNodeAndTimeMap = new HashMap<>();
        this.msgLock = new ReentrantLock();
        
        this.stripeAvailableNode = new HashMap<>();
        this.downloaderLock = new ReentrantLock();

        this.logger = LoggerFactory.getLogger(this.getClass());

        this.noSender = -1;
    }

    public boolean nodeOffline(int nodeId) {
        boolean res = false;
        this.subscribeLock.lock();
        if (this.subscribeMap.containsKey(nodeId)) {
            HashSet<Integer> subedStripeSet = this.getSubscribedStripe(nodeId);
            this.removeSubscribe(nodeId, subedStripeSet);
            res = true;
        }

        this.downloaderLock.lock();
        for(int stripe: this.stripeAvailableNode.keySet())
            this.stripeAvailableNode.get(stripe).remove(nodeId);
        this.downloaderLock.unlock();
        this.subscribeLock.unlock();
        return res;
    }

    public void removeSubscribe(int nodeId, HashSet<Integer> stripeSet) {
        if (stripeSet == null || stripeSet.isEmpty())
            return;
        this.subscribeLock.lock();
        // update subscribeMap
        for(int stripe: stripeSet) {
            int sender = this.getStripeSender(stripe);
            if (sender == nodeId) {
                this.stripeSenderMap.remove(stripe);
            }
            else {
                logger.warn("stripe {}'s' current sender is {} not {}", stripe, sender, nodeId);
            }
        } 
        if(this.subscribeMap.containsKey(nodeId)) {
            this.subscribeMap.get(nodeId).removeAll(stripeSet);
            if (this.subscribeMap.get(nodeId).isEmpty())
                this.subscribeMap.remove(nodeId); 
        }
        this.subscribeLock.unlock();
    }

    private void addStripeSender(int sender, int stripeId) {
        this.subscribeLock.lock();
        if (this.subscribeMap.containsKey(sender) == false)
            this.subscribeMap.put(sender, new HashSet<Integer>());
        this.subscribeMap.get(sender).add(stripeId);
        this.stripeSenderMap.put(stripeId, sender);
        this.subscribeLock.unlock();
    }

    public void addSubscribe(int sender, HashSet<Integer> stripeSet) {
        if (stripeSet == null || stripeSet.isEmpty())
            return;
        this.subscribeLock.lock();
        for(int stripeId: stripeSet)
            this.addStripeSender(sender, stripeId);
        this.subscribeLock.unlock();
    }

    public int getStripeSender(int stripe) {
        int sender = this.noSender;
        this.subscribeLock.lock();
        if (this.stripeSenderMap.containsKey(stripe))
            sender = this.stripeSenderMap.get(stripe);
        this.subscribeLock.unlock();
        return sender;
    }

    public HashMap<Integer, Integer> getStripeSender(HashSet<Integer> stripeSet) {
        HashMap<Integer, Integer> res = new HashMap<>();
        if (stripeSet == null || stripeSet.isEmpty())
            return res;
        this.subscribeLock.lock();
        for(int stripe: stripeSet) {
            res.put(stripe, this.getStripeSender(stripe));
        }
        this.subscribeLock.unlock();
        return res;
    }

    public HashSet<Integer> getSubscribedStripe(int nodeId) {
        this.subscribeLock.lock();
        HashSet<Integer> res = new HashSet<>();
        if (this.subscribeMap.containsKey(nodeId))
            res.addAll(subscribeMap.get(nodeId));
        this.subscribeLock.unlock();
        return res;
    }

    public HashSet<Integer> getStripeCanSubscribe(int startStripeId, int maxStripeId) {
        HashSet<Integer> hs = new HashSet<>();
        this.subscribeLock.lock();
        for (int i = startStripeId; i < maxStripeId; ++i) {
            if (shouldISubscribe(i))
                hs.add(i);
        }
        this.subscribeLock.unlock();
        return hs;
    }

    public String stripeSenderMapToString(){
        this.subscribeLock.lock();
        String str = this.stripeSenderMap.toString();
        this.subscribeLock.unlock();
        return str;
    }

    public String subscribeMapToString(){
        this.subscribeLock.lock();
        String str = this.subscribeMap.toString();
        this.subscribeLock.unlock();
        return str;
    }

    public void addSubscribeMsg(int nodeId, HashSet<Integer> stripeSet) {
        if (stripeSet == null || stripeSet.isEmpty())
            return;
        this.msgLock.lock();
        if (this.subscribeMsgSentMap.containsKey(nodeId) == false) {
            this.subscribeMsgSentMap.put(nodeId, stripeSet);
        }
        
        long now = System.currentTimeMillis();
        for (int stripeId: stripeSet) {
            ArrayList<Long> arr = new ArrayList<Long>();
            arr.add(Long.valueOf(nodeId));
            arr.add(now);
            this.stripeSubedNodeAndTimeMap.put(stripeId, arr);
        }
        this.msgLock.unlock();
    }

    public void removeSubscribeMsg(int nodeId, ArrayList<Integer> subedStripes) {
        if (subedStripes == null )
            return;
        this.msgLock.lock();
        if (subedStripes.isEmpty() && this.subscribeMsgSentMap.containsKey(nodeId)) 
            subedStripes.addAll(this.subscribeMsgSentMap.get(nodeId));
        
        if (this.subscribeMsgSentMap.containsKey(nodeId)) {
            this.subscribeMsgSentMap.get(nodeId).removeAll(subedStripes);
            if (this.subscribeMsgSentMap.get(nodeId).isEmpty())
                this.subscribeMsgSentMap.remove(nodeId);
            for(int stripeId: subedStripes) {
                this.stripeSubedNodeAndTimeMap.remove(stripeId);
            }
        }
        this.msgLock.unlock();
    }

    public HashSet<Integer> getSubscribeMsgSentTo(int nodeId) {
        HashSet<Integer> res = new HashSet<Integer>();
        this.msgLock.lock();
        if (this.subscribeMsgSentMap.containsKey(nodeId)) {
            res.addAll(this.subscribeMsgSentMap.get(nodeId));
        }
        this.msgLock.unlock();
        return res;
    }

    public void addStripeSubscribeSource(int stripeId, ArrayList<Integer> nodeList) {
        this.downloaderLock.lock();
        if (this.stripeAvailableNode.containsKey(stripeId) == false)
            this.stripeAvailableNode.put(stripeId, new HashSet<Integer>());
        this.stripeAvailableNode.get(stripeId).addAll(nodeList);
        this.downloaderLock.unlock();
    }

    public void removeStripeSubscribeSource(int nodeId, HashSet<Integer> stripeSet) {
        this.downloaderLock.lock();
        for(int stripeId: stripeSet ) {
            if (this.stripeAvailableNode.containsKey(stripeId))
                this.stripeAvailableNode.remove(nodeId);
        }
        this.downloaderLock.unlock();
    }

    public ArrayList<Integer> getStripeSubscribeSource(int stripeId) {
        this.downloaderLock.lock();
        HashSet<Integer> hs = new HashSet<>();
        if (this.stripeAvailableNode.containsKey(stripeId))
            hs.addAll(this.stripeAvailableNode.get(stripeId));
        else
            hs.add(stripeId);
        this.downloaderLock.unlock();
        return new ArrayList<Integer>(hs);
    }


    public boolean isSendSubscribeMsgBefore(int stripeId) {
        this.msgLock.lock();
        boolean res = false;
        if ( this.stripeSubedNodeAndTimeMap.containsKey(stripeId)){
            long sendTime = this.stripeSubedNodeAndTimeMap.get(stripeId).get(1);
            long now = System.currentTimeMillis();
            res = now - sendTime <= 5000;
        }
        this.msgLock.unlock();
        return res;
    }

    /**
     * /I send a subscribe msg right now 
     * or I already have stripe sender
     * In the two cases above, I should not subscribe the stripe
     * @param stripeId
     * @return
     */
    public boolean shouldISubscribe(int stripeId) {
        int sender = getStripeSender(stripeId);
        boolean send = isSendSubscribeMsgBefore(stripeId);
        return (sender == noSender) && (send == false);
    }

}