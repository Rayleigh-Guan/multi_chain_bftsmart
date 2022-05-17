/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bftsmart.demo.microbenchmarks;

import java.io.IOException;
import java.util.Arrays;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Storage;
import bftsmart.demo.microbenchmarks.AsyncReplyListener;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 * @author anogueira
 *
 */
public class AsyncLatencyClient {

    static int initId;
    
    public static void main(String[] args) throws IOException {
        if (args.length < 7) {
            System.out.println("Usage: java ...AsyncLatencyClient <initial client id> <number of clients> <number of operations> <request size> <interval (ms)> <read only?> <verbose?>");
            System.exit(-1);
        }

        initId = Integer.parseInt(args[0]);
        int numThreads = Integer.parseInt(args[1]);
        int numberOfOps = Integer.parseInt(args[2]);
        int requestSize = Integer.parseInt(args[3]);
        int interval = Integer.parseInt(args[4]);
        boolean readOnly = Boolean.parseBoolean(args[5]);
        boolean verbose = Boolean.parseBoolean(args[6]);
        
        Client[] clients = new Client[numThreads];

        for (int i = 0; i < numThreads; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }

            System.out.println("Launching client " + (initId + i));
            clients[i] = new AsyncLatencyClient.Client(initId + i, numberOfOps, requestSize, interval, readOnly, verbose);
        }
        
        ExecutorService exec = Executors.newFixedThreadPool(clients.length);
        Collection<Future<?>> tasks = new LinkedList<>();
        
        for (Client c : clients) {
            tasks.add(exec.submit(c));
        }
        
        // wait for tasks completion
        for (Future<?> currTask : tasks) {
            try {
                currTask.get();
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }
    
        exec.shutdown();
        
        System.out.println("All clients done.");

    }

    static class Client extends Thread {

        int id;
        AsynchServiceProxy serviceProxy;
        int numberOfOps;
        int interval;
        byte[] request;
        TOMMessageType reqType;
        boolean verbose;

        public Client(int id, int numberOfOps, int requestSize, int interval, boolean readOnly, boolean verbose) {

            this.id = id;
            this.serviceProxy = new AsynchServiceProxy(id);

            this.numberOfOps = numberOfOps;
            this.interval = interval;
            this.request = new byte[requestSize];
            this.reqType = (readOnly ? TOMMessageType.UNORDERED_REQUEST : TOMMessageType.ORDERED_REQUEST);
            this.verbose = verbose;

        }

        public void run() {

            try {

                if (this.verbose) System.out.println("Executing experiment for " + this.numberOfOps + " ops");
                AsyncReplyListener listener = new AsyncReplyListener(id, this.serviceProxy, this.verbose);
                long startTime = System.nanoTime();
                for (int i = 0; i < this.numberOfOps; i++) {
                    
                    long last_send_instant = System.nanoTime();
                    
                    listener.storeRequest(i);
                    this.serviceProxy.invokeAsynchRequest(this.request, listener, this.reqType);

                    long send_finish = System.nanoTime();
                    if (i%250 == 0)
                        System.out.printf("%d send request %d use %d us\n", this.id, i, (send_finish-last_send_instant));

                    if (this.interval > 0) {
                        Thread.sleep(this.interval);
                    }
                    
                    if (this.verbose) System.out.println("Sending " + (i + 1) + "th op");
                }
                long endTime = System.nanoTime();
                while (!listener.receiveAllReply(numberOfOps)) {
                    // System.out.println(this.id + " waitting reply...");
                    Thread.sleep(interval);//wait 100ms to receive the last replies
                    
                }
                System.out.printf("%d generate %d requests in %d ms\n", this.id, numberOfOps, (endTime-startTime)/1000);
                listener.printStaticsInfo();

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                this.serviceProxy.close();
            }

        }

    }
}
