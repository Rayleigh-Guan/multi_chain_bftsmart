/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bftsmart.demo.testErasureCode;

import bftsmart.erasureCode.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestErasureCodec {

  
    /**
     * Zero size encode.
     */
    
    public void testZeroSizeEncode() {
        ReedSolomon codec = ReedSolomon.create(2, 1);
        byte [] [] shards = new byte [3] [0];
        codec.encodeParity(shards, 0, 0);
    }

    /**
     * Make sure that the results on a tiny encoding match what
     * the prototype Python code did, and that all of the different
     * coding loops produce the same answers.
     */
    
    public void testOneEncode() {
        for (CodingLoop codingLoop : CodingLoop.ALL_CODING_LOOPS) {
            ReedSolomon codec = new ReedSolomon(5, 5, codingLoop);
            byte[][] shards = new byte[10][];
            shards[0] = new byte[]{0, 1};
            shards[1] = new byte[]{4, 5};
            shards[2] = new byte[]{2, 3};
            shards[3] = new byte[]{6, 7};
            shards[4] = new byte[]{8, 9};
            shards[5] = new byte[2];
            shards[6] = new byte[2];
            shards[7] = new byte[2];
            shards[8] = new byte[2];
            shards[9] = new byte[2];
            codec.encodeParity(shards, 0, 2);
            assertArrayEquals(new byte[]{12, 13}, shards[5]);
            assertArrayEquals(new byte[]{10, 11}, shards[6]);
            assertArrayEquals(new byte[]{14, 15}, shards[7]);
            assertArrayEquals(new byte[]{90, 91}, shards[8]);
            assertArrayEquals(new byte[]{94, 95}, shards[9]);

            assertTrue(codec.isParityCorrect(shards, 0, 2));
            shards[8][0] += 1;
            assertFalse(codec.isParityCorrect(shards, 0, 2));
        }
    }

    /**
     * Try a simple case of encoding and decoding.
     */
    
    public void testSimpleEncodeDecode() {
        byte [] [] dataShards = new byte [] [] {
                new byte [] { 0, 1 },
                new byte [] { 1, 2 },
                new byte [] { 1, 3 },
                new byte [] { 2, 4 },
                new byte [] { 3, 5 }
        };
        runEncodeDecode(5, 5, dataShards);
    }

    /**
     * Try encoding and decoding with a lot of shards.
     */
    
    public void testBigEncodeDecode(int dataCount, int parityCount, int shardSize) {
        final Random random = new Random(0);
        // final int dataCount = 64;
        // final int parityCount = 64;
        // final int shardSize = 200;
        byte [] [] dataShards = new byte [dataCount] [shardSize];
        for (byte [] shard : dataShards) {
            for (int i = 0; i < shard.length; i++) {
                shard[i] = (byte) random.nextInt(256);
            }
        }
        runEncodeDecode(dataCount, parityCount, dataShards);
    }

    /**
     * Encodes a set of data shards, and then tries decoding
     * using all possible subsets of the encoded shards.
     *
     * Uses 5+5 coding, so there must be 5 input data shards.
     */
    private void runEncodeDecode(int dataCount, int parityCount, byte[][] dataShards) {

        final int totalCount = dataCount + parityCount;
        final int shardLength = dataShards[0].length;
        final double mark = 1000000000/(1024*1024);
        // Make the list of data and parity shards.
        assertEquals(dataCount, dataShards.length);
        final int dataLength = dataShards[0].length;
        byte [] [] allShards = new byte [totalCount] [];
        for (int i = 0; i < dataCount; i++) {
            allShards[i] = Arrays.copyOf(dataShards[i], dataLength);
        }
        for (int i = dataCount; i < totalCount; i++) {
            allShards[i] = new byte [dataLength];
        }

        // Encode.
        final long encodeStart = System.nanoTime();
        ReedSolomon codec = ReedSolomon.create(dataCount, parityCount);
        codec.encodeParity(allShards, 0, dataLength);
        final long encodeTime = System.nanoTime() - encodeStart;
        final double encodeSpeed = (mark* shardLength * parityCount)/ encodeTime;
        System.out.printf("Encode k = %d, m = %d, packetSize = %d, using %d ns(10-9 sec, speed = %f MB/s)\n", dataCount, parityCount, shardLength, encodeTime, encodeSpeed);
        // Make a copy to decode with.
        byte [] [] testShards = new byte [totalCount] [];
        boolean [] shardPresent = new boolean [totalCount];
        for (int i = 0; i < totalCount; i++) {
            testShards[i] = Arrays.copyOf(allShards[i], shardLength);
            shardPresent[i] = true;
        }

        // Decode with different number of missing shards.
        for (int numberMissing = 1; numberMissing < parityCount + 1; numberMissing++) {
            final double decodeTime = tryAllSubsetsMissing(codec, allShards, testShards, shardPresent, numberMissing);
            final double decodeSpeed = (mark* shardLength * numberMissing)/ decodeTime;
            System.out.printf("Decode k = %d, m = %d, packetSize = %d, using %f ns(10-9 sec), speed = %f MB/s)\n", dataCount, parityCount, shardLength, decodeTime, decodeSpeed);
        }
    }

    private double tryAllSubsetsMissing(ReedSolomon codec,
                                      byte [] [] allShards, byte [] [] testShards,
                                      boolean [] shardPresent, int numberMissing) {
        final int shardLength = allShards[0].length;
        final int maxVal = codec.getDataShardCount() + codec.getDataShardCount()-1;
        List<int []> subsets = allSubsets(numberMissing, 0, maxVal);
        int testCnt = 0;
        double testTime = 0;
        for (int [] subset : subsets) {
            // Get rid of the shards specified by this subset.
            for (int missingShard : subset) {
                clearBytes(testShards[missingShard]);
                shardPresent[missingShard] = false;
            }
            ++testCnt;
            long decodeStart = System.nanoTime();
            // Reconstruct the missing shards
            codec.decodeMissing(testShards, shardPresent, 0, shardLength);
            long decodeFinish = System.nanoTime();
            testTime = decodeFinish - decodeStart;
            // Check the results.  After checking, the contents of testShards
            // is ready for the next test, the next time through the loop.
            checkShards(allShards, testShards);

            // Put the "present" flags back
            for (int i = 0; i < codec.getTotalShardCount(); i++) {
                shardPresent[i] = true;
            }
        }
        if (testCnt!= 0)
            return testTime / testCnt;
        return 0;
    }



    /**
     * Checks that all of the coding loops produce the same results.
     */
    
    public void testCodingLoopsProduceSameAnswers() {
        final int DATA_COUNT = 5;
        final int PARITY_COUNT = 5;
        final int SHARD_SIZE = 2000;
        final Random random = new Random(0);

        // Make a set of input data shards
        byte [] [] dataShards = new byte [DATA_COUNT] [SHARD_SIZE];
        for (byte[] shard : dataShards) {
            for (int iByte = 0; iByte < shard.length; iByte++) {
                shard[iByte] = (byte) random.nextInt(256);
            }
        }

        // Make a reference set of parity shards using an arbitrary coding
        // loop.
        byte [] [] expectedParityShards = computeParityShards(dataShards, ReedSolomon.create(DATA_COUNT, PARITY_COUNT));

        // Check that all coding loops produce the same set of parity shards.
        for (CodingLoop codingLoop : CodingLoop.ALL_CODING_LOOPS) {
            ReedSolomon codec = new ReedSolomon(DATA_COUNT, PARITY_COUNT, codingLoop);
            byte [] [] actualParityShards = computeParityShards(dataShards, codec);
            for (int i = 0; i < PARITY_COUNT; i++) {
                assertArrayEquals(expectedParityShards[i], actualParityShards[i]);
            }
        }
    }

    /**
     * Given an array of data shards, computes parity and returns an array
     * of the resulting parity shards.
     */
    private byte [] [] computeParityShards(byte [] [] dataShards, ReedSolomon codec) {
        final int shardSize = dataShards[0].length;
        final int totalShardCount = codec.getTotalShardCount();
        final int dataShardCount = codec.getDataShardCount();
        final int parityShardCount = codec.getParityShardCount();

        final byte [] [] parityShards = new byte [parityShardCount] [shardSize];

        final byte [] [] allShards = new byte [totalShardCount] [];
        for (int iShard = 0; iShard < totalShardCount; iShard++) {
            if (iShard < dataShardCount) {
                allShards[iShard] = dataShards[iShard];
            }
            else {
                allShards[iShard] = parityShards[iShard - dataShardCount];
            }
        }

        codec.encodeParity(allShards, 0, shardSize);

        byte [] tempBuffer = new byte [shardSize];
        allShards[parityShardCount - 1][0] += 1;
        assertFalse(codec.isParityCorrect(allShards, 0, shardSize));
        assertFalse(codec.isParityCorrect(allShards, 0, shardSize, tempBuffer));
        allShards[parityShardCount - 1][0] -= 1;
        assertTrue(codec.isParityCorrect(allShards, 0, shardSize));
        assertTrue(codec.isParityCorrect(allShards, 0, shardSize, tempBuffer));


        return parityShards;
    }

    private void clearBytes(byte [] data) {
        for (int i = 0; i < data.length; i++) {
            data[i] = 0;
        }
    }

    private void checkShards(byte[][] expectedShards, byte[][] actualShards) {
        assertEquals(expectedShards.length, actualShards.length);
        for (int i = 0; i < expectedShards.length; i++) {
            assertArrayEquals(expectedShards[i], actualShards[i]);
        }
    }

    /**
     * Returns a list of arrays with all possible sets of
     * unique values where (min <= n < max).
     *
     * This is NOT EFFICIENT, because it allocates lots of
     * temporary arrays, but it's OK for these tests.
     *
     * To avoid duplicates that are in a different order,
     * each subset is generated with elements in increasing
     * order.
     *
     * Given (n=2, min=1, max=4), returns:
     *    [1, 2]
     *    [1, 3]
     *    [1, 4]
     *    [2, 3]
     *    [2, 4]
     *    [3, 4]
     */
    private List<int []> allSubsets(int n, int min, int max) {
        List<int []> result = new ArrayList<int[]>();
        if (n == 0) {
            result.add(new int [0]);
        }
        else {
            for (int i = min; i < max - n; i++) {
                int [] prefix = { i };
                for (int [] suffix : allSubsets(n - 1, i + 1, max)) {
                    result.add(appendIntArrays(prefix, suffix));
                }
            }
        }
        return result;
    }

    private int [] appendIntArrays(int [] a, int [] b) {
        int [] result = new int[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

  public static void main(String[] args) {
    if (args.length < 3) {
        System.out.println(
                "Usage: ... TestErasureCodec <k> <m> <packetSize>");
        System.exit(-1);
    }
    
    int dataBlockNum = Integer.parseInt(args[0]);
    int codingBlockNum = Integer.parseInt(args[1]);
    int packLen = Integer.parseInt(args[2]);

    TestErasureCodec test = new TestErasureCodec();
    test.testBigEncodeDecode(dataBlockNum, codingBlockNum, packLen);
}
}