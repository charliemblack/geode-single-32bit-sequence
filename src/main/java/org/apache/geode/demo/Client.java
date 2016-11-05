/*
 * Copyright 2016 Charlie Black
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.demo;

import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.demo.functions.OrdinalSequenceUnsigned32bit;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Charlie Black on 11/1/16.
 */
public class Client {

    private static int index = 0;

    private static String findArg(String findArg, String[] args) {

        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equalsIgnoreCase(findArg)) {
                return args[i + 1];
            }
        }
        return null;
    }

    public static void main(String[] args) throws InterruptedException {

        String found = findArg("--threads", args);
        Integer numberOfThreads = found != null ? Integer.parseInt(found) : null;
        found = findArg("--block-size", args);
        Integer blockSizeInteger = found != null ? Integer.parseInt(found) : null;
        found = findArg("--iterations", args);
        final Integer iterations = found != null ? Integer.parseInt(found) : null;

        if (blockSizeInteger != null && numberOfThreads != null && iterations != null) {
            final int blockSize = blockSizeInteger;
            long[] test = new long[numberOfThreads * blockSize * iterations];
            final Object lock = new Object();
            ClientCacheFactory factory = new ClientCacheFactory();
            factory.set("log-level", "error")
                    .addPoolLocator("localhost", 10334)
                    .create();

            CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);

            Unsigned32bitSequence unsigned32bitSequence = new Unsigned32bitSequence("mySequence");

            Runnable runnable = () -> {
                for (int i = iterations; i > 0; i--) {
                    try {
                        long[] result = unsigned32bitSequence.getNextBlock(blockSize);
                        for (long curr : result) {
                            synchronized (lock) {
                                test[index++] = curr;
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Exception retry " + e.getMessage());
                    }
                }
                countDownLatch.countDown();
            };
            for (int i = numberOfThreads; i > 0; i--) {
                new Thread(runnable).start();
            }
            countDownLatch.await();

            Arrays.sort(test);

            System.out.println("Checking order of " + index + " items - right count " + (test.length == (numberOfThreads * blockSize * iterations)));
            for (int i = 1; i < test.length; i++) {
                if (test[i - 1] > test[i]) {
                    //overflow test.
                    if (!(test[i - 1] == OrdinalSequenceUnsigned32bit.MAX_UNSIGNED_INT && test[i] == 0)) {
                        System.out.println("Failure : " + test[i - 1] + " > " + test[i]);
                        System.exit(1);
                    }
                }
            }
            System.out.println("Everything OK");
        } else {
            System.out.println("Usage: --threads <threads> --block-size <size of blocks> --iterations <iterations per thread>");
        }
    }
}
