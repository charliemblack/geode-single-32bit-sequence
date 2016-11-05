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

package org.apache.geode.demo.functions;


import org.apache.geode.LogWriter;
import org.apache.geode.cache.*;
import org.apache.geode.cache.execute.*;
import org.apache.geode.distributed.DistributedLockService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Charlie Black on 11/1/16.
 */
public class OrdinalSequenceUnsigned32bit implements Function, Declarable {

    public static final String ID = "OrdinalSequenceUnsigned32bit";
    public static final String DLOCK_PREFIX = "sequence_";
    public static final String REGION_NAME = "hidden" + ID;
    public static final long MAX_UNSIGNED_INT = (long) (Math.pow(2, 32) - 1);


    private ConcurrentHashMap<String, DistributedLockService> dLockMap = new ConcurrentHashMap<>();
    private Region<String, Long> backingRegion;
    private LogWriter logWriter;
    private final Object lock = new Object();


    public OrdinalSequenceUnsigned32bit() {
        logWriter = CacheFactory.getAnyInstance().getLogger();
        new Thread(() -> {
            init(new Properties());
        }).start();
    }

    @Override
    public void execute(FunctionContext context) {
        logWriter.config("OrdinalSequenceUnsigned32bit.execute");
        ArrayList<Long> results = new ArrayList<>();
        if (context instanceof RegionFunctionContext) {
            int blockSize = getBlockSize(context);
            String sequenceName = getSequenceName(context);
            DistributedLockService distributedLockService = getDistributedLockService(sequenceName);
            distributedLockService.lock(sequenceName, -1, -1);
            try {
                long value = backingRegion.getOrDefault(sequenceName, 0L);
                for (int i = 0; i < blockSize; i++) {
                    results.add(value++);
                    if (value >= MAX_UNSIGNED_INT) {
                        value = 0;
                    }
                }
                backingRegion.put(sequenceName, value);
            } finally {
                distributedLockService.unlock(sequenceName);
            }
        }
        context.getResultSender().lastResult(results);
    }

    // We are going to create a lock service per sequence name this way we can enable the lock grantor to move around
    // on failure.
    private DistributedLockService getDistributedLockService(String sequenceName) {
        sequenceName = DLOCK_PREFIX + sequenceName;
        DistributedLockService distributedLockService = DistributedLockService.getServiceNamed(sequenceName);
        if (distributedLockService == null) {
            synchronized (lock) {
                //Lets just make sure another thread didn't beat me to the d-lock creation
                distributedLockService = DistributedLockService.getServiceNamed(sequenceName);
                if (distributedLockService == null) {
                    Cache cache = CacheFactory.getAnyInstance();
                    distributedLockService = DistributedLockService.create(sequenceName, cache.getDistributedSystem());
                }
            }
        }
        if (!distributedLockService.isLockGrantor()) {
            distributedLockService.becomeLockGrantor();
        }
        return distributedLockService;
    }

    private int getBlockSize(FunctionContext context) {
        Object[] args = (Object[]) context.getArguments();
        int blockSize = 100;
        if (args.length > 0) {
            if (args[0] instanceof String) {
                blockSize = Integer.parseInt((String) args[0]);
            } else if (args[0] instanceof Integer) {
                blockSize = ((Integer) args[0]).intValue();
            }
        }
        return blockSize;
    }

    private String getSequenceName(FunctionContext context) {
        Object[] args = (Object[]) context.getArguments();
        String sequenceName = "default";
        if (args.length > 1 && args[1] instanceof String) {
            sequenceName = (String) args[1];
        }
        return sequenceName;
    }

    /**
     * <p>Return true to indicate to GemFire the method
     * requires optimization for writing the targeted {@link FunctionService#onRegion(Region)} and any
     * associated {@linkplain Execution#withFilter(Set) routing objects}.</p>
     * <p>
     * <p>Returning false will optimize for read behavior on the targeted
     * {@link FunctionService#onRegion(Region)} and any
     * associated {@linkplain Execution#withFilter(Set) routing objects}.</p>
     * <p>
     * <p>This method is only consulted when Region passed to
     * FunctionService#onRegion(org.apache.geode.cache.Region) is a partitioned region
     * </p>
     *
     * @return false if the function is read only, otherwise returns true
     * @see FunctionService
     * @since GemFire 6.0
     */
    @Override
    public boolean optimizeForWrite() {
        return true;
    }

    @Override
    public boolean hasResult() {
        return true;
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public void init(Properties props) {

        Cache cache = CacheFactory.getAnyInstance();
        cache.getLogger().config("in init()");
        cache.createDiskStoreFactory().create(props.getProperty("diskStoreName", ID));

        backingRegion = cache.getRegion(REGION_NAME);
        if (backingRegion == null) {
            RegionFactory<String, Long> regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
            regionFactory.setDiskStoreName(ID);
            PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
            partitionAttributesFactory.setRedundantCopies(1);
            partitionAttributesFactory.setRecoveryDelay(0);
            partitionAttributesFactory.setTotalNumBuckets(113);
            regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
            backingRegion = regionFactory.create(REGION_NAME);
        }
    }

    public static long[] getNextBlock(Region<Byte, Long> region, String sequenceName, int blockCount) {

        ArrayList<Long> results = new ArrayList<>();

        Object[] args = {blockCount, sequenceName};

        // Use the Geode onRegion to target the server that is dynamically responsible for the sequence number.
        ResultCollector resultCollector = FunctionService.onRegion(region)
                .withFilter(Collections.singleton(sequenceName))
                .withArgs(args)
                .execute(ID);

        Collection<Collection<Long>> resultBlocks = (Collection<Collection<Long>>) resultCollector.getResult();
        for (Collection curr : resultBlocks) {
            results.addAll(curr);
        }
        long[] returnValue = new long[results.size()];
        int i = 0;
        for (Long curr : results) {
            returnValue[i++] = curr.longValue();
        }
        return returnValue;
    }
}
