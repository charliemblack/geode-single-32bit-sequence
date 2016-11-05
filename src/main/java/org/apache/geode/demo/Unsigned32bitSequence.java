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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.demo.functions.OrdinalSequenceUnsigned32bit;

/**
 * Created by Charlie Black on 11/2/16.
 */
public class Unsigned32bitSequence {

    private static Region region;

    static {
        ClientCache cache = ClientCacheFactory.getAnyInstance();
        region = cache.getRegion(OrdinalSequenceUnsigned32bit.REGION_NAME);
        if (region == null) {
            ClientRegionFactory factory = ClientCacheFactory.getAnyInstance()
                    .createClientRegionFactory(ClientRegionShortcut.PROXY);
            region = factory.create(OrdinalSequenceUnsigned32bit.REGION_NAME);
        }
    }

    private String sequenceName;

    public Unsigned32bitSequence(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public long[] getNextBlock(int blockSize) {
        long[] result = null;
        if (blockSize > 0) {
            result = OrdinalSequenceUnsigned32bit.getNextBlock(region, sequenceName, blockSize);
        }
        return result;
    }
}
