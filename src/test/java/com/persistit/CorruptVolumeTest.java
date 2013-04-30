/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

package com.persistit;

import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.PersistitException;

public class CorruptVolumeTest extends PersistitUnitTestCase {

    private final String _volumeName = "persistit";

    @Test
    public void testCorruptVolume() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "CorruptVolumeTest", true);
        // store some records
        exchange.getValue().put("CorruptVolumeTest");
        for (int i = 0; i < 10000; i++) {
            exchange.to(i).store();
        }
        // Corrupt the volume by zonking the the index page
        final long pageAddr = exchange.fetchBufferCopy(1).getPageAddress();
        final Buffer buffer = exchange.getBufferPool().get(exchange.getVolume(), pageAddr, true, true);
        Arrays.fill(buffer.getBytes(), 20, 200, (byte) 0);
        buffer.setDirtyAtTimestamp(_persistit.getTimestampAllocator().updateTimestamp());
        buffer.releaseTouched();
        //
        try {
            exchange.to(9000).fetch();
            fail("Should have gotten a CorruptVolumeException");
        } catch (final CorruptVolumeException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

}
