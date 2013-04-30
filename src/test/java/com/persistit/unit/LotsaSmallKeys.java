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

package com.persistit.unit;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.PersistitUnitTestCase;
import com.persistit.exception.PersistitException;
import com.persistit.policy.SplitPolicy;

public class LotsaSmallKeys extends PersistitUnitTestCase {

    private final String _volumeName = "persistit";

    /**
     * Tests bug 775752. Logic to limit the key count was missing in the delete
     * case. This caused an AIOOBE in Buffer#recomputeFindex.
     * 
     * @throws PersistitException
     */
    @Test
    public void testInsertAndDeleteSmallRecords() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "LotsOfSmallKeys", true);
        exchange.removeAll();
        exchange.setSplitPolicy(SplitPolicy.LEFT_BIAS);
        exchange.getValue().clear();
        for (int i = 0; i < 20000; i++) {
            exchange.clear().append(i).store();
        }
        for (int j = 0; j < 100; j++) {
            for (int i = j; i < 20000; i += 100) {
                exchange.clear().append(i).remove();
            }
        }
    }

    @Override
    public Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    @Override
    public void runAllTests() throws Exception {
        testInsertAndDeleteSmallRecords();
    }

}
