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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class IOMeterChargeBenchmark extends PersistitUnitTestCase {

    private final static long SECOND = 1000000000;

    /**
     * Crude validation that the chargeXX methods no longer cost too much. This
     * test used to take > 20 seconds.
     * 
     * @throws Exception
     */
    @Test
    public void testBenchmarkIOMeterCharge() throws Exception {
        final IOMeter ioMeter = new IOMeter();
        final long start = System.nanoTime();
        for (int count = 0; count < 10000000; count++) {
            ioMeter.chargeWriteOtherToJournal(123, 445678);
        }
        final long elapsed = System.nanoTime() - start;
        assertTrue(elapsed < 2 * SECOND);
    }

    @Test
    public void testIOMeterPoll() throws Exception {
        final IOMeter ioMeter = new IOMeter();
        for (int counter = 0; counter < 200; counter++) {
            ioMeter.chargeWriteOtherToJournal(1000, counter);
            if (counter % 40 == 0) {
                ioMeter.poll();
            }
            final long charge = ioMeter.recentCharge();
            if (counter > 10) {
                assertTrue("charge=" + charge, charge > 18000 && charge < 22000);
            }
            Thread.sleep(50);
        }
    }

    @Override
    public void runAllTests() throws Exception {

    }

}
