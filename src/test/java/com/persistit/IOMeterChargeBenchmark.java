/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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
        long start = System.nanoTime();
        for (int count = 0; count < 10000000; count++) {
            ioMeter.chargeWriteOtherToJournal(123, 445678);
        }
        long elapsed = System.nanoTime() - start;
        assertTrue(elapsed < 2 * SECOND);
    }

    @Test
    public void testIOMeterPoll() throws Exception {
        IOMeter ioMeter = new IOMeter();
        for (int counter = 0; counter < 200; counter++) {
            ioMeter.chargeWriteOtherToJournal(1000, counter);
            if (counter % 40 == 0) {
                ioMeter.poll();
            }
            long charge = ioMeter.recentCharge();
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
