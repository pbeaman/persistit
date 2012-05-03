/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

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
