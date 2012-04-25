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

import java.util.WeakHashMap;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class AccumulatorMemoryTest extends PersistitUnitTestCase {

    @Test
    public void testReleaseMemory() throws Exception {
        WeakHashMap<Accumulator, Object> weakMap = new WeakHashMap<Accumulator, Object>();
        final Volume volume = _persistit.getVolume("persistit");
        for (int count = 0; count < 5000; count++) {
            Exchange ex = _persistit.getExchange(volume, String.format("tree%6d", count), true);
            Accumulator accumulator = ex.getTree().getAccumulator(Accumulator.Type.SUM, 0);
            weakMap.put(accumulator, null);
            ex.removeTree();
            if ((count % 1000) == 0) {
                System.gc();
            }
            assertTrue(weakMap.size() < 2000);
        }
    }

    @Test
    public void testReleaseMemoryTxn() throws Exception {
        WeakHashMap<Accumulator, Object> weakMap = new WeakHashMap<Accumulator, Object>();
        final Volume volume = _persistit.getVolume("persistit");
        final Transaction txn = _persistit.getTransaction();
        for (int count = 0; count < 5000; count++) {
            txn.begin();
            Exchange ex = _persistit.getExchange(volume, String.format("tree%6d", count), true);
            Accumulator accumulator = ex.getTree().getAccumulator(Accumulator.Type.SUM, 0);
            weakMap.put(accumulator, null);
            ex.removeTree();
            if ((count % 1000) == 0) {
                System.gc();
            }
            assertTrue(weakMap.size() < 2000);
            txn.commit();
            txn.end();
        }
    }

}
