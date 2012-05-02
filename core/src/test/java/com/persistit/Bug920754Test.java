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

package com.persistit.bug;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.Accumulator;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.TestShim;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

public class Bug920754Test extends PersistitUnitTestCase {
    /*
     * https://bugs.launchpad.net/akiban-persistit/+bug/920754
     * 
     * While working on my "ApiTestBase turbo" branch, I noticed the volume file
     * growing faster than I expected. I put a debug point in ApiTestBase's
     * 
     * @After method, when it should be compressing journals and such. When I
     * looked in the persistit UI, I saw that there were a few trees still
     * defined but with nothing in them; the AIS (which I looked into via the
     * DXL jmx bean) had only the primordial tables. But _directory contained
     * what looked like "a whole ton" of accumulators, for trees that no longer
     * existed.
     * 
     * To reproduce: - lp:~yshavit/akiban-server/ApiTestBase_turbo -r 1440 -
     * conditional breakpoint at ApiTestBase #274
     * ("System.out.println("flushing");"), to be triggered when size >= 400 *
     * 1024 * 1024. - look in persistit UI at the _directory tree. It should be
     * empty; it's very full.
     * 
     * From within the debugger, I invoked a checkpoint. Judging from write
     * time, the journal file did seem to have been written to; but the
     * accumulators didn't go away.
     */

    @Test
    public void testAccumumulatorTreeIsDeleted() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "Bug920754Test", true);
        final Transaction txn = _persistit.getTransaction();
        final Accumulator[] accumulators = new Accumulator[10];
        for (int i = 0; i < 10; i++) {
            accumulators[i] = exchange.getTree().getAccumulator(Accumulator.Type.SUM, 1);
        }
        txn.begin();
        for (int i = 0; i < 10; i++) {
            accumulators[i].update(1, txn);
        }
        txn.commit();
        txn.end();
        _persistit.checkpoint();
        final Exchange dir = TestShim.directoryExchange(exchange.getVolume());
        exchange.removeTree();
        int keys = 0;
        dir.to(Key.BEFORE);
        while (dir.next(true)) {
            keys++;
        }
        assertEquals("There should be no remaining keys in the directory tree", 0, keys);
    }
}
