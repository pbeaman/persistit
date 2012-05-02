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

import com.persistit.Exchange;
import com.persistit.TestShim;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.util.Util;

/**
 * This is a subtle but serious database corruption issue that could lead to
 * data loss.
 * 
 * The Buffer#pruneMvvValues method in some cases must write the page being
 * pruned before modifying it. This happens when the page is already dirty, a
 * checkpoint was allocated, and the pruning operation is taking place after the
 * checkpoint. The bug occurred occasionally in AccumulatorRecoveryTest after
 * background pruning was added, but the mechanism could cause this failure in
 * other circumstances.
 * 
 * The issue is that before writing the page Persistit reorganizes it to squeeze
 * out unused space using the Buffer#clearSlack() method. The problem is that
 * pruneMvvValues calls this method while traversing keys and does not reset its
 * internal state after the buffer has been reorganized. The result is that
 * internal pointers subsequently point to invalid data and can cause serious
 * corruption.
 * 
 * Plan for a unit test:
 * 
 * 1. Create a tree with MVV values using a transaction that aborts after
 * creating them.
 * 
 * 2. Allocate a new checkpoint.
 * 
 * 3. Prune pages of that tree.
 * 
 * Pruning should result in removing the aborted MVVs which will create gaps in
 * the allocated space within the page. The bug mechanism should then corrupt
 * the page and lead to one of several different kinds of exceptions.
 */

public class Bug923790Test extends PersistitUnitTestCase {

    @Test
    public void testInduceBug923790() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "Bug923790Test", true);

        ex.getValue().put("abcdef");
        ex.to(-1).store();
        for (int i = 200; --i >= 100;) {
            ex.getValue().put(RED_FOX.substring(0, i % RED_FOX.length()));
            ex.to(i).store();
        }

        Transaction txn = _persistit.getTransaction();
        txn.begin();
        for (int i = 100; --i >= 0;) {
            ex.getValue().put(RED_FOX.substring(0, i % RED_FOX.length()));
            ex.to(i).store();
        }
        new Thread(new Runnable() {
            public void run() {
                Transaction txn = _persistit.getTransaction();
                try {
                    txn.begin();
                    Util.sleep(1000);
                    txn.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    txn.end();
                }
            }
        });
        txn.commit();
        txn.end();
        ex.to(-1).remove();

        Util.sleep(2000);
        TestShim.allocateCheckpointTimestamp(_persistit);
        ex.to(1);
        TestShim.prune(ex);
        txn.begin();
        for (int i = 0; i < 200; i++) {
            ex.to(i).fetch();
            assertEquals(RED_FOX.substring(0, i % RED_FOX.length()), ex.getValue().get());
        }
        txn.commit();
        txn.end();
    }
}
