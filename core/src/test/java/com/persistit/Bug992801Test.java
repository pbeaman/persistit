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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class Bug992801Test extends PersistitUnitTestCase {
    /*
     * https://bugs.launchpad.net/akiban-persistit/+bug/992801
     * 
     * A transaction creates an table index and commits. Subsequently many
     * threads read the index. Because there are no other update transactions
     * the TransactionStatus for the index creation remains present in the
     * TransactionIndex, and therefore the concurrent reader threads are slowed
     * by contention on the bucket lock in getStatus.
     * 
     * Fixing this could improve performance in some scenarios. For example,
     * customer creates a new index and then starts a bunch of concurrent query
     * threads. Note that ongoing replication clears the condition relatively
     * quickly, so this is a condition that requires little or no update
     * activity subsequent to index creation. The bug is exploited particularly
     * on indexes because query threads traverse the index and repeatedly see
     * MVVs with the same transaction's start timestamp.
     */

    @Test
    public void testFloorRises() throws Exception {

        final Transaction txn = _persistit.getTransaction();
        final Exchange ex = _persistit.getExchange("persistit", "Bug992801Test", true);

        txn.begin();
        final long tsv = txn.getStartTimestamp();
        for (int i = 1; i < 1000; i++) {
            ex.getValue().put(RED_FOX + i);
            ex.to(i).store();
        }
        txn.commit();
        txn.end();

        long whenStatusCleared = Long.MAX_VALUE;

        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + 5000) {
            final TransactionStatus status = _persistit.getTransactionIndex().getStatus(tsv);
            if (status == null) {
                whenStatusCleared = System.currentTimeMillis();
                break;
            }
            Thread.sleep(10);
        }
        assertTrue(String.format("Should have cleared within 1000ms, actual=%,d", whenStatusCleared - start),
                whenStatusCleared - start < 1000);
    }
}
