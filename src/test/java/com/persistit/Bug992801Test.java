/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.persistit.exception.PersistitException;

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
