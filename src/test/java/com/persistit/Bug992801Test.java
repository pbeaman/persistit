/**
 * Copyright 2012 Akiban Technologies, Inc.
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
