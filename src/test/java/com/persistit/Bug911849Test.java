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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.Accumulator.SumAccumulator;
import com.persistit.exception.RollbackException;

/**
 * TableStatusRecoveryIT and RenameTableIT fail intermittently
 * 
 * These two tests have been failing intermittently since the new MVCC code came
 * in. I'm going to mark them as @Ignore to unblock other merges, but we have to
 * fix them before we can release MVCC into the wild.
 * 
 * Since the failure is intermittent, I recommend several runs of both of these
 * tests, on various machines including the AMIs, before we mark this as
 * fix-released. We should also un-ignore those two tests.
 * 
 * The intermittent failure in RenameTableIT may or may not be related to bug
 * 852142. We should treat the two bugs as separate for now, since that other
 * bug was observed only once before (and this one is now fairly frequent).
 * 
 * 
 * =====
 * 
 * This test looks for possible mechanism. Persistit#initialize(Properties)
 * starts the CheckpointManager's polling thread before applying transactions.
 * The setting of the initial timestamp for new transactions is updated _while_
 * transactions are being applied. This provides an opportunity for the
 * checkpoint transaction to have the same start timestamp or commit timestamp
 * as one of the transactions being recovered.
 * 
 * This can lead to two effects, both of which have been observed:
 * 
 * - recovery can issue an error message saying that a transaction already
 * exists having the same start timestamp
 * 
 * - A previously committed transactions can fail to be recovered.
 * 
 */

public class Bug911849Test extends PersistitUnitTestCase {
    private final static int ROW_COUNT_ACCUMULATOR_INDEX = 0;
    private final Random random = new Random();
    private final AtomicInteger counter = new AtomicInteger();

    @Test
    public void testOverlappingCheckpointTransactions() throws Exception {
        for (int i = 0; i < 10; i++) {
            accumulateRows(10000);
            _persistit.getJournalManager().force();
            _persistit.crash();
            _persistit = new Persistit(_config);
            final Exchange exchange = _persistit.getExchange("persistit", "AccumulatorRecoveryTest", false);
            final Accumulator rowCount = exchange.getTree().getAccumulator(Accumulator.Type.SUM,
                    ROW_COUNT_ACCUMULATOR_INDEX);
            assertEquals(counter.get(), rowCount.getLiveValue());
        }
    }

    private void accumulateRows(final int max) throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "AccumulatorRecoveryTest", true);
        final Transaction txn = _persistit.getTransaction();
        int count = 0;
        while (count++ < max) {
            int retryCount = 0;
            txn.begin();
            try {
                final SumAccumulator rowCount = exchange.getTree().getSumAccumulator(ROW_COUNT_ACCUMULATOR_INDEX);
                rowCount.add(1);
                txn.commit();
                counter.incrementAndGet();
                if ((count % 10) == 0) {
                    Thread.sleep(1);
                }
            } catch (final RollbackException re) {
                retryCount++;
                assertTrue(retryCount < 5);
                System.out.println("(Acceptable) rollback in " + Thread.currentThread().getName());
            } finally {
                txn.end();
            }
        }
    }

}
