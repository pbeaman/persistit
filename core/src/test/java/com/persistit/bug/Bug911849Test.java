/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.bug;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.Accumulator;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.RollbackException;
import com.persistit.unit.PersistitUnitTestCase;

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
        final Properties props = _persistit.getProperties();
        for (int i = 0; i < 10; i++) {
            accumulateRows(10000);
            _persistit.getJournalManager().force();
            _persistit.crash();
            _persistit = new Persistit();
            _persistit.initialize(props);
            final Exchange exchange = _persistit.getExchange("persistit", "AccumulatorRecoveryTest", false);
            Accumulator rowCount = exchange.getTree().getAccumulator(Accumulator.Type.SUM,
                    ROW_COUNT_ACCUMULATOR_INDEX);
            assertEquals(counter.get(), rowCount.getLiveValue());
        }
    }

    private void accumulateRows(int max) throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "AccumulatorRecoveryTest", true);
        final Transaction txn = _persistit.getTransaction();
        int count = 0;
        Exception exception = null;
        while (count++ < max) {
            int retryCount = 0;
            txn.begin();
            try {
                Accumulator rowCount = exchange.getTree().getAccumulator(Accumulator.Type.SUM,
                        ROW_COUNT_ACCUMULATOR_INDEX);
                rowCount.update(1, txn);
                txn.commit();
                counter.incrementAndGet();
                if ((count % 10) == 0) {
                    Thread.sleep(1);
                }
            } catch (RollbackException re) {
                retryCount++;
                assertTrue(retryCount < 5);
                System.out.println("(Acceptable) rollback in " + Thread.currentThread().getName());
            } catch (Exception e) {
                exception = e;
                throw e;
            } finally {
                txn.end();
            }
        }
    }

}
