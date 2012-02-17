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

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

import com.persistit.*;

/**
 * Failure detected during TPCC testing. Upon restarting server, the following
 * error was emitted:
 * 
 * [main] ERROR Recovery failed due to
 * com.persistit.exception.CorruptJournalException: Missing journal file
 * /home/akiban
 * /dev/persistit/fix-pruning-deadlock2/bench/run/../data/BenchmarkSQL_journal
 * .000000000011 - the journal needs to be repaired or discarded
 * 
 * This is a false positive. The data is not actually corrupt. RecoveryManager
 * thinks it needs journal file 11 because the transaction map at the front of
 * file 12 lists transactions that started in file 11. However, all of these
 * aborted and were subsequently pruned, and the lastValidCheckpoint in file 12
 * has a base address in file 12.
 * 
 * Upon further examination, the circumstances are a little different than my
 * initial view. Jounrnal file 12 had several checkpoint records. Each
 * checkpoint contains the base address current at the time that checkpoint was
 * written. The base value actually _decreased_ between the first and second CP
 * records and then increased again. This indicates an unknown failure mechanism
 * in the JournalManager.
 * 
 * Note that this failure occurred while the wwLock bug 923761 was occurring.
 * One side-effect of that bug would be for a thread to have a start timestamp
 * and then be delayed by up to a minute before being aborted.
 */

public class Bug927701Test extends PersistitUnitTestCase {

    @Test
    public void testBug927701() throws Exception {
        final JournalManager jman = _persistit.getJournalManager();
        TestShim.setMinimumPruningDelay(_persistit, 0);
        jman.setCopierInterval(1000);
        final long blockSize = jman.getBlockSize();
        /*
         * 1. Add at least 4MB + of stuff to the journal
         */
        {
            final Transaction txn = _persistit.getTransaction();
            txn.begin();
            final Exchange exchange = _persistit.getExchange("persistit", "Bug927701Test", true);
            exchange.getValue().put(RED_FOX);
            int index = 0;
            while ((jman.getCurrentAddress() % blockSize) < JournalManager.ROLLOVER_THRESHOLD) {
                exchange.to(index++).store();
            }
            txn.commit();
            txn.end();
        }
        /*
         * 3. Write part of a transaction, then abort.
         */
        long journalAddress = jman.getCurrentAddress();
        final Transaction abortingTxn = _persistit.getTransaction();
        abortingTxn.begin();
        final Exchange exchange = _persistit.getExchange("persistit", "Bug927701Test", false);
        // write enough stuff to overflow and flush the transaction buffer
        exchange.getValue().put(RED_FOX.toUpperCase());
        final int count = 65536 / (RED_FOX.length());
        for (int index = 0; index < count; index++) {
            exchange.to(index).store();
        }
        /*
         * 3. Abort transaction
         */
        abortingTxn.rollback();
        // abortingTxn.end();
//        TestShim.flushTransactionBuffer(abortingTxn);
        /*
         * Wait for JournalManager call to pruneObsoleteTransactions
         */
        Thread.sleep(3000);

        /*
         * 2. Checkpoint - this will flush the transaction buffer
         */
        {
            _persistit.copyBackPages();
            _persistit.checkpoint();
        }
        /*
         * 4. Wait for a rollover triggered by journal copier
         */
        for (int wait = 5; --wait >= 0;) {
            Thread.sleep(1000);
            if (jman.getCurrentAddress() / blockSize != journalAddress / blockSize) {
                break;
            }
            assertTrue(wait > 0);
        }

        abortingTxn.end();

        final long baseAddress1 = jman.getBaseAddress();
        /*
         * 5. Force checkpoint; should now flush the aborted transaction
         * buffers.
         */
        {
            _persistit.checkpoint();
        }

        final long baseAddress2 = jman.getBaseAddress();
        assertTrue(baseAddress2 >= baseAddress1);

    }
}
