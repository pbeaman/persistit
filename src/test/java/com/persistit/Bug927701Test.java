/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

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
 * initial view. Journal file 12 had several checkpoint records. Each checkpoint
 * contains the base address current at the time that checkpoint was written.
 * The base value actually _decreased_ between the first and second CP records
 * and then increased again. This indicates an unknown failure mechanism in the
 * JournalManager.
 * 
 * Note that this failure occurred while the wwLock bug 923761 was occurring.
 * One side-effect of that bug would be for a thread to have a start timestamp
 * and then be delayed by up to a minute before being aborted.
 */

public class Bug927701Test extends PersistitUnitTestCase {

    @Test
    public void testBug927701() throws Exception {
        final JournalManager jman = _persistit.getJournalManager();
        _persistit.getCleanupManager().setMinimumPruningDelay(0);
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
         * 2. Write part of a transaction, then abort.
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
        abortingTxn.rollback();
        /*
         * Checkpoint to advance base address
         */
        _persistit.checkpoint();
        /*
         * Wait for CleanupManager call to pruneObsoleteTransactions
         */
        Thread.sleep(2000);
        /*
         * Copy-back to discharge any remaining pages in the journal. Note that
         * this method also calls checkpoint beforehand.
         */
        _persistit.copyBackPages();
        /*
         * Another checkpoint to move the base address again, this time to the
         * very end of the journal.
         */
        _persistit.checkpoint();
        /*
         * Wait for a rollover triggered by journal copier
         */
        for (int wait = 5; --wait >= 0;) {
            if (jman.getCurrentAddress() / blockSize != journalAddress / blockSize) {
                break;
            }
            assertTrue(wait > 0);
            System.out.printf("Cur=%,d base=%,d lvc=%,d\n", jman.getCurrentAddress(), jman.getBaseAddress(), jman
                    .getLastValidCheckpointBaseAddress());
            Thread.sleep(1000);
        }

        abortingTxn.end();

        final long baseAddress1 = jman.getBaseAddress();
        /*
         * Force checkpoint; should now flush the aborted transaction buffers.
         */
        _persistit.checkpoint();

        final long baseAddress2 = jman.getBaseAddress();
        assertTrue(baseAddress2 >= baseAddress1);

    }
}
