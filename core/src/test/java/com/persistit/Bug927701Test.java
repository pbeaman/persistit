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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.JournalManager;
import com.persistit.TestShim;
import com.persistit.Transaction;
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
        // TestShim.flushTransactionBuffer(abortingTxn);
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
