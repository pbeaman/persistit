/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Transaction;

/**
 * At a DP site experiencing very low insert rates the JournalManager was
 * failing to delete obsolete journal files. This was noticed just prior to the
 * disk becoming full.
 * 
 * Current journal file is xx36, the obsolete journal file not being removed is
 * xx27 Many of the journal files are short, indicating repeated cycles of
 * shutdown/restart.
 * 
 * The root cause is that a transaction which began in xx27 aborted, but its
 * transaction status was (correctly) written to the journal's TM (transaction
 * map) record after the next recovery. What is incorrect is that after the
 * subsequent recovery, the same transaction was also copied into _its_
 * liveTransactionMap where it was subsequently carried through many additional
 * start/stop cycles.
 * 
 * Two issues to fix:
 * 
 * (1) an aborted transaction in the LiveTransactionMap should not hold back the
 * journal cleanup process.
 * 
 * (2) the aborted transaction should not have been retained in the
 * liveTransactionMap after the second startup.
 */

public class Bug918909Test extends PersistitUnitTestCase {

    private final int RESTART_ITERATIONS = 5;
    private final int SMALL_RECORD_COUNT = 10;
    private final int LARGE_RECORD_COUNT = 1000;
    private final int TRANSACTION_COUNT = 10;

    @Test
    public void testSmallAbortedEmptyTransaction() throws Exception {
        doTest(1, SMALL_RECORD_COUNT, 1);
    }

    @Test
    public void testMultipleSmallAbortedEmptyTransactions() throws Exception {
        doTest(1, SMALL_RECORD_COUNT, TRANSACTION_COUNT);
    }

    @Test
    public void testSmallAbortedInsertTransaction() throws Exception {
        doTest(SMALL_RECORD_COUNT / 2, SMALL_RECORD_COUNT, 1);
    }

    @Test
    public void testSmallAbortedStoreDeleteTransaction() throws Exception {
        doTest(SMALL_RECORD_COUNT, SMALL_RECORD_COUNT, 1);
    }

    @Test
    public void testLargeAbortedEmptyTransaction() throws Exception {
        doTest(1, LARGE_RECORD_COUNT, 1);
    }

    @Test
    public void testLargeAbortedInsertTransaction() throws Exception {
        doTest(LARGE_RECORD_COUNT / 2, LARGE_RECORD_COUNT, 1);
    }

    @Test
    public void testLargeAbortedStoreDeleteTransaction() throws Exception {
        doTest(LARGE_RECORD_COUNT, LARGE_RECORD_COUNT, 1);
    }

    @Test
    public void testMultipleLargeAbortedStoreDeleteTransactions() throws Exception {
        doTest(LARGE_RECORD_COUNT, LARGE_RECORD_COUNT, TRANSACTION_COUNT);
    }

    private void doTest(final int when, final int records, final int transactions) throws Exception {
        for (int i = 1; i < transactions; i++) {
            {
                final Exchange ex = _persistit.getExchange("persistit", "Bug918909Test", true);
                final Transaction txn = ex.getTransaction();
                txn.begin();
                for (int j = 1; j <= records; j++) {
                    ex.getValue().put(RED_FOX);
                    if (j == when) {
                        _persistit.flush();
                        _persistit.checkpoint();
                    }
                    ex.clear().append(j).store();
                }
                if (when == records) {
                    ex.removeAll();
                }
                txn.rollback();
                txn.end();
            }
        }
        final Properties properties = _persistit.getProperties();
        _persistit.crash();

        for (int i = 0; i < RESTART_ITERATIONS; i++) {
            _persistit = new Persistit();
            _persistit.initialize(properties);
            final Exchange ex = _persistit.getExchange("persistit", "Bug918909Test", true);
            final Transaction txn = ex.getTransaction();
            txn.begin();
            for (int j = 1; j <= records; j++) {
                ex.clear().append(j).fetch();
                assertFalse(ex.getValue().isDefined());
                assertFalse(ex.isValueDefined());
            }
            assertFalse(ex.to(Key.BEFORE).next());
            assertFalse(ex.to(Key.AFTER).previous());
            txn.commit();
            txn.end();
            _persistit.close();
        }

        _persistit = new Persistit();
        _persistit.initialize(properties);
        _persistit.checkpoint();
        _persistit.copyBackPages();
        assertTrue(_persistit.getJournalManager().getBaseAddress() > RESTART_ITERATIONS
                * _persistit.getJournalManager().getBlockSize());
    }
}
