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

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

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

    @Test
    public void testSmallAbortedEmptyTransaction() throws Exception {
        doTest(1, SMALL_RECORD_COUNT);
    }

    @Test
    public void testSmallAbortedInsertTransaction() throws Exception {
        doTest(SMALL_RECORD_COUNT / 2, SMALL_RECORD_COUNT);
    }

    @Test
    public void testSmallAbortedStoreDeleteTransaction() throws Exception {
        doTest(SMALL_RECORD_COUNT, SMALL_RECORD_COUNT);
    }

    @Test
    public void testLargeAbortedEmptyTransaction() throws Exception {
        doTest(1, LARGE_RECORD_COUNT);
    }

    @Test
    public void testLargeAbortedInsertTransaction() throws Exception {
        doTest(LARGE_RECORD_COUNT / 2, LARGE_RECORD_COUNT);
    }

    @Test
    public void testLargeAbortedStoreDeleteTransaction() throws Exception {
        doTest(LARGE_RECORD_COUNT, LARGE_RECORD_COUNT);
    }

    private void doTest(final int when, final int records) throws Exception {
        {
            final Exchange ex = _persistit.getExchange("persistit", "Bug918909Test", true);
            final Transaction txn = ex.getTransaction();
            txn.begin();
            for (int i = 1; i <= records; i++) {
                ex.getValue().put(RED_FOX);
                if (i == when) {
                    _persistit.flush();
                    _persistit.checkpoint();
                }
                ex.clear().append(i).store();
            }
            if (when == records) {
                ex.removeAll();
            }
            txn.rollback();
            txn.end();
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
                ex.clear().append(1).fetch();
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
