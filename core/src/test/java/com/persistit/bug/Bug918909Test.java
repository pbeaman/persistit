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

    @Test
    public void testAbortedTransactionDoesntBlockCleanup() throws Exception {
        {
            final Exchange ex = _persistit.getExchange("persistit", "Bug918909Test", true);
            final Transaction txn = ex.getTransaction();
            txn.begin();
            for (int i = 1; i <= 10; i++) {
                ex.getValue().put(RED_FOX);
                if (i == 5) {
                    _persistit.flush();
                    _persistit.checkpoint();
                }
                ex.clear().append(i).store();
            }
            txn.rollback();
            txn.end();
        }
        final Properties properties = _persistit.getProperties();
        _persistit.crash();

        for (int i = 0; i < 10; i++) {
            _persistit = new Persistit();
            _persistit.initialize(properties);
            _persistit.close();
        }

        _persistit = new Persistit();
        _persistit.initialize(properties);
        _persistit.copyBackPages();
        assertTrue(_persistit.getJournalManager().getBaseAddress() > 10L * _persistit.getJournalManager()
                .getBlockSize());
    }
}
