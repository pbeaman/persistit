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
import static com.persistit.util.ThreadSequencer.*;
import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.TestShim;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.util.Debug;

public class Bug942669Test extends PersistitUnitTestCase {

    // See https://bugs.launchpad.net/akiban-persistit/+bug/942669
    //
    // persistit version: 3.0.5
    // server version: 1.0.4
    //
    // At client site, disk filled up due to journal files not being purged. The
    // contents of the akiban data directory looked like:
    //
    // [root@admindb02 akiban]# ls -ltrh /var/lib/akiban/
    // total 55G
    // -rw-r--r-- 1 akiban akiban 330M Feb 27 10:33 akiban_journal.000000001346
    // -rw-r--r-- 1 akiban akiban 0 Feb 27 10:34 akiban_txn.v0.lck
    // -rw-r--r-- 1 akiban akiban 0 Feb 27 10:34 akiban_system.v0.lck
    // -rw-r--r-- 1 akiban akiban 0 Feb 27 10:34 akiban_data.v01.lck
    // -rw-r--r-- 1 akiban akiban 0 Feb 27 10:34 akiban_journal.000000001345
    // -rw-r--r-- 1 akiban akiban 1.0M Feb 27 10:35 akiban_txn.v0
    // -rw-r--r-- 1 akiban akiban 48K Feb 27 10:36 akiban_system.v0
    // -rw-r--r-- 1 akiban akiban 954M Feb 27 10:44 akiban_journal.000000001347
    // -rw-r--r-- 1 akiban akiban 954M Feb 27 10:56 akiban_journal.000000001348
    // -rw-r--r-- 1 akiban akiban 954M Feb 27 11:09 akiban_journal.000000001349

    /**
     * Test for a race condition that probably caused 942669. Note that file
     * 1535 is empty, suggesting that the JOURNAL_COPIER thread decided it was
     * obsolete and deleted it. Hypothesis:
     * 
     * 1. RecoveryManager pruned an aborted transaction
     * 
     * 2. RecoveryManager then set its MVV count to zero.
     * 
     * 3. JOURNAL_COPIER thread pruneObsoleteTransactions then removed the
     * TransactionMapItem for it from the live map.
     * 
     * 4. RecoveryManager called writeTransactionToJournal, which reinstated the
     * TransactionMapItem.
     * 
     * 5. JOURNAL_COPIER then repeatedly tried to prune the transaction, causing
     * an attempt to read file 1345. That attempt caused creation of a new
     * zero-length file and then failed with an FileNotFoundException.
     * 
     * 6. Rinse and repeat until the disk fills up.
     * 
     * This code attempts to recreate that scenario.
     * 
     * @throws Exception
     */
    @Test
    public void testRecoveryRace() throws Exception {
        
        enableSequencer(true);


        final Exchange ex = _persistit.getExchange("persistit", "test", true);
        final Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        for (int k = 1; k < 10; k++) {
            ex.clear().append(k).store();
        }
        txn.commit();
        txn.end();
    }
}
