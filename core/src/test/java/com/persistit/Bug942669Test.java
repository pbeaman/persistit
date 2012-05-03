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

package com.persistit;

import static com.persistit.util.SequencerConstants.RECOVERY_PRUNING_SCHEDULE;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static org.junit.Assert.assertNull;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

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
     * Test for a possible race condition examined in diagnosing 942669. Note
     * that file 1535 is empty, suggesting that the JOURNAL_COPIER thread
     * decided it was obsolete and deleted it. Hypothesis:
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

        /*
         * Create a journal with an uncommitted transaction
         */
        Exchange ex = _persistit.getExchange("persistit", "test", true);
        Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        for (int k = 1; k < 10; k++) {
            ex.clear().append(k).store();
        }
        _persistit.checkpoint();
        txn.commit();
        txn.end();
        _persistit.crash();
        Properties properties = _persistit.getProperties();
        _persistit = new Persistit();

        enableSequencer(true);
        addSchedules(RECOVERY_PRUNING_SCHEDULE);

        _persistit.initialize(properties);
        _persistit.copyBackPages();
        disableSequencer();
    }

    /**
     * Note that journal file 1345 has a length of zero. The issue is that
     * JournalManager.pruneObsoleteTransactions is attempting to prune two
     * aborted transactions that once upon a time lived in file 1345. Those
     * transactions were successfully pruned before the system was shut down,
     * and then incorrectly resurrected into the new new epoch. The solution is
     * to avoid resurrecting transactions which started before the base address
     * during startup. Prior to the fix, this code fails with an
     * IllegalArgumentException in the JOURNAL_COPIER thread.
     */

    @Test
    public void testResurrectedTransactions() throws Exception {
        /*
         * Create a journal with an uncommitted transaction
         */
        Exchange ex = _persistit.getExchange("persistit", "test", true);
        _persistit.flush();
        Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        for (int k = 1; k < 10; k++) {
            ex.clear().append(k).store();
        }

        _persistit.checkpoint();
        _persistit.getJournalManager().rollover();

        txn.rollback();
        txn.end();

        /*
         * copyBackPages creates a checkpoint, prunes obsolete transactions,
         * copies pages from the journal back to the volume and then deletes
         * obsolete journal files. We need to call it twice so that the results
         * of pruning get checkpointed.
         */
        _persistit.copyBackPages();
        _persistit.copyBackPages();

        _persistit.close();

        Properties properties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(properties);
        _persistit.copyBackPages();
        assertNull(_persistit.getJournalManager().getLastCopierException());
    }
}
