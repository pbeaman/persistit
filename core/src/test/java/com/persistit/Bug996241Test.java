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

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Bug996241Test extends PersistitUnitTestCase {
    final static String TREE_NAME1 = "Bug996241Test_1";
    final static String TREE_NAME2 = "Bug996241Test_2";

    @Test
    public void pruningAssertAfterRollover() throws Exception {
        // Make it more obvious if we jump backwards
        _persistit.getTimestampAllocator().bumpTimestamp(1000000);

        // Write records until we have enough journal
        Exchange ex = getExchange(TREE_NAME1);
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        for(long i = 0, curSize = 0; curSize < JournalManager.ROLLOVER_THRESHOLD; i += 1000) {
            writeRecords(ex, i, 1000);
            curSize = _persistit.getJournalManager().getCurrentJournalSize();
        }
        txn.commit();
        txn.end();
        _persistit.releaseExchange(ex);

        // Now write a few records that won't be pruned
        ex = getExchange(TREE_NAME2);
        txn.begin();
        writeRecords(ex, 0, 10);
        txn.commit();
        txn.end();
        _persistit.releaseExchange(ex);

        /*
         * Two iterations needed:
         * 1) Dirty pages from writes
         * 2) Empty checkpoint (baseAddress now equals curAddress - CP.OVERHEAD)
         */
        for(int i = 0; i < 2; ++i) {
            _persistit.checkpoint();
            _persistit.getJournalManager().copyBack();
        }

        crashWithoutFlushAndRestoreProperties();

        // Timestamp was now back in time, writing same records creates invalid MVVs
        txn = _persistit.getTransaction();
        ex = getExchange(TREE_NAME2);
        txn.begin();
        writeRecords(ex, 0, 10);
        txn.commit();
        txn.end();

        // Pruning caused assert
        ex.clear().append(0);
        ex.prune();
    }

    @Test
    public void rolloverCreatesNewJournal() throws PersistitException {
        JournalManager jman = _persistit.getJournalManager();
        final int count1 = jman.getJournalFileCount();
        jman.rolloverWithNewFile();
        final int count2 = jman.getJournalFileCount();
        assertTrue("Rollover created a new journal file", count2 > count1);
    }

    @Test
    public void volumeSavesTimestamp() throws Exception {
        final int RECORDS = 100;
        // Make it more obvious if we jump backwards
        _persistit.getTimestampAllocator().bumpTimestamp(1000000);

        // Write records to check on later
        Exchange ex = getExchange(TREE_NAME1);
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        writeRecords(ex, 0, RECORDS);
        txn.commit();
        txn.end();

        _persistit.copyBackPages();
        List<File> journalFiles = _persistit.getJournalManager().unitTestGetAllJournalFiles();

        Properties properties = _persistit.getProperties();
        _persistit.crash();

        /*
         * Worst case (or slipped finger) scenario of missing journal files
         */
        for(File file : journalFiles) {
            boolean success = file.delete();
            assertEquals("Deleted journal file " + file.getName(), true, success);
        }

        _persistit = new Persistit();
        _persistit.initialize(properties);

        /*
         * If the volume had not saved a timestamp, we wouldn't be
         * able to see any of the un-pruned records
         */
        int sawCount = 0;
        ex = getExchange(TREE_NAME1);
        txn = _persistit.getTransaction();
        txn.begin();
        ex.clear().append(Key.BEFORE);
        while(ex.next(true)) {
            ++sawCount;
        }
        txn.commit();
        txn.end();

        assertEquals("Traversed record count", RECORDS, sawCount);
    }

    private void writeRecords(Exchange ex, long offset, int count) throws PersistitException {
        for(int i = 0; i < count; ++i) {
            ex.clear().append(offset + i);
            ex.getValue().put(i);
            ex.store();
        }
    }

    private Exchange getExchange(String tree) throws PersistitException {
        return _persistit.getExchange(UnitTestProperties.VOLUME_NAME, tree, true);
    }
}
