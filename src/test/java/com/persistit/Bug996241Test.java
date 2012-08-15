/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import com.persistit.exception.PersistitException;
import com.persistit.unit.UnitTestProperties;

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
        for (long i = 0, curSize = 0; curSize < JournalManager.ROLLOVER_THRESHOLD; i += 1000) {
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
         * Two iterations needed: 1) Dirty pages from writes 2) Empty checkpoint
         * (baseAddress now equals curAddress - CP.OVERHEAD)
         */
        for (int i = 0; i < 2; ++i) {
            _persistit.checkpoint();
            _persistit.getJournalManager().copyBack();
        }

        crashWithoutFlushAndRestoreProperties();

        // Timestamp was now back in time, writing same records creates invalid
        // MVVs
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
        final JournalManager jman = _persistit.getJournalManager();
        final int count1 = jman.getJournalFileCount();
        jman.rolloverWithNewFile();
        final int count2 = jman.getJournalFileCount();
        assertTrue("Rollover created a new journal file", count2 > count1);
    }

    private void writeRecords(final Exchange ex, final long offset, final int count) throws PersistitException {
        for (int i = 0; i < count; ++i) {
            ex.clear().append(offset + i);
            ex.getValue().put(i);
            ex.store();
        }
    }

    private Exchange getExchange(final String tree) throws PersistitException {
        return _persistit.getExchange(UnitTestProperties.VOLUME_NAME, tree, true);
    }
}
