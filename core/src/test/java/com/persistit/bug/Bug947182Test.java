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

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.TestShim;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.persistit.util.SequencerConstants.WRITE_WRITE_STORE_C;
import static com.persistit.util.SequencerConstants.WRITE_WRITE_STORE_SCHEDULE;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequence;
import static com.persistit.util.ThreadSequencer.sequencerHistory;

public class Bug947182Test extends PersistitUnitTestCase {
    private static final String TREE_NAME = "Bug947182Test";
    private static final String KEY = "key1";
    private static final String WWS = "WRITE_WRITE_STORE_";
    private static final String IN_A = "+" + WWS + "A";
    private static final String IN_B = "+" + WWS + "B";
    private static final String IN_C = "+" + WWS + "C";
    private static final String OUT_A = "-" + WWS + "A";
    private static final String OUT_B = "-" + WWS + "B";
    private static final String OUT_C = "-" + WWS + "C";

    /*
     * Bug seen many times but only in a large (100 terminal), long running
     * TPCC test as a hung thread inside of Buffer#repack() due to _alloc
     * appearing to have a zero-ed out header. This turned out to be a
     * secondary issue to the real problem.
     *
     * The Exchange#storeInternal() method tracks a number of variables
     * related to long record chains. There are four in total: new and old
     * for value being stored and MVV itself. Logically, we don't need to
     * keep new ones if the store doesn't succeed and we can't free old
     * ones until we have committed the new. We also don't need create
     * multiple long record chains for the new one if we have to retry for
     * any reason (e.g. split) as it isn't visible to anyone else yet.
     * Conversely, we do have to create multiple long MVVs (if required)
     * upon retry as the state of the MVV may have changed in-between.
     *
     * That is where the real bug was. The storeInternal() method would
     * incorrectly track the old long MVV pointer across retries. Imagine
     * this sequence:
     *  1) txn1 stores value at A
     *  2) txn2 tries to store at A but is stopped due to WW
     *  3) txn2 releases latches and sleeps for short period
     *  4) txn1 aborts
     *  5) txn2 wakes, sees tx1n aborted, and retries
     *  6) Buffer containing A is converted to short record (pruned, stored again, etc)
     *  7) txn2 re-acquires page and successfully stores value at A
     *
     *  If the value during step 2 was a long MVV and was reduced to a
     *  short MVV at step 6 (which would free the long record chain), then
     *  when txn2 completed at step 7 it would still be holding onto the
     *  old long record pointer.
     *
     *  Ultimately, that resulted in the same page address being on the
     *  garbage list twice. When the first came off and was being used for
     *  data and then simultaneously being used for as the garbage root,
     *  we would see the end result of _alloc appearing to be junk/zeroed out.
     */
    public void testDoubleFreeOfLongMvvChain() throws InterruptedException {
        enableSequencer(true);
        addSchedules(WRITE_WRITE_STORE_SCHEDULE);

        final Semaphore firstStore = new Semaphore(0);
        final ConcurrentLinkedQueue<Throwable> throwableList = new ConcurrentLinkedQueue<Throwable>();
        
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Exchange ex = null;
                try {
                    Transaction txn = _persistit.getTransaction();
                    ex = getExchange(_persistit);
                    txn.begin();
                    try {
                        ex.clear().append(KEY);
                        storeLongMVV(ex);
                        firstStore.release();
                        sequence(WRITE_WRITE_STORE_C);
                        txn.rollback();
                    } finally {
                        txn.end();
                    }

                    /*
                     * Store and rollback again but this time with a short value
                     * so the long record chain gets de-allocated
                     */
                    txn.begin();
                    try {
                        ex.clear().append(KEY);
                        ex.getValue().clear().put("in-between value");
                        ex.store();
                        txn.rollback();
                    } finally {
                        txn.end();
                    }

                    sequence(WRITE_WRITE_STORE_C);
                } catch(Throwable t) {
                    throwableList.add(t);
                } finally {
                    if (ex != null) {
                        _persistit.releaseExchange(ex);
                    }
                }
            }
        });
        
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                Exchange ex = null;
                try {
                    if(!firstStore.tryAcquire(5, TimeUnit.SECONDS)) {
                        throw new Exception("Timed out waiting for first store to complete");
                    }
                    Transaction txn = _persistit.getTransaction();
                    ex = getExchange(_persistit);
                    txn.begin();
                    try {
                        ex.clear().append(KEY).getValue().clear().put(KEY);
                        ex.store();
                        txn.commit();
                    } finally {
                        txn.end();
                        _persistit.releaseExchange(ex);
                    }
                } catch(Throwable t) {
                    throwableList.add(t);
                } finally {
                    _persistit.releaseExchange(ex);
                }
            }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        assertEquals("Threads had no excretions", "[]", throwableList.toString());
        
        final String expected = IN_C +","+ IN_A +","+ OUT_A +","+ OUT_C +","+ IN_B +","+ IN_C +","+ OUT_C +","+ OUT_B;
        assertEquals("Sequence order", expected, sequencerHistory());

    }
    
    private static Exchange getExchange(Persistit persistit) throws PersistitException {
        return persistit.getExchange(UnitTestProperties.VOLUME_NAME, TREE_NAME, true);
    }

    private static void storeLongMVV(Exchange ex) throws PersistitException {
        final int size = TestShim.maxStorableValueSize(ex, ex.getKey().getEncodedSize()) - 1;
        StringBuilder builder = new StringBuilder(size);
        while(builder.length() < size) {
            builder.append("0123456789");
        }
        builder.setLength(size);
        ex.getValue().clear().put(builder.toString());
        ex.store();
        assertEquals("Did store long MVV", true, TestShim.isValueLongRecord(ex));
    }
}
