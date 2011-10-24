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

package com.persistit;

import static com.persistit.TransactionStatus.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import com.persistit.exception.TimeoutException;

public class TransactionIndexTest extends TestCase {

    private final TimestampAllocator _tsa = new TimestampAllocator();

    public void testBasicMethods() throws Exception {
        final TransactionIndex ti = new TransactionIndex(_tsa, 1);
        TransactionStatus ts1 = ti.registerTransaction();
        ti.updateActiveTransactionCache();
        assertTrue(ti.hasConcurrentTransaction(0, ts1.getTs() + 1));
        ts1.commit(_tsa.updateTimestamp());
        TransactionStatus ts2 = ti.registerTransaction();
        assertTrue(ti.hasConcurrentTransaction(0, ts1.getTs() + 1));
        assertTrue(ti.hasConcurrentTransaction(0, ts2.getTs() + 1));
        ti.updateActiveTransactionCache();
        assertFalse(ti.hasConcurrentTransaction(0, ts1.getTs() + 1));
        assertTrue(ti.hasConcurrentTransaction(0, ts2.getTs() + 1));
        ts2.commit(_tsa.updateTimestamp());
        ti.updateActiveTransactionCache();
        assertFalse(ti.hasConcurrentTransaction(0, ts2.getTs() + 1));
        TransactionStatus ts3 = ti.registerTransaction();
        _tsa.updateTimestamp();
        assertEquals(TransactionStatus.UNCOMMITTED, ti.commitStatus(TransactionIndex.ts2vh(ts3.getTs()), _tsa
                .getCurrentTimestamp(), 0));
        assertEquals(ts3.getTs(), ti.commitStatus(TransactionIndex.ts2vh(ts3.getTs()), ts3.getTs(), 0));
        ts3.incrementMvvCount();
        ts3.abort();
        assertEquals(TransactionStatus.ABORTED, ti.commitStatus(TransactionIndex.ts2vh(ts3.getTs()), _tsa
                .getCurrentTimestamp(), 0));
        assertEquals(3, ti.getCurrentCount());
        ti.notifyCompleted(ts1.getTs());
        assertEquals(ts1.getTs(), ti.commitStatus(TransactionIndex.ts2vh(ts1.getTs()), _tsa.getCurrentTimestamp(), 0));
        ti.notifyCompleted(ts2.getTs());
        ti.notifyCompleted(ts3.getTs());
        assertEquals(0, ti.getCurrentCount());
        assertEquals(2, ti.getFreeCount());
        assertEquals(1, ti.getAbortedCount());
        ts3.decrementMvvCount();
        ti.updateActiveTransactionCache();
        ti.cleanup();
        assertEquals(0, ti.getCurrentCount());
        assertEquals(3, ti.getFreeCount());
        assertEquals(0, ti.getAbortedCount());
    }

    public void testNonBlockingWwDependency() throws Exception {
        final TransactionIndex ti = new TransactionIndex(_tsa, 1);
        final TransactionStatus ts1 = ti.registerTransaction();
        final TransactionStatus ts2 = ti.registerTransaction();
        ts1.commit(_tsa.updateTimestamp());
        ti.notifyCompleted(ts1.getTs());
        /*
         * Should return true because ts1 and ts2 are concurrent.
         */
        assertEquals(ts1.getTc(), ti.wwDependency(TransactionIndex.ts2vh(ts1.getTs()), ts2.getTs(), 1000));
        final TransactionStatus ts3 = ti.registerTransaction();
        ts2.abort();
        ti.notifyCompleted(ts2.getTs());
        /*
         * Should return false because ts1 and ts3 are not concurrent
         */
        assertEquals(0, ti.wwDependency(TransactionIndex.ts2vh(ts1.getTs()), ts3.getTs(), 1000));
        /*
         * Should return false because ts2 aborted
         */
        assertEquals(ABORTED, ti.wwDependency(TransactionIndex.ts2vh(ts2.getTs()), ts3.getTs(), 1000));
        ts3.commit(_tsa.updateTimestamp());
    }

    public void testReduce() throws Exception {
        final TransactionStatus[] array = new TransactionStatus[100];
        final TransactionIndex ti = new TransactionIndex(_tsa, 1);

        for (int count = 0; count < array.length; count++) {
            array[count] = ti.registerTransaction();
            array[count].incrementMvvCount();
        }
        assertEquals(ti.getLongRunningThreshold(), ti.getCurrentCount());
        assertEquals(array.length - ti.getLongRunningThreshold(), ti.getLongRunningCount());

        for (int count = 20; count < 70; count++) {
            array[count].abort();
            ti.notifyCompleted(array[count].getTs());
        }
        for (int count = 50; count < 60; count++) {
            array[count].decrementMvvCount();
            ti.notifyPruned(array[count].getTs());
        }
        assertEquals(ti.getLongRunningThreshold(), ti.getCurrentCount());
        assertEquals(array.length - ti.getLongRunningThreshold() - ti.getAbortedCount() - ti.getFreeCount(), ti
                .getLongRunningCount());
        assertEquals(50, ti.getAbortedCount());
        for (int count = 0; count < 20; count++) {
            array[count].commit(array[20].getTs());
            ti.notifyCompleted(array[count].getTs());
        }
        assertEquals(ti.getMaxFreeListSize(), ti.getFreeCount());
        assertEquals(50, ti.getAbortedCount());
        assertEquals(ti.getLongRunningThreshold(), ti.getCurrentCount());
        assertEquals(array.length - ti.getCurrentCount() - ti.getAbortedCount() - ti.getFreeCount()
                - ti.getDroppedCount(), ti.getLongRunningCount());
        
        ti.updateActiveTransactionCache();
        ti.cleanup();
        /*
         * aborted set retained due to currently active transactions
         * that started before the mvvCount was decremented
         */
        assertEquals(50, ti.getAbortedCount());
        /*
         * Commit all remaining transactions so that there no currently active
         * transactions.
         */
        for (int count=70; count < array.length; count++) {
            array[count].commit(_tsa.updateTimestamp());
            ti.notifyCompleted(array[count].getTs());
        }
        /*
         * Refresh ActiveTransactionCache to recognize new commits. 
         */
        ti.updateActiveTransactionCache();
        assertEquals(50, ti.getAbortedCount());
        ti.cleanup();
        assertEquals(40, ti.getAbortedCount());
    }

    public void testBlockingWwDependency() throws Exception {
        final TransactionIndex ti = new TransactionIndex(_tsa, 1);
        final TransactionStatus ts1 = ti.registerTransaction();
        final TransactionStatus ts2 = ti.registerTransaction();
        final AtomicLong elapsed = new AtomicLong();
        final boolean result1 = tryBlockingWwDependency(ti, ts1, ts2.getTs(), 1000, 10000, elapsed, true);
        assertTrue(result1);
        assertTrue(elapsed.get() >= 900);
        final TransactionStatus ts3 = ti.registerTransaction();
        final boolean result2 = tryBlockingWwDependency(ti, ts2, ts3.getTs(), 1000, 10000, elapsed, false);
        assertFalse(result2);
        assertTrue(elapsed.get() >= 900);
    }

    boolean tryBlockingWwDependency(final TransactionIndex ti, final TransactionStatus ts1, final long ts,
            final long wait, final long timeout, final AtomicLong elapsed, boolean commit) throws Exception {
        final AtomicLong result = new AtomicLong();
        final Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    final long start = System.currentTimeMillis();
                    result.set(ti.wwDependency(TransactionIndex.ts2vh(ts1.getTs()), ts, timeout));
                    elapsed.set(System.currentTimeMillis() - start);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
        Thread.sleep(wait);
        if (commit) {
            ts1.commit(_tsa.updateTimestamp());
        } else {
            ts1.abort();
        }
        ti.notifyCompleted(ts1.getTs());
        t.join();
        return result.get() > 0;
    }
}
