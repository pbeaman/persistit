/**
 * Copyright 2005-2013 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.ConcurrentUtil;
import com.persistit.unit.UnitTestProperties;

/**
 * <p>
 * Inspired by bug1126297: Assertion failure in
 * TransactionIndexBucket#allocateTransactionStatus
 * </p>
 * <p>
 * The symptom was the bug (a locked TransactionStatus on the free list) but the
 * cause was mishandling of abandoned transactions from the
 * {@link Persistit#cleanup()} method.
 * </p>
 * <p>
 * When attempting to rollback the abandoned transaction, the status was
 * notified and then unlocked. Since the lock was held by a now dead thread, an
 * IllegalMonitorStateException occurred. It was then put on the free list
 * during the next cleanup of that bucket since it had been notified.
 * </p>
 */
public class TransactionAbandonedTest extends PersistitUnitTestCase {
    private static final String TREE = TransactionAbandonedTest.class.getSimpleName();
    private static final int KEY_START = 1;
    private static final int KEY_RANGE = 10;
    private static final long MAX_TIMEOUT_MS = 10 * 1000;

    private static class TxnAbandoner extends ConcurrentUtil.ThrowingRunnable {
        private final Persistit persistit;
        private final boolean doRead;
        private final boolean doWrite;

        public TxnAbandoner(final Persistit persistit, final boolean doRead, final boolean doWrite) {
            this.persistit = persistit;
            this.doRead = doRead;
            this.doWrite = doWrite;
        }

        @Override
        public void run() throws PersistitException {
            final Transaction txn = persistit.getTransaction();
            txn.begin();
            if (doRead) {
                assertEquals("Traverse count", KEY_RANGE, scanAndCount(getExchange(persistit)));
            }
            if (doWrite) {
                loadData(persistit, KEY_START + KEY_RANGE, KEY_RANGE);
            }
        }
    }

    private static Exchange getExchange(final Persistit persistit) throws PersistitException {
        return persistit.getExchange(UnitTestProperties.VOLUME_NAME, TREE, true);
    }

    private static void loadData(final Persistit persistit, final int keyOffset, final int count)
            throws PersistitException {
        final Exchange ex = getExchange(persistit);
        for (int i = 0; i < count; ++i) {
            ex.clear().append(keyOffset + i).store();
        }
    }

    private static int scanAndCount(final Exchange ex) throws PersistitException {
        ex.clear().append(Key.BEFORE);
        int saw = 0;
        while (ex.next()) {
            ++saw;
        }
        return saw;
    }

    @Before
    public void disableAndLoad() throws PersistitException {
        disableBackgroundCleanup();
        loadData(_persistit, KEY_START, KEY_RANGE);
    }

    private void runAndCleanup(final String name, final boolean doRead, final boolean doWrite) {
        final Thread t = ConcurrentUtil.createThread(name, new TxnAbandoner(_persistit, false, false));
        ConcurrentUtil.startAndJoinAssertSuccess(MAX_TIMEOUT_MS, t);
        // Threw exception before fix
        _persistit.cleanup();
    }

    @Test
    public void noReadsOrWrites() {
        runAndCleanup("NoReadNoWrite", false, false);
    }

    @Test
    public void readOnly() throws PersistitException {
        runAndCleanup("ReadOnly", true, false);
        assertEquals("Traversed after abandoned", KEY_RANGE, scanAndCount(getExchange(_persistit)));
    }

    @Test
    public void readAndWrite() throws Exception {
        runAndCleanup("ReadAndWrite", true, true);
        assertEquals("Traversed after abandoned", KEY_RANGE, scanAndCount(getExchange(_persistit)));
        // Check that the abandoned was pruned
        final CleanupManager cm = _persistit.getCleanupManager();
        for (int i = 0; i < 5 && cm.getEnqueuedCount() > 0; ++i) {
            cm.runTask();
        }
        final Exchange rawEx = getExchange(_persistit);
        rawEx.ignoreMVCCFetch(true);
        assertEquals("Raw traversed after abandoned", KEY_RANGE, scanAndCount(rawEx));
    }
}
