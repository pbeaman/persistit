/**
 * Copyright © 2005-2013 Akiban Technologies, Inc.  All rights reserved.
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

import com.persistit.exception.PersistitException;
import com.persistit.unit.ConcurrentUtil;
import org.junit.Before;
import org.junit.Test;

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
    private static final long MAX_TIMEOUT_MS = 10 * 1000;

    enum TxnEnd {
        NONE, COMMIT, ROLLBACK
    }

    private static class TxnRunnable extends ConcurrentUtil.ThrowingRunnable {
        private final Persistit persistit;
        private final TxnEnd txnEnd;

        public TxnRunnable(Persistit persistit, TxnEnd txnEnd) {
            this.persistit = persistit;
            this.txnEnd = txnEnd;
        }

        @Override
        public void run() throws PersistitException {
            Transaction txn = persistit.getTransaction();
            txn.begin();
            if (txnEnd != TxnEnd.NONE) {
                if (txnEnd == TxnEnd.COMMIT) {
                    txn.commit();
                } else {
                    txn.rollback();
                }
                txn.end();
            }
        }
    }

    @Before
    public void stopCleanupManager() {
        _persistit.getCleanupManager().setPollInterval(-1);
    }

    @Test
    public void noReadsOrWrites() {
        Thread t = ConcurrentUtil.createThread("NoReadNoWrite", new TxnRunnable(_persistit, TxnEnd.NONE));
        ConcurrentUtil.startAndJoinAssertSuccess(MAX_TIMEOUT_MS, t);
        // Threw exception before fix
        _persistit.cleanup();
    }
}
