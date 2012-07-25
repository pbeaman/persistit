/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.exception.TreeNotFoundException;

/**
 * Attempt to cover all cases from the pseudo graph below and ensure that the
 * TransactionIndex, JournalManger#_liveTransactionMap, and any k/v stored are
 * in the proper state after each step.
 * 
 * <pre>
 * 
 *                     +--> abort -> (done)
 *                     |
 *          +-----+    +--> commit -> (done)
 *  (in) -->| seq |--->|
 *          +-----+    +-> restart -> (done)
 *                     |
 *                     +-> checkpoint -> (out)
 * 
 * 
 *        (seq)        (seq)            (seq)
 *          |            |                |
 *  begin --+--> write --+--> writeMany --+--> write --> (seq)
 * 
 * </pre>
 * 
 * Note that tests including a RESTART assume that any aborted transaction was
 * fully pruned and removed from the running state.
 */
public class TransactionLifetimeTest extends PersistitUnitTestCase {
    private static final String VOLUME_NAME = "persistit";
    private static final String TREE_NAME = "transaction_lifetime_test";
    private static final String KEY_PREFIX = "key_";
    private static final String VALUE_PREFIX = "value_";

    public final void setUp() throws Exception {
        final long FIVE_MIN_NANOS = 1000000000L * 60 * 5;
        _persistit.getCheckpointManager().setCheckpointIntervalNanos(FIVE_MIN_NANOS);
        super.setUp();
        _persistit.getJournalManager().setRollbackPruningEnabled(false);
        _persistit.getJournalManager().setWritePagePruningEnabled(false);

    }

    @Test
    public void testBeginAbort() throws PersistitException {
        doTest(false, false, ABORT);
    }

    @Test
    public void testBeginCommit() throws PersistitException, InterruptedException {
        doTest(false, false, COMMIT);
    }

    @Test
    public void testBeginCheckpointAbort() throws PersistitException {
        doTest(false, false, CHECKPOINT, ABORT);
    }

    @Test
    public void testBeginCheckpointCommit() throws PersistitException {
        doTest(false, false, CHECKPOINT, COMMIT);
    }

    @Test
    public void testBeginWriteAbort() throws PersistitException {
        doTest(true, true, WRITE, ABORT);
    }

    @Test
    public void testBeginWriteCommit() throws PersistitException {
        doTest(false, true, WRITE, COMMIT);
    }

    @Test
    public void testBeginCheckpointWriteAbort() throws PersistitException {
        doTest(true, true, CHECKPOINT, WRITE, ABORT);
    }

    @Test
    public void testBeginCheckpointWriteCommit() throws PersistitException {
        doTest(false, true, CHECKPOINT, WRITE, COMMIT);
    }

    @Test
    public void testBeginWriteCheckpointAbort() throws PersistitException {
        doTest(true, true, WRITE, CHECKPOINT, ABORT);
    }

    @Test
    public void testBeginWriteCheckpointCommit() throws PersistitException {
        doTest(false, true, WRITE, CHECKPOINT, COMMIT);
    }

    @Test
    public void testBeginWriteCheckpointWriteAbort() throws PersistitException {
        doTest(true, true, WRITE, CHECKPOINT, WRITE, ABORT);
    }

    @Test
    public void testBeginWriteCheckpointWriteCommit() throws PersistitException {
        doTest(false, true, WRITE, CHECKPOINT, WRITE, COMMIT);
    }

    @Test
    public void testBeginWriteManyAbort() throws PersistitException {
        doTest(true, true, WRITE_MANY, ABORT);
    }

    @Test
    public void testBeginWriteManyCommit() throws PersistitException {
        doTest(false, true, WRITE_MANY, COMMIT);
    }

    @Test
    public void testBeginWriteManyCheckpointAbort() throws PersistitException {
        doTest(true, true, WRITE_MANY, CHECKPOINT, ABORT);
    }

    @Test
    public void testBeginWriteManyCheckpointCommit() throws PersistitException {
        doTest(false, true, WRITE_MANY, CHECKPOINT, COMMIT);
    }

    @Test
    public void testBeginWriteManyCheckpointWriteAbort() throws PersistitException {
        doTest(true, true, WRITE_MANY, CHECKPOINT, WRITE, ABORT);
    }

    @Test
    public void testBeginWriteManyCheckpointWriteCommit() throws PersistitException {
        doTest(false, true, WRITE_MANY, CHECKPOINT, WRITE, COMMIT);
    }

    @Test
    public void testBeginWriteManyCheckpointWriteManyAbort() throws PersistitException {
        doTest(true, true, WRITE_MANY, CHECKPOINT, WRITE_MANY, ABORT);
    }

    @Test
    public void testBeginWriteManyCheckpointWriteManyCommit() throws PersistitException {
        doTest(false, true, WRITE_MANY, CHECKPOINT, WRITE_MANY, COMMIT);
    }

    @Test
    public void testBeginRestart() throws PersistitException {
        doTest(false, false, RESTART);
    }

    @Test
    public void testBeginAbortRestart() throws PersistitException {
        doTest(false, false, ABORT, RESTART);
    }

    @Test
    public void testBeginCommitRestart() throws PersistitException {
        doTest(false, false, COMMIT, RESTART);
    }

    @Test
    public void testBeginWriteAbortRestart() throws PersistitException {
        doTest(false, false, WRITE, ABORT, RESTART);
    }

    @Test
    public void testBeginWriteCommitRestart() throws PersistitException {
        doTest(false, false, WRITE, COMMIT, RESTART);
    }

    @Test
    public void testBeginWriteManyAbortRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, ABORT, RESTART);
    }

    @Test
    public void testBeginWriteManyCommitRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, COMMIT, RESTART);
    }

    @Test
    public void testBeginWriteCheckpointAbortRestart() throws PersistitException {
        doTest(false, false, WRITE, CHECKPOINT, ABORT, RESTART);
    }

    @Test
    public void testBeginWriteCheckpointCommitRestart() throws PersistitException {
        doTest(false, false, WRITE, CHECKPOINT, COMMIT, RESTART);
    }

    @Test
    public void testBeginWriteManyAbortCheckpointRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, ABORT, CHECKPOINT, RESTART);
    }

    @Test
    public void testBeginWriteManyCommitCheckpointRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, COMMIT, CHECKPOINT, RESTART);
    }

    @Test
    public void testBeginWriteManyCheckpointAbortRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, CHECKPOINT, ABORT, RESTART);
    }

    @Test
    public void testBeginWriteManyCheckpointCommitRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, CHECKPOINT, COMMIT, RESTART);
    }

    private static class Node {
        public Node(String description) {
            _description = description;
        }

        @Override
        public String toString() {
            return _description;
        }

        private final String _description;
    }

    private static final Node ABORT = new Node("ABORT");
    private static final Node COMMIT = new Node("COMMIT");
    private static final Node WRITE = new Node("WRITE");
    private static final Node WRITE_MANY = new Node("WRITE_MANY");
    private static final Node CHECKPOINT = new Node("CHECKPOINT");
    private static final Node RESTART = new Node("RESTART");

    private static int storeMoreThanTxnBuffer(Exchange ex, int writeCount) throws PersistitException {
        ByteBuffer txnBuffer = ex.getTransaction().getTransactionBuffer();
        for (;;) {
            final int prevPos = txnBuffer.position();
            ex.clear().append(KEY_PREFIX + writeCount);
            ex.getValue().clear().put(VALUE_PREFIX + writeCount);
            ex.store();
            ++writeCount;
            if (prevPos > txnBuffer.position()) {
                break;
            }
        }
        return writeCount;
    }

    private void checkKeys(boolean shouldExist, int writeCount) throws PersistitException {
        Exchange ex = null;
        try {
            ex = _persistit.getExchange(VOLUME_NAME, TREE_NAME, false);
        } catch (TreeNotFoundException e) {
            if (shouldExist && writeCount > 0) {
                fail("Keys expected but tree does not exist: " + e);
            } else {
                return;
            }
        }
        try {
            for (int i = 0; i < writeCount; ++i) {
                final String expectedKey = KEY_PREFIX + i;
                final boolean isDefined = ex.clear().append(expectedKey).isValueDefined();
                assertEquals(expectedKey + " exists after test", shouldExist, isDefined);
            }
        } finally {
            _persistit.releaseExchange(ex);
        }
    }

    private void checkTransaction(String desc, long ts, Boolean statusExists, Boolean liveMapExists) {
        if (statusExists != null) {
            _persistit.getTransactionIndex().cleanup();
            final TransactionStatus status = _persistit.getTransactionIndex().getStatus(ts);
            final boolean actualInTxnIndex = status != null;
            assertEquals("TransactionStatus exists after " + desc + "  ", statusExists.booleanValue(), actualInTxnIndex);
        }

        if (liveMapExists != null) {
            final boolean actualInLiveMap = _persistit.getJournalManager().unitTestTxnExistsInLiveMap(ts);
            assertEquals("Transaction in live map after " + desc + "  ", liveMapExists.booleanValue(), actualInLiveMap);
        }
    }

    private void doTest(boolean expectedInIndex, boolean expectedInLiveMap, Node... nodes) throws PersistitException {
        Exchange ex = _persistit.getExchange(VOLUME_NAME, TREE_NAME, true);
        Transaction txn = ex.getTransaction();

        int writeCount = 0;
        int fullWriteCount = 0;
        boolean aborted = false;
        boolean committed = false;
        boolean currentInTxnIndex = true;
        boolean currentInLiveMap = false;
        String stateDescription = "BEGIN";

        txn.begin();
        final long ts = txn.getStartTimestamp();
        try {
            for (Node curNode : nodes) {
                stateDescription += "," + curNode;

                if (curNode == ABORT) {
                    txn.rollback();
                    aborted = true;
                    currentInTxnIndex = (writeCount > 0);
                    currentInLiveMap = (writeCount > 0);
                    fullWriteCount += writeCount;
                    writeCount = 0;
                } else if (curNode == COMMIT) {
                    txn.commit();
                    committed = true;
                    currentInTxnIndex = false;
                    currentInLiveMap = (writeCount > 0);
                    fullWriteCount += writeCount;
                    writeCount = 0;
                } else if (curNode == WRITE) {
                    ex.clear().append(KEY_PREFIX + writeCount);
                    ex.getValue().clear().put(VALUE_PREFIX + writeCount);
                    ex.store();
                    ++writeCount;
                } else if (curNode == WRITE_MANY) {
                    writeCount = storeMoreThanTxnBuffer(ex, writeCount);
                    currentInLiveMap = true;
                } else if (curNode == CHECKPOINT) {
                    CheckpointManager.Checkpoint cp = _persistit.checkpoint();
                    assertEquals("Checkpoint successfully written", true, cp != null);
                    currentInLiveMap = (!committed && writeCount > 0) || (aborted && writeCount == 0);
                } else if (curNode == RESTART) {
                    txn = null;
                    ex = null;
                    currentInTxnIndex = false;
                    currentInLiveMap = false;
                    safeCrashAndRestoreProperties();
                    _persistit.getJournalManager().setRollbackPruningEnabled(false);
                    _persistit.getJournalManager().setWritePagePruningEnabled(false);

                } else {
                    fail("Unknown test node: " + curNode);
                }

                checkTransaction(stateDescription, ts, currentInTxnIndex, currentInLiveMap);
            }
        } finally {
            if (txn != null) {
                txn.end();
            }
        }

        checkTransaction("POST_CONDITION", ts, expectedInIndex, expectedInLiveMap);

        if (aborted && committed) {
            fail("Saw both commit AND abort?");
        }

        checkKeys(committed, fullWriteCount);
    }
}
