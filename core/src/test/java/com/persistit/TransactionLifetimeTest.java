/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

import java.nio.ByteBuffer;

/**
 * Attempt to cover all cases from the pseudo graph below and ensure that
 * the TransactionIndex and JournalManger#_liveTransactionMap are in the
 * desired state after completing the sequence.
 *
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
 */
public class TransactionLifetimeTest extends PersistitUnitTestCase {
    private static final String VOLUME_NAME = "persistit";
    private static final String TREE_NAME = "transaction_lifetime_test";
    private static final String KEY_PREFIX = "key_";
    private static final String VALUE_PREFIX = "value_";

    public final void setUp() throws Exception {
        final long FIVE_MIN_NANOS = 1000000000L * 60 * 5;
        _persistit.getCheckpointManager().setCheckpointInterval(FIVE_MIN_NANOS);
        super.setUp();
    }

    public void testBeginAbort() throws PersistitException {
        doTest(false, false, ABORT);
    }

    public void testBeginCommit() throws PersistitException, InterruptedException {
        doTest(false, false, COMMIT);
    }

    public void testBeginCheckpointAbort() throws PersistitException {
        doTest(false, false, CHECKPOINT, ABORT);
    }

    public void testBeginCheckpointCommit() throws PersistitException {
        doTest(false, false, CHECKPOINT, COMMIT);
    }

    public void testBeginWriteAbort() throws PersistitException {
        doTest(true, false, WRITE, ABORT);
    }

    public void testBeginWriteCommit() throws PersistitException {
        doTest(false, true, WRITE, COMMIT);
    }

    public void testBeginCheckpointWriteAbort() throws PersistitException {
        doTest(true, false, CHECKPOINT, WRITE, ABORT);
    }

    public void testBeginCheckpointWriteCommit() throws PersistitException {
        doTest(false, true, CHECKPOINT, WRITE, COMMIT);
    }

    public void testBeginWriteCheckpointAbort() throws PersistitException {
        doTest(true, true, WRITE, CHECKPOINT, ABORT);
    }

    public void testBeginWriteCheckpointCommit() throws PersistitException {
        doTest(false, true, WRITE, CHECKPOINT, COMMIT);
    }

    public void testBeginWriteCheckpointWriteAbort() throws PersistitException {
        doTest(true, true, WRITE, CHECKPOINT, WRITE, ABORT);
    }

    public void testBeginWriteCheckpointWriteCommit() throws PersistitException {
        doTest(false, true, WRITE, CHECKPOINT, WRITE, COMMIT);
    }

    public void testBeginWriteManyAbort() throws PersistitException {
        doTest(true, false, WRITE_MANY, ABORT);
    }

    public void testBeginWriteManyCommit() throws PersistitException {
        doTest(false, true, WRITE_MANY, COMMIT);
    }

    public void testBeginWriteManyCheckpointAbort() throws PersistitException {
        doTest(true, true, WRITE_MANY, CHECKPOINT, ABORT);
    }

    public void testBeginWriteManyCheckpointCommit() throws PersistitException {
        doTest(false, true, WRITE_MANY, CHECKPOINT, COMMIT);
    }

    public void testBeginWriteManyCheckpointWriteAbort() throws PersistitException {
        doTest(true, true, WRITE_MANY, CHECKPOINT, WRITE, ABORT);
    }

    public void testBeginWriteManyCheckpointWriteCommit() throws PersistitException {
        doTest(false, true, WRITE_MANY, CHECKPOINT, WRITE, COMMIT);
    }

    public void testBeginWriteManyCheckpointWriteManyAbort() throws PersistitException {
        doTest(true, true, WRITE_MANY, CHECKPOINT, WRITE_MANY, ABORT);
    }

    public void testBeginWriteManyCheckpointWriteManyCommit() throws PersistitException {
        doTest(false, true, WRITE_MANY, CHECKPOINT, WRITE_MANY, COMMIT);
    }

    public void testBeginRestart() throws PersistitException {
        doTest(false, false, RESTART);
    }

    public void testBeginAbortRestart() throws PersistitException {
        doTest(false, false, ABORT, RESTART);
    }

    public void testBeginCommitRestart() throws PersistitException {
        doTest(false, false, COMMIT, RESTART);
    }

    public void testBeginWriteAbortRestart() throws PersistitException {
        doTest(false, false, WRITE, ABORT, RESTART);
    }

    public void testBeginWriteCommitRestart() throws PersistitException {
        doTest(false, false, WRITE, COMMIT, RESTART);
    }

    public void testBeginWriteManyAbortRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, ABORT, RESTART);
    }

    public void testBeginWriteManyCommitRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, COMMIT, RESTART);
    }

    public void testBeginWriteCheckpointAbortRestart() throws PersistitException {
        doTest(false, false, WRITE, CHECKPOINT, ABORT, RESTART);
    }

    public void testBeginWriteCheckpointCommitRestart() throws PersistitException {
        doTest(false, false, WRITE, CHECKPOINT, COMMIT, RESTART);
    }

    public void testBeginWriteManyAbortCheckpointRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, ABORT, CHECKPOINT, RESTART);
    }

    public void testBeginWriteManyCommitCheckpointRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, COMMIT, CHECKPOINT, RESTART);
    }

    public void testBeginWriteManyCheckpointAbortRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, CHECKPOINT, ABORT, RESTART);
    }

    public void testBeginWriteManyCheckpointCommitRestart() throws PersistitException {
        doTest(false, false, WRITE_MANY, CHECKPOINT, COMMIT, RESTART);
    }


    private static final int ABORT        = 1;
    private static final int COMMIT       = 2;
    private static final int WRITE        = 3;
    private static final int WRITE_MANY   = 4;
    private static final int CHECKPOINT   = 5;
    private static final int RESTART      = 6;


    private static int storeFillTxnBuffer(Exchange ex, int writeCount) throws PersistitException {
        ByteBuffer txnBuffer= ex.getTransaction().getTransactionBuffer();
        final int remainingStart = txnBuffer.remaining();
        while (txnBuffer.remaining() >= remainingStart) {
            ex.clear().append(KEY_PREFIX + writeCount);
            ex.getValue().clear().put(VALUE_PREFIX + writeCount);
            ex.store();
            ++writeCount;
        }
        return writeCount;
    }

    private void doTest(boolean expectedInIndex, boolean expectedInLiveMap, int... pieces) throws PersistitException {
        final Exchange ex = _persistit.getExchange(VOLUME_NAME, TREE_NAME, true);
        final Transaction txn = ex.getTransaction();
        int writeCount = 0;

        txn.begin();
        final long ts = txn.getStartTimestamp();
        try {
            for (int piece : pieces) {
                switch (piece) {
                    case ABORT:
                        txn.rollback();
                    break;
                    
                    case COMMIT:
                        txn.commit();
                    break;
                    
                    case WRITE:
                        ex.clear().append(KEY_PREFIX + writeCount);
                        ex.getValue().clear().put(VALUE_PREFIX + writeCount);
                        ex.store();
                        ++writeCount;
                    break;
    
                    case WRITE_MANY:
                        writeCount = storeFillTxnBuffer(ex, writeCount);
                    break;
    
                    case CHECKPOINT:
                        CheckpointManager.Checkpoint cp = _persistit.checkpoint();
                        assertEquals("Checkpoint successfully written", true, cp != null);
                    break;

                    case RESTART:
                        safeCrashAndRestoreProperties();
                        break;

                    default:
                        fail("Unknown test piece: " + piece);
                    break;
                }
            }
        } finally {
            txn.end();
        }

        _persistit.getTransactionIndex().cleanup();
        final TransactionStatus status = _persistit.getTransactionIndex().getStatus(ts);
        final boolean actualInTxnIndex = status != null;
        assertEquals("TransactionStatus exists after test", expectedInIndex, actualInTxnIndex);

        final boolean actualInLiveMap = _persistit.getJournalManager().unitTestTxnExistsInLiveMap(ts);
        assertEquals("Transaction exists in live map after test", expectedInLiveMap, actualInLiveMap);
    }
}
