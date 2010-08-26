package com.persistit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.persistit.JournalManager.FileAddress;
import com.persistit.JournalManager.JournalNotClosedException;
import com.persistit.exception.CorruptJournalException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;

/**
 * Holds pointers to Transactions that must be applied during recovery. Called
 * by the JournalManager during recovery processing to build a map of
 * timestamp->FileAddress of transaction records that must be processed to
 * finish recovery.
 * 
 * Transactions are applied in their commit timestamp ordering so that their
 * affect on the recovered database is consistent with their original serial
 * order.
 * 
 * A checkpoint at timestamp T indicates that all pages made dirty prior to T
 * have been written to the journal; therefore any transaction with a commit
 * timestamp before T does not need to be reapplied because its effects are
 * already present in the recovered B-Trees.
 * 
 * This class is not threadsafe; it is intended to be called only during the
 * single- threaded recovery process.
 * 
 * @author peter
 * 
 */
public class RecoveryPlan {

    private static final int _readBufferSize = 4 * 1024 * 1024;

    private final SortedMap<Long, TRecord> _transactionMap = new TreeMap<Long, TRecord>();

    private final Persistit _persistit;

    private FileAddress _failureFileAddress;

    private final byte[] _bytes = new byte[128];

    private volatile int _appliedCount;

    private enum State {
        SCANNING, COMPLETE, STARTED, COMMITTED, ABORTED,
    };

    private static class TRecord {

        private final FileAddress _startsAt;

        private State _state = State.SCANNING;

        private TRecord(final FileAddress start) {
            _startsAt = start;
        }

        void setState(final State state) {
            if (state.ordinal() < _state.ordinal()) {
                throw new IllegalArgumentException("Can't revert from "
                        + _state + " to " + state);
            }
            _state = state;
        }

        FileAddress getStartsAt() {
            return _startsAt;
        }

        State getState() {
            return _state;
        }

        boolean isComplete() {
            return _state == State.COMPLETE;
        }

        boolean isDone() {
            return _state == State.COMMITTED || _state == State.ABORTED;
        }

        @Override
        public String toString() {
            return _startsAt + "_" + _state;
        }
    }

    RecoveryPlan(final Persistit persistit) {
        _persistit = persistit;
    }

    /**
     * Called by the JournalManager to record the FileAddress of a Transaction
     * Start record in the journal.
     * 
     * @param fa
     * @throws CorruptJournalException
     */
    public void startTransaction(final FileAddress fa)
            throws CorruptJournalException {
        final Long key = Long.valueOf(fa.getTimestamp());
        final TRecord previous = _transactionMap.get(key);
        if (previous != null) {
            throw new CorruptJournalException(
                    "Duplicate transactions with same timestamp(" + key
                            + "): previous/current=" + previous.getStartsAt()
                            + "/" + fa);
        }
        _transactionMap.put(key, new TRecord(fa));
    }

    /**
     * Called by the JournalManager to record the FileAddress of a Transaction
     * Commit record in the journal.
     * 
     * @param fa
     * @throws CorruptJournalException
     */
    public void commitTransaction(final FileAddress fa)
            throws CorruptJournalException {
        final Long key = Long.valueOf(fa.getTimestamp());
        final TRecord previous = _transactionMap.get(key);
        if (previous == null) {
            throw new CorruptJournalException(
                    "Missing Transaction Start record for timestamp(" + key
                            + ") at " + fa);
        } else if (previous.isComplete()) {
            throw new CorruptJournalException(
                    "Redundant Transaction Commit Record for " + previous
                            + " at " + fa);
        }
        previous.setState(State.COMPLETE);
    }

    /**
     * Called by the JournalManager to record the FileAddress of a Transaction
     * Rollback record in the journal.
     * 
     * @param fa
     * @throws CorruptJournalException
     */
    public void rollbackTransaction(final FileAddress fa)
            throws CorruptJournalException {
        final Long key = Long.valueOf(fa.getTimestamp());
        final TRecord previous = _transactionMap.get(key);
        if (previous == null) {
            throw new CorruptJournalException(
                    "Missing Transaction Start record for timestamp(" + key
                            + ") at " + fa);
        } else if (previous.isComplete()) {
            throw new CorruptJournalException(
                    "Invalid Transaction Rollback record for " + previous
                            + " at " + fa);
        }
        _transactionMap.remove(previous._startsAt.getTimestamp());
    }

    /**
     * Called by the JournalManager to record the FileAddress of a checkpoint
     * record in the journal.
     * 
     * @param fa
     */
    public void checkpoint(final FileAddress fa) {
        final long timestamp = fa.getTimestamp();
        for (final Iterator<Map.Entry<Long, TRecord>> iterator = _transactionMap
                .entrySet().iterator(); iterator.hasNext();) {
            final Map.Entry<Long, TRecord> entry = iterator.next();
            if (entry.getValue().getStartsAt().getTimestamp() < timestamp) {
                iterator.remove();
            } else {
                break;
            }
        }
    }

    public int getCommittedCount() {
        int count = 0;
        for (final TRecord trecord : _transactionMap.values()) {
            if (trecord.isComplete()) {
                count++;
            }
        }
        return count;
    }

    public int getUncommittedCount() {
        int count = 0;
        for (final TRecord trecord : _transactionMap.values()) {
            if (!trecord.isComplete()) {
                count++;
            }
        }
        return count;
    }

    public int getAppliedCount() {
        return _appliedCount;
    }

    public int size() {
        return _transactionMap.size();
    }

    public void applyAllCommittedTransactions() {
        for (final TRecord trecord : _transactionMap.values()) {
            try {
                if (trecord.isComplete()) {
                    applyTransaction(trecord);
                    _appliedCount++;
                }
            } catch (PersistitException pe) {
                _persistit.getLogBase().log(LogBase.LOG_TXN_RECOVERY_EXCEPTION,
                        pe, trecord);
            }
        }
    }

    public void applyTransaction(final TRecord trecord)
            throws PersistitException {
        final JournalManager jman = _persistit.getJournalManager();
        File file = trecord.getStartsAt().getFile();
        long bufferAddress = trecord.getStartsAt().getAddress();
        final Set<Tree> removedTrees = new HashSet<Tree>();
        boolean done = false;

        while (!done) {
            try {
                final FileChannel channel = new FileInputStream(file)
                        .getChannel();
                while (!done && bufferAddress < channel.size()) {
                    final long size = Math.min(channel.size() - bufferAddress,
                            _readBufferSize);

                    final MappedByteBuffer readBuffer = channel.map(
                            MapMode.READ_ONLY, bufferAddress, size);
                    while (!done && applyOneRecord(file, bufferAddress, readBuffer,
                            trecord, removedTrees)) {
                        done = trecord.isDone();
                    }
                    bufferAddress += readBuffer.position();
                }
                if (!done) {
                    final long generation = jman.fileToGeneration(file);
                    file = jman.generationToFile(generation + 1);
                }
            } catch (IOException ioe) {
                throw new PersistitIOException(ioe);
            }
        }
        
        for (final Tree tree : removedTrees) {
            tree.getVolume().removeTree(tree);
        }

    }

    private boolean applyOneRecord(final File file, final long bufferAddress,
            final MappedByteBuffer readBuffer, final TRecord trecord,
            final Set<Tree> removedTrees) throws PersistitException,
            JournalNotClosedException {

        final JournalManager jman = _persistit.getJournalManager();
        final int from = readBuffer.position();
        if (readBuffer.remaining() < JournalRecord.OVERHEAD) {
            return false;
        }

        readBuffer.get(_bytes, 0, JournalRecord.OVERHEAD);

        final int recordSize = JournalRecord.getLength(_bytes);

        if (recordSize + from > readBuffer.limit()) {
            readBuffer.position(from);
            return false;
        }

        final int type = JournalRecord.getType(_bytes);
        final long timestamp = JournalRecord.getTimestamp(_bytes);
        if (timestamp == trecord.getStartsAt().getTimestamp()) {
            switch (type) {

            case JournalRecord.TYPE_IV:
            case JournalRecord.TYPE_IT:
            case JournalRecord.TYPE_PA:
            case JournalRecord.TYPE_TJ:
            case JournalRecord.TYPE_CP:
                break;

            case JournalRecord.TYPE_TS:
                trecord.setState(State.STARTED);
                break;

            case JournalRecord.TYPE_TC:
                trecord.setState(State.COMMITTED);
                break;

            case JournalRecord.TYPE_TR:
                // TODO - should never happen because we never try to applied
                // a rolled-back transaction
                throw new IllegalStateException(
                        "TR record can't be processed in a "
                                + "committed transaction at "
                                + new FileAddress(file, bufferAddress + from,
                                        timestamp));

            case JournalRecord.TYPE_SR: {
                readBuffer.get(_bytes, JournalRecord.OVERHEAD,
                        JournalRecord.SR.OVERHEAD - JournalRecord.OVERHEAD);
                final Tree tree = jman.treeForHandle(JournalRecord.SR
                        .getTreeHandle(_bytes));
                final int keySize = JournalRecord.SR.getKeySize(_bytes);
                final Exchange exchange = _persistit.getExchange(tree
                        .getVolume(), tree.getName(), true);
                exchange.ignoreTransactions();
                final Key key = exchange.getKey();
                final Value value = exchange.getValue();
                readBuffer.get(key.getEncodedBytes(), 0, keySize);
                key.setEncodedSize(keySize);
                final int valueSize = recordSize - JournalRecord.SR.OVERHEAD
                        - keySize;
                readBuffer.get(value.getEncodedBytes(), 0, valueSize);
                value.setEncodedSize(valueSize);
                exchange.store();
                break;
            }

            case JournalRecord.TYPE_DR: {
                readBuffer.get(_bytes, JournalRecord.OVERHEAD,
                        JournalRecord.DR.OVERHEAD - JournalRecord.OVERHEAD);
                final Tree tree = jman.treeForHandle(JournalRecord.DR
                        .getTreeHandle(_bytes));
                final int key1Size = JournalRecord.DR.getKey1Size(_bytes);
                final Exchange exchange = _persistit.getExchange(tree
                        .getVolume(), tree.getName(), true);
                exchange.ignoreTransactions();
                final Key key1 = exchange.getAuxiliaryKey1();
                final Key key2 = exchange.getAuxiliaryKey2();

                readBuffer.get(key1.getEncodedBytes(), 0, key1Size);
                key1.setEncodedSize(key1Size);
                final int key2Size = recordSize - JournalRecord.DR.OVERHEAD
                        - key1Size;
                readBuffer.get(key2.getEncodedBytes(), 0, key2Size);
                key2.setEncodedSize(key2Size);
                exchange.removeKeyRangeInternal(key1, key2, false);
                break;
            }

            case JournalRecord.TYPE_DT:
                readBuffer.get(_bytes, JournalRecord.OVERHEAD,
                        JournalRecord.DT.OVERHEAD - JournalRecord.OVERHEAD);
                final Tree tree = jman.treeForHandle(JournalRecord.DT
                        .getTreeHandle(_bytes));
                if (tree != null) {
                    removedTrees.add(tree);
                }
                break;

            default:
                throw new JournalNotClosedException(new FileAddress(file,
                        bufferAddress + from, timestamp));
            }
        }
        readBuffer.position(from + recordSize);
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final Iterator<Map.Entry<Long, TRecord>> iterator = _transactionMap
                .entrySet().iterator(); iterator.hasNext();) {
            final Map.Entry<Long, TRecord> entry = iterator.next();
            sb.append(entry.getValue());
            sb.append(Util.NEW_LINE);
        }
        return sb.toString();
    }
}
