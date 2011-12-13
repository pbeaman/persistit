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

import java.lang.ref.WeakReference;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

/**
 * <p>
 * An Accumulator accumulates statistical information in the MVCC Transaction
 * environment without creating write-write dependency conflicts. Subclasses
 * include SumAccumulator, MinAccumulator, MaxAccumulator and SeqAccumulator
 * which compute the sum, minimum and maximum values of contributions by
 * individual transactions. (See below for semantics of the SeqAccummulator.)
 * Each contribution is accounted for separately as a {@link Delta} instance
 * until the transaction is either committed or aborted <i>and<i> there are no
 * other concurrently executing transactions that started before the commit
 * timestamp. This mechanism is designed to provide a "snapshot" view of the
 * Accumulator that is consistent with the snapshot view of the database.
 * </p>
 * <p>
 * In more detail: the {@link #update(long, Transaction)} method of an
 * Accumulator is invoked within the scope of a transaction T. That update is
 * not visible to other transactions until T commits. Moreover, any other
 * concurrently executing transaction having a start timestamp less than T's
 * commit timestamp does not see the results of the update. To accomplish this,
 * the state of the Accumulator visible within a transaction is computed by
 * determining which updates are visible and aggregating them on demand.
 * </p>
 * <p>
 * The updates invoked on Transactions are recorded in the Journal and reapplied
 * during recovery to reproduce an accurate version of the Accumulator state
 * when Persistit starts up.
 * <p>
 * There can be at most N Accumulators per Tree (where N is a reasonably small
 * number like 64). A snapshot value of each Accumulator is stored once per
 * checkpoint. Checkpoint snapshot values are held in the directory tree of the
 * volume containing the Tree.
 * </p>
 * <p>
 * The following describes example use cases for the various types of
 * accumulators:
 * <dl>
 * <dt>SUM</dt>
 * <dd>Row count, total size, sums of various other characteristics</dd>
 * <dt>MAX</dt>
 * <dd>Max value</dd>
 * <dt>MIN</dt>
 * <dd>Minimum value</dd>
 * <dt>SEQ</dt>
 * <dd>Sequence number generation, e.g., auto-increment or internal primary key
 * assignment
 * </dl>
 * </p>
 * <p>
 * The <code>SeqAccumulator</code> is a combination of
 * <code>SumAccumulator</code> and <code>MaxAccumulator</code>. When the
 * {@link #update(long, Transaction)} method is called, the supplied long value
 * is atomically added to the Accumulator's <code>live</code> value and the
 * result is returned. In addition, a {@link Delta} holding the resulting sum as
 * a proposed maximum value is added to the transaction.
 * </p>
 * 
 * @author peter
 */
public abstract class Accumulator {

    public static enum Type {
        SUM, MAX, MIN, SEQ
    };

    /**
     * A Comparator used to sort Accumulators when writing checkpoints
     */
    final static Comparator<Accumulator> SORT_COMPARATOR = new Comparator<Accumulator>() {

        @Override
        public int compare(Accumulator a, Accumulator b) {
            final String treeNameA = a.getTree() == null ? "" : a.getTree().getName();
            final String treeNameB = b.getTree() == null ? "" : b.getTree().getName();
            int compare = treeNameA.compareTo(treeNameB);
            if (compare != 0) {
                return compare;
            } else {
                return a.getIndex() - b.getIndex();
            }
        }
    };

    // Note: architectural limit of 255 for JournalRecord encoding
    final static int MAX_INDEX = 63;

    final static int MAX_SERIALIZED_SIZE = Tree.MAX_SERIALIZED_SIZE + 24;

    private final Tree _tree;
    private final int _index;
    private final TransactionIndex _transactionIndex;

    private final AtomicLong _liveValue = new AtomicLong();
    /*
     * Check-pointed value read during recovery.
     */
    private long _baseValue;

    /*
     * Snapshot value at the most recent checkpoint
     */
    private volatile long _checkpointValue;
    /*
     * Timestamp of the most recent checkpoint for which the checkpointValue has
     * been computed
     */
    private volatile long _checkpointTimestamp;
    /*
     * Temporary used only during the computation of checkpoint values.
     */
    private long _checkpointTemp;

    /*
     * Accumulated value per TransactionIndex bucket. This number represents the
     * accumulation of all delta values that have been coalesced and are no
     * longer present in live TransactionStatus objects. This array has one
     * element per TransactionIndexBucket.
     */
    private long[] _bucketValues;

    /*
     * Object held on the accumulators list in {@link Persistit}. An
     * AccumulatorRef is carefully crafted to keep a strong reference to the
     * Accumulator when needed a WeakReference used to detect that the there are
     * no other references to the Accumulator so that the AccumulatorRef can be
     * removed from the accumulators list.
     */
    final AccumulatorRef _accumulatorRef;

    /**
     * An Accumulator that computes a sum
     */
    final static class SumAccumulator extends Accumulator {

        private SumAccumulator(final Tree tree, final int index, final long baseValue,
                final TransactionIndex transactionIndex) {
            super(tree, index, baseValue, transactionIndex);
        }

        @Override
        long applyValue(final long a, final long b) {
            return a + b;
        }

        @Override
        long updateValue(final long a, final long b) {
            return applyValue(a, b);
        }

        @Override
        long selectValue(final long value, final long updated) {
            return value;
        }

        @Override
        Type getType() {
            return Type.SUM;
        }
    }

    /**
     * An Accumulator that computes a minimum value
     */
    final static class MinAccumulator extends Accumulator {

        private MinAccumulator(final Tree tree, final int index, final long baseValue,
                final TransactionIndex transactionIndex) {
            super(tree, index, baseValue, transactionIndex);
        }

        @Override
        long applyValue(final long a, final long b) {
            return Math.min(a, b);
        }

        @Override
        long updateValue(final long a, final long b) {
            return applyValue(a, b);
        }

        @Override
        long selectValue(final long value, final long updated) {
            return value;
        }

        @Override
        Type getType() {
            return Type.MIN;
        }
    }

    /**
     * An Accumulator that computes a maximum value
     */
    final static class MaxAccumulator extends Accumulator {

        private MaxAccumulator(final Tree tree, final int index, final long baseValue,
                final TransactionIndex transactionIndex) {
            super(tree, index, baseValue, transactionIndex);
        }

        @Override
        long applyValue(final long a, final long b) {
            return Math.max(a, b);
        }

        @Override
        long updateValue(final long a, final long b) {
            return applyValue(a, b);
        }

        @Override
        long selectValue(final long value, final long updated) {
            return value;
        }

        @Override
        Type getType() {
            return Type.MAX;
        }
    }

    /**
     * An Accumulator used to generate unique ID values. The
     * {@link #update(long, Transaction)} method applied to a
     * <code>SeqAccumulator</code> generates a new, unique long value. The
     * transaction records this value as a candidate for maximum value of the
     * Accumulator. On recovery, the highest such value ever allocated by a
     * committed transaction is recovered, and so after recovery the next
     * allocated ID value will be larger than any previously consumed.
     */
    final static class SeqAccumulator extends Accumulator {

        private SeqAccumulator(final Tree tree, final int index, final long baseValue,
                final TransactionIndex transactionIndex) {
            super(tree, index, baseValue, transactionIndex);
        }

        @Override
        long applyValue(final long a, final long b) {
            return Math.max(a, b);
        }

        @Override
        long updateValue(final long a, final long b) {
            if (b > 0) {
                return a + b;
            } else {
                throw new IllegalArgumentException("Update value must be positive");
            }
        }

        @Override
        long selectValue(final long value, final long updated) {
            return updated;
        }

        @Override
        Type getType() {
            return Type.SEQ;
        }
    }

    final static class Delta {
        Accumulator _accumulator;
        int _step;
        long _value;
        Delta _next;

        Accumulator getAccumulator() {
            return _accumulator;
        }

        int getStep() {
            return _step;
        }

        long getValue() {
            return _value;
        }

        void setAccumulator(final Accumulator accumulator) {
            _accumulator = accumulator;
        }

        void setValue(final long newValue) {
            _value = newValue;
        }

        void setStep(final int step) {
            _step = step;
        }

        Delta getNext() {
            return _next;
        }

        void setNext(final Delta delta) {
            _next = delta;
        }

        void merge(final Delta delta) {
            _value = _accumulator.applyValue(_value, delta.getValue());
        }

        @Override
        public String toString() {
            return String.format("Delta(type=%s value=%,d%s)", _accumulator == null ? "Null" : _accumulator.getType()
                    .toString(), _value, _next == null ? "" : "*");
        }
    }

    /**
     * <p>
     * Device that maintains a strong reference to the Accumulator when it
     * contains updates and needs to be checkpointed, and a weak reference
     * otherwise. The Persistit instance contains a collection of
     * <code>AccumulatorRef</code> instances; these are used when determining
     * which accumulators to include in the checkpoint operation. Once an
     * <code>Accumulator</code>'s checkpoint has been written, the strong
     * reference is removed until there is a subsequent update.
     * </p>
     * <p>
     * Scenario: a process creates a new Tree, creates an Accumulator and then
     * releases all references to the Tree. Eventually the Tree and the new
     * Accumulator should be garbage collected; however, the Accumulator must be
     * retained until any values it has accumulated have been written as part of
     * a checkpoint. The dual references in this class are intended to support
     * this behavior; the _checkpointRef field is null whenever there are no
     * changes to checkpoint; the _weakRef is used to detect when it is
     * permissible to remove the <code>AccumulatorRef</code> from the Persistit
     * instance's accumulator set.
     * <p>
     */
    final static class AccumulatorRef {
        final WeakReference<Accumulator> _weakRef;
        volatile Accumulator _checkpointRef;

        AccumulatorRef(final Accumulator acc) {
            _weakRef = new WeakReference<Accumulator>(acc);
            _checkpointRef = acc;
        }

        Accumulator takeCheckpointRef() {
            Accumulator result = _checkpointRef;
            _checkpointRef = null;
            return result;
        }

        void checkpointNeeded(final Accumulator acc) {
            if (_checkpointRef == null) {
                _checkpointRef = acc;
            }
        }

        boolean isLive() {
            return _weakRef.get() != null || _checkpointRef != null;
        }
    }

    private Accumulator(final Tree tree, final int index, final long baseValue, final TransactionIndex transactionIndex) {
        if (index < 0 || index > MAX_INDEX) {
            throw new IllegalArgumentException("Index out of bounds: " + index);
        }
        _tree = tree;
        _index = index;
        _baseValue = baseValue;
        _checkpointValue = baseValue;
        _liveValue.set(baseValue);
        _transactionIndex = transactionIndex;
        _bucketValues = new long[transactionIndex.getHashTableSize()];
        _accumulatorRef = new AccumulatorRef(this);
    }

    /**
     * Apply the value from a <code>Delta</code> to an aggregate. This method
     * must be commutative, that is, apply(x, y) must be equal to apply(y, x).
     * This method is called when computing a snapshot value and when
     * aggregating <code>Delta</code> instances.
     * 
     * @param a
     * @param b
     * @return
     */
    abstract long applyValue(long a, long b);

    /**
     * Compute a live updated value. For <code>SumAccumulator</code>,
     * <code>MaxAccumlator</code> and <code>MinAccumulator</code> this method
     * returns the same value as {@link #applyValue(long, long)}. For
     * <code>SeqAccumulator</code> update(a, b) returns a + b (computed
     * atomically) whereas apply(a, b) return Math.max(a, b).
     * 
     * @param a
     * @param b
     * @return
     */
    abstract long updateValue(long a, long b);

    /**
     * @param value
     * @param updated
     *            return One of the supplied parameters as the value to be held
     *            in a <code>Delta</code>.
     * 
     */
    abstract long selectValue(long value, long updated);

    abstract Type getType();

    void aggregate(final int hashIndex, final Delta delta) {
        _bucketValues[hashIndex] = applyValue(_bucketValues[hashIndex], delta.getValue());
    }

    AccumulatorRef getAccumulatorRef() {
        return _accumulatorRef;
    }

    void checkpointNeeded() {
        _accumulatorRef.checkpointNeeded(this);
    }

    long getBucketValue(final int hashIndex) {
        return _bucketValues[hashIndex];
    }

    void setCheckpointValueAndTimestamp(final long value, final long timestamp) {
        _checkpointValue = value;
        _checkpointTimestamp = timestamp;
    }

    long getCheckpointValue() {
        return _checkpointValue;
    }

    long getCheckpointTimestamp() {
        return _checkpointTimestamp;
    }

    void setCheckpointTemp(final long value) {
        _checkpointTemp = value;
    }

    long getCheckpointTemp() {
        return _checkpointTemp;
    }

    /**
     * @param type
     *            Indicates which kind of <code>Accumulator</code> to return
     * @param tree
     *            The {@link Tree} to which this Accumulator will belong
     * @param index
     *            An index number by which this Accumulator can be accessed.
     * @param baseValue
     *            a value that accurately reflects the contributions of all
     *            transactions that committed before the baseTimestamp
     * @param transactionIndex
     *            the <code>TransactionIndex</code> component
     * @return an Accumulator of the specified type
     * 
     */
    static Accumulator accumulator(final Type type, final Tree tree, final int index, final long baseValue,
            final TransactionIndex transactionIndex) {
        switch (type) {
        case SUM:
            return new SumAccumulator(tree, index, baseValue, transactionIndex);
        case MAX:
            return new MaxAccumulator(tree, index, baseValue, transactionIndex);
        case MIN:
            return new MinAccumulator(tree, index, baseValue, transactionIndex);
        case SEQ:
            return new SeqAccumulator(tree, index, baseValue, transactionIndex);
        default:
            throw new IllegalArgumentException("No such type " + type);
        }
    }

    long getBaseValue() {
        return _baseValue;
    }

    long getLiveValue() {
        return _liveValue.get();
    }

    /**
     * @param txn
     *            The transaction reading the snapshot
     * @return The value computed by accumulating values contributed by (a) all
     *         transactions having commit timestamps less than or equal to
     *         <code>transaction</code>'s start timestamp, and (b) all
     *         operations performed by the current transaction having step
     *         numbers equal to zero or less than the <code>transaction</code>'s
     *         current step.
     * @throws InterruptedException
     */
    public long getSnapshotValue(final Transaction txn) throws PersistitInterruptedException {
        if (!txn.isActive()) {
            throw new IllegalStateException("Transaction has not been started");
        }
        return getSnapshotValue(txn.getStartTimestamp(), txn.getCurrentStep());
    }

    /**
     * @param timestamp
     * @param step
     * @return The value computed by accumulating values contributed by (a) all
     *         transactions having commit timestamps less than or equal to
     *         <code>timestamp</code>, and (b) all operations performed by the
     *         current transaction having step numbers less than
     *         <code>step</code>.
     * @throws InterruptedException
     */
    long getSnapshotValue(final long timestamp, final int step) throws PersistitInterruptedException {
        try {
            return _transactionIndex.getAccumulatorSnapshot(this, timestamp, step, _baseValue);
        } catch (InterruptedException ie) {
            throw new PersistitInterruptedException(ie);
        }
    }

    /**
     * Apply an update to the base value. This method is used only during
     * recovery processing to apply Deltas from recovered committed
     * transactions.
     * 
     * @param value
     */
    void updateBaseValue(final long value) {
        _baseValue = applyValue(_baseValue, value);
    }

    /**
     * Update the Accumulator by contributing a value. The contribution is
     * immediately accumulated into the live value, and it is also posted with a
     * 
     * @{link {@link Delta} instance to the supplied {@link Transaction}.
     * 
     * @param value
     *            The delta value
     * @param txn
     *            The transaction it applies to
     */
    public long update(final long value, final Transaction txn) {
        return update(value, txn.getTransactionStatus(), txn.getCurrentStep());
    }

    /**
     * Update the Accumulator by contributing a value. The contribution is
     * immediately accumulated into the live value, and it is also posted with a
     * 
     * @{link {@link Delta} instance to the supplied {@link Transaction}. This
     *        package-private method is provided primarily for unit tests.
     * 
     * @param value
     *            The delta value
     * @param status
     *            The TransactionStatus of the transaction it applies to
     * @param step
     *            The step at which the value is applied
     */
    long update(final long value, final TransactionStatus status, final int step) {
        if (status.getTc() != TransactionStatus.UNCOMMITTED) {
            throw new IllegalStateException("Transaction has already committed or aborted");
        }
        /*
         * Update the live value using compare-and-set
         */
        long previous;
        long updated;
        for (;;) {
            previous = _liveValue.get();
            updated = updateValue(previous, value);
            if (_liveValue.compareAndSet(previous, updated)) {
                break;
            }
        }
        /*
         * Add a Delta to the TransactionStatus
         */
        Delta delta = _transactionIndex.addDelta(status);
        delta.setValue(selectValue(value, updated));
        delta.setStep(step);
        delta.setAccumulator(this);
        return updated;
    }

    Tree getTree() {
        return _tree;
    }

    int getIndex() {
        return _index;
    }

    @Override
    public String toString() {
        return String.format("Accumulator(tree=%s index=%d type=%s base=%,d live=%,d)", _tree == null ? "null" : _tree
                .getName(), _index, getType(), _baseValue, _liveValue.get());
    }

    void store(final Value value) {
        value.put(_tree == null ? "" : _tree.getName());
        value.put(_index);
        value.put(getType().toString());
        value.put(getCheckpointValue());
    }

    static AccumulatorState getAccumulatorState(final Tree tree, final int index) throws PersistitException {
        final Exchange exchange = tree.getVolume().getStructure().directoryExchange();
        exchange.clear().append(VolumeStructure.DIRECTORY_TREE_NAME).append(VolumeStructure.TREE_ACCUMULATOR).append(
                tree.getName()).append(index).fetch();
        if (exchange.getValue().isDefined()) {
            return (AccumulatorState) exchange.getValue().get();
        } else {
            return null;
        }
    }

    static void saveAccumulatorCheckpointValues(final List<Accumulator> list) throws PersistitException {
        Exchange exchange = null;
        for (final Accumulator accumulator : list) {

            final Volume volume = accumulator.getTree().getVolume();
            if (exchange == null || !exchange.getVolume().equals(volume)) {
                exchange = volume.getStructure().accumulatorExchange();
            }
            exchange.clear().append(VolumeStructure.DIRECTORY_TREE_NAME).append(VolumeStructure.TREE_ACCUMULATOR)
                    .append(accumulator.getTree().getName()).append(accumulator.getIndex());
            exchange.getValue().put(accumulator);
            exchange.store();
        }
    }
}
