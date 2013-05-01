/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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
 * include <code>SumAccumulator</code>, <code>MinAccumulator</code>,
 * <code>MaxAccumulator</code> and <code>SeqAccumulator</code> which compute the
 * sum, minimum and maximum values of contributions by individual transactions.
 * (See <a href="#_SeqAccumulator">below</a> for semantics of the
 * <code>SeqAccummulator</code>.) Each contribution is accounted for separately
 * as a <code>Delta</code> instance until the transaction is either committed or
 * aborted and there are no other concurrently executing transactions that
 * started before the commit timestamp. This mechanism is designed to provide a
 * "snapshot" view of the Accumulator that is consistent with the snapshot view
 * of the database.
 * </p>
 * <p>
 * In more detail: each type of Accumulator has an update method that may only
 * be invoked within the scope of a transaction T. That update is not visible to
 * any other transaction until T commits. Moreover, any other concurrently
 * executing transaction having a start timestamp less than T's commit timestamp
 * does not see the results of the update. To accomplish this, the state of the
 * Accumulator visible within a transaction is computed by determining which
 * updates are visible and aggregating them on demand.
 * </p>
 * <p>
 * The updates invoked on committed Transactions are recorded in the Journal and
 * reapplied during recovery to reproduce an accurate version of the Accumulator
 * state when Persistit starts up. There can be at most 64 Accumulators per
 * Tree. A snapshot value of each Accumulator is stored once per checkpoint.
 * Checkpoint snapshot values are held in the directory tree of the volume
 * containing the Tree.
 * </p>
 * <p>
 * <h3>Types of Accumulators</h3>
 * The following defines intended use cases for the various types of
 * accumulators:
 * <dl>
 * <dt>{@link com.persistit.Accumulator.Type#SUM}</dt>
 * <dd>Row count, total size, sums of various other characteristics</dd>
 * <dt>{@link com.persistit.Accumulator.Type#MAX}</dt>
 * <dd>Maximum value</dd>
 * <dt>{@link com.persistit.Accumulator.Type#MIN}</dt>
 * <dd>Minimum value</dd>
 * <dt>{@link com.persistit.Accumulator.Type#SEQ}</dt>
 * <dd>Sequence number generation, e.g., auto-increment or internal primary key
 * assignment</dd>
 * </dl>
 * </p>
 * <p>
 * <a name="_SnapshotValue" />
 * <h3>Snapshot and Live Values</h3>
 * Each Accumulator type supplies both a "snapshot" value and a "live" value.
 * The snapshot value is computed as described above by selectively applying
 * only those updates visible to the transaction. The live value, however, is
 * simply the result of applying each update operation atomically to a long
 * value maintained by the Accumulator. For example, if ten transactions
 * increment a SUM accumulator by one, and then five commit of them and five and
 * roll back, the live value is nonetheless increased by ten. Thus the live
 * value is only an estimate. Its value is cheap to acquire but not
 * transactionally accurate.
 * </p>
 * <p>
 * <a name="_SeqAccumulator" />
 * <h3>SeqAccumulator</h3>
 * The <code>SeqAccumulator</code> is a combination of
 * <code>SumAccumulator</code> and <code>MaxAccumulator</code>. When the
 * {@link com.persistit.Accumulator.SeqAccumulator#allocate()} method is called,
 * the Accumulator's <code>live</code> value is atomically incremented and the
 * resulting value is returned. In addition, a <code>Delta</code> holding the
 * resulting sum as a proposed minimum value is added to the transaction. These
 * semantics guarantee that every value returned by a SeqAccumulator (within a
 * transaction that actually commits) is unique, and that upon recovery after a
 * normal shutdown or crash, the first value returned will be larger than the
 * maximum value assigned by any transaction that committed successfully before
 * the shutdown. Note that a transaction that allocates a value and then aborts
 * leaves a gap in the numerical sequence.
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
        public int compare(final Accumulator a, final Accumulator b) {
            final String treeNameA = a.getTree() == null ? "" : a.getTree().getName();
            final String treeNameB = b.getTree() == null ? "" : b.getTree().getName();
            final int compare = treeNameA.compareTo(treeNameB);
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

    protected final Tree _tree;
    private final int _index;
    private final TransactionIndex _transactionIndex;

    private final AtomicLong _liveValue = new AtomicLong();
    /*
     * Check-pointed value read during recovery.
     */
    private volatile long _baseValue;

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
    private final long[] _bucketValues;

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
    public final static class SumAccumulator extends Accumulator {

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
            if (b > 0 && a + b < a || b < 0 && a + b > a) {
                throw new IllegalArgumentException("Accumulator value overflow: (" + a + "+" + b + ")");
            }
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

        /**
         * <p>
         * Add the supplied value to this <code>SumAccumulator</code>. The
         * contribution is immediately accumulated into the live value, and it
         * is also posted with a <code>Delta</code>instance to the supplied
         * {@link Transaction}. This method may be called only within the scope
         * of an active <code>Transaction</code>.
         * </p>
         * 
         * @param value
         *            The delta value
         */
        public void add(final long value) {
            final Transaction txn = _tree.getPersistit().getTransaction();
            txn.checkActive();
            update(value, txn.getTransactionStatus(), txn.getStep());
        }

    }

    /**
     * An Accumulator that computes a minimum value
     */
    public final static class MinAccumulator extends Accumulator {

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

        /**
         * <p>
         * Modify the Accumulator so that its value is no less than the supplied
         * value. The contribution is immediately accumulated into the live
         * value, and it is also posted with a <code>Delta</code> instance to
         * the supplied {@link Transaction}. This method may be called only
         * within the scope of an active <code>Transaction</code>.
         * </p>
         * 
         * @param min
         *            The candidate minimum value
         */
        public void minimum(final long min) {
            final Transaction txn = _tree.getPersistit().getTransaction();
            txn.checkActive();
            update(min, txn.getTransactionStatus(), txn.getStep());
        }

    }

    /**
     * An Accumulator that computes a maximum value
     */
    public final static class MaxAccumulator extends Accumulator {

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

        /**
         * <p>
         * Modify the Accumulator so that its value is no greater than the
         * supplied value. The contribution is immediately accumulated into the
         * live value, and it is also posted with a <code>Delta</code> instance
         * to the supplied {@link Transaction}. This method may be called only
         * within the scope of an active <code>Transaction</code>.
         * </p>
         * 
         * @param max
         *            The candidate maximum value
         */
        public void maximum(final long max) {
            final Transaction txn = _tree.getPersistit().getTransaction();
            txn.checkActive();
            update(max, txn.getTransactionStatus(), txn.getStep());
        }

    }

    /**
     * An Accumulator used to generate unique ID values. The
     * {@link com.persistit.Accumulator.SeqAccumulator#allocate()} method
     * generates a new, unique long value. The transaction records this value as
     * a candidate for maximum value of the Accumulator. On recovery, the
     * highest such value ever allocated by a committed transaction is
     * recovered, and so after recovery the next allocated ID value will be
     * larger than any previously consumed.
     */
    public final static class SeqAccumulator extends Accumulator {

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
            if (b <= 0) {
                throw new IllegalArgumentException("Update value must be positive");
            }
            if (a + b < a) {
                throw new IllegalArgumentException("Accumulator value overflow: (" + a + "+" + b + ")");
            }
            return a + b;
        }

        @Override
        long selectValue(final long value, final long updated) {
            return updated;
        }

        @Override
        Type getType() {
            return Type.SEQ;
        }

        /**
         * <p>
         * Allocate a sequence number. The value returned is guaranteed to be
         * unique for the lifetime of the database. Values are usually assigned
         * as consecutive integers, but in some cases there may be gaps in the
         * sequence.
         * </p>
         * <p>
         * The value returned is equal to the <a href="#_SnapshotValue">live
         * value</a> the instant it is updated. However, note that the following
         * code is <em>not</em> guaranteed to generate a unique value:
         * <code><pre>
         *    seqAccumulator.allocate();
         *    long id = seqAccumulator.getLiveValue();
         * </pre></code>while the following is: <code><pre>
         *   long id = seqAccumulator.allocate();
         * </p>
         * 
         * @return the updated live value
         */
        public long allocate() {
            final Transaction txn = _tree.getPersistit().getTransaction();
            return update(1, txn.getTransactionStatus(), txn.getStep());
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

        void merge(final long value) {
            _value = _accumulator.applyValue(_value, value);
        }

        boolean canMerge(final Accumulator accumulator, final int step) {
            return (_accumulator == accumulator) && (_step == step);
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
        final AtomicLong _latestUpdate = new AtomicLong();
        volatile Accumulator _checkpointRef;

        AccumulatorRef(final Accumulator acc) {
            _weakRef = new WeakReference<Accumulator>(acc);
        }

        Accumulator takeCheckpointRef(final long timestamp) {
            final Accumulator result = _checkpointRef;

            if (timestamp > _latestUpdate.get()) {
                _checkpointRef = null;
                if (timestamp <= _latestUpdate.get()) {
                    _checkpointRef = result;
                }
            }

            return result;
        }

        void checkpointNeeded(final Accumulator acc, final long timestamp) {
            while (true) {
                final long latest = _latestUpdate.get();
                if (latest > timestamp) {
                    return;
                }
                if (_latestUpdate.compareAndSet(latest, timestamp)) {
                    break;
                }
            }
            _checkpointRef = acc;
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
     * @return the result of the commutative operation on a and b
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
     * @return the result of the commutative operation on a and b
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

    void checkpointNeeded(final long timestamp) {
        _accumulatorRef.checkpointNeeded(this, timestamp);
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

    /**
     * Non-transactional view aggregating all updates applied to this
     * Accumulator, whether committed or not. See <a
     * href="#_SnapshotValue">Snapshot and Live Values</a>.
     * 
     * @return the live value
     */
    public long getLiveValue() {
        return _liveValue.get();
    }

    /**
     * Compute the value computed by accumulating values contributed by (a) all
     * transactions having commit timestamps less than or equal to the specified
     * <code>transaction</code>'s start timestamp, and (b) all operations
     * performed by the specified transaction having step numbers equal to or
     * less than the <code>transaction</code>'s current step. See <a
     * href="#_SnapshotValue">Snapshot and Live Values</a>.
     * 
     * @return the computed snapshot value
     * @throws InterruptedException
     */
    public long getSnapshotValue() throws PersistitInterruptedException {
        final Transaction txn = _tree.getPersistit().getTransaction();
        txn.checkActive();
        return getSnapshotValue(txn.getStartTimestamp(), txn.getStep());
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
        } catch (final InterruptedException ie) {
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
    void updateBaseValue(final long value, final long commitTimestamp) {
        _baseValue = applyValue(_baseValue, value);
        _liveValue.set(_baseValue);
        /*
         * This method is called during recovery processing to handle a delta
         * operation that was part of a transaction that committed after the
         * keystone checkpoint. That update requires the accumulator to be saved
         * on the next checkpoint.
         */
        checkpointNeeded(commitTimestamp);
    }

    /**
     * Update the Accumulator by contributing a value. The contribution is
     * immediately accumulated into the live value, and it is also posted with a
     * {@link Delta} instance to the supplied {@link Transaction}.
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
        final long selectedValue = selectValue(value, updated);
        _transactionIndex.addOrCombineDelta(status, this, step, selectedValue);
        return updated;
    }

    Tree getTree() {
        return _tree;
    }

    int getIndex() {
        return _index;
    }

    @Override
    /**
     * @return a formatted report showing the Tree, index, type, and accumulated
     *         values for this <code>Accumulator</code>.
     */
    public String toString() {
        return String.format("Accumulator(tree=%s index=%d type=%s base=%,d live=%,d)",
                _tree == null ? "null" : _tree.getName(), _index, getType(), _baseValue, _liveValue.get());
    }

    void store(final Value value) {
        value.put(_tree == null ? "" : _tree.getName());
        value.put(_index);
        value.put(getType().toString());
        value.put(getCheckpointValue());
    }

    static AccumulatorState getAccumulatorState(final Tree tree, final int index) throws PersistitException {
        final Exchange exchange = tree.getVolume().getStructure().directoryExchange();
        exchange.clear().append(VolumeStructure.DIRECTORY_TREE_NAME).append(VolumeStructure.TREE_ACCUMULATOR)
                .append(tree.getName()).append(index).fetch();
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
