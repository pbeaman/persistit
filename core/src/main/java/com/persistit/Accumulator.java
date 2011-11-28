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

import java.util.concurrent.atomic.AtomicLong;

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
 * In more detail: the {@link #update(long, TransactionStatus, int)} method of
 * an Accumulator is invoked within the scope of a transaction T. That update is
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
 * {@link #update(long, TransactionStatus, int)} method is called, the supplied
 * long value is atomically added to the Accumulator's <code>live</code> value
 * and the result is returned. In addition, a {@link Delta} holding the
 * resulting sum as a proposed maximum value is added to the transaction.
 * </p>
 * 
 * @author peter
 */
abstract class Accumulator {

    public static enum Type {
        SUM, MAX, MIN, SEQ
    };

    private final Tree _tree;
    private final int _index;
    private final TransactionIndex _transactionIndex;

    private final AtomicLong _liveValue = new AtomicLong();
    private final long _baseValue;

    private long[] _bucketValues;

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
        Type type() {
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
        Type type() {
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
        Type type() {
            return Type.MAX;
        }
    }

    /**
     * An Accumulator used to generate unique ID values. The
     * {@link #update(long, TransactionStatus, int)} method applied to a
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
        Type type() {
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
            return String.format("Delta(type=%s value=%,d%s)", _accumulator == null ? "Null" : _accumulator.type()
                    .toString(), _value, _next == null ? "" : "*");
        }
    }

    private Accumulator(final Tree tree, final int index, final long baseValue, final TransactionIndex transactionIndex) {
        _tree = tree;
        _index = index;
        _baseValue = baseValue;
        _liveValue.set(baseValue);
        _transactionIndex = transactionIndex;
        _bucketValues = new long[transactionIndex.getHashTableSize()];
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
     * return One of the supplied parameters as the value to be held in a <code>Delta</code>.
     * 
     */
    abstract long selectValue(long value, long updated);

    abstract Type type();

    void aggregate(final int hashIndex, final Delta delta) {
        _bucketValues[hashIndex] = applyValue(_bucketValues[hashIndex], delta.getValue());
    }

    long getBucketValue(final int hashIndex) {
        return _bucketValues[hashIndex];
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

    long getLiveValue() {
        return _liveValue.get();
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
    long getSnapshotValue(final long timestamp, final int step) throws InterruptedException {
        return _transactionIndex.getAccumulatorSnapshot(this, timestamp, step, _baseValue);
    }

    /**
     * Update the Accumulator by contributing a value. The contribution is
     * immediately accumulated into the live value, and it is also posted with a
     * 
     * @{link {@link Delta} instance to the supplied {@link TransactionStatus}.
     * 
     * @param value
     * @param ts
     * @param step
     */
    void update(final long value, final TransactionStatus status, final int step) {
        assert status.getTc() == TransactionStatus.UNCOMMITTED;
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
    }

    Tree getTree() {
        return _tree;
    }

    int getIndex() {
        return _index;
    }

    @Override
    public String toString() {
        return String.format("Accumulator(tree=%s index=%d type=%s base=%,d live=%,d", _tree.getName(), _index, type(),
                _baseValue, _liveValue.get());
    }
}
