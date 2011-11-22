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
 * include SumAccumulator, MinAccumulator and MaxAccumulator which compute the
 * sum, minimum and maximum values of contributions by individual transactions.
 * Each contribution is accounted for separately until the transaction is either
 * committed or aborted <i>and<i> there are no other concurrently executing
 * transactions that started before the commit timestamp. This mechanism is
 * designed to provide a "snapshot" view of the Accumulator that is consistent
 * with the snapshot view of the database.
 * </p>
 * <p>
 * In more detail: the mutator methods of an Accumulator are invoked within the
 * scope of a transaction T. The mutation operations (add, min, max) are all
 * commutative. Those mutations are not visible to other transactions until T
 * commits. Moreover, another concurrently executing transaction having a start
 * timestamp less than T's commit timestamp does not see the results of the
 * mutations either. To accomplish this, the state of the Accumulator visible
 * within a transaction is computed by determining which mutations are visible
 * and applying them to a transient copy.
 * </p>
 * <p>
 * There can be at most N Accumulators per tree (where N is a reasonably small
 * number like 64). Their state is serialized at checkpoints into the Tree's
 * directory entry.
 * </p>
 * <p>
 * The following describes intended use cases for the various types of
 * accumulators:
 * <dl>
 * <dt>sum</dt>
 * <dd>Row count, total size, sums of various other characteristics</dd>
 * <dt>max</dt>
 * <dd>Auto-increment and generated primary key assignment for positive
 * increment values</dd>
 * <dt>min</dt>
 * <dd>Auto-increment and generated primary key assignment for negative
 * increment values</dd>
 * </dl>
 * </p>
 * <p>
 * To allocate a new unique key value, for instance, the application is expected
 * to atomically increment a counter maintained outside the scope of this class,
 * and then post an update to a {@link MaxAccumulator}. Upon recovery the
 * maximum value proposed by any committed transaction is restored and should be
 * used to the startup value of the counter. This guarantees that the next
 * allocated ID is larger than any key inserted as part of a committed
 * transaction.
 * </p>
 * 
 * @author peter
 * 
 */
abstract class Accumulator {

    public static enum Type {
        SUM, MAX, MIN
    };

    private final Tree _tree;
    private final int _index;
    private final TransactionIndex _transactionIndex;

    private AtomicLong _liveValue;
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
        long combine(final long a, final long b) {
            return a + b;
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
        long combine(final long a, final long b) {
            return Math.min(a, b);
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
        long combine(final long a, final long b) {
            return Math.max(a, b);
        }

        @Override
        Type type() {
            return Type.MAX;
        }
    }

    private Accumulator(final Tree tree, final int index, final long baseValue, final TransactionIndex transactionIndex) {
        _tree = tree;
        _index = index;
        _baseValue = baseValue;
        _transactionIndex = transactionIndex;
        _bucketValues = new long[transactionIndex.getHashTableSize()];
    }

    abstract long combine(long a, long b);

    abstract Type type();

    void aggregate(final int hashIndex, final Delta delta) {
        _bucketValues[hashIndex] = combine(_bucketValues[hashIndex], delta.getValue());
    }

    long getBucketValue(final int hashIndex) {
        return _bucketValues[hashIndex];
    }

    /**
     * @param type
     * @param tree
     *            The {@link Tree} to which this Accumulator will belong
     * @param index
     *            An index number by which this Accumulator can be accessed.
     * @param baseValue
     *            a value that accurately reflects the contributions of all
     *            transactions that committed before the baseTimestamp
     * @param transactionIndex
     *            the <code>TransactionIndex</code> component
     * @return a SumAccumulator
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
        default:
            throw new IllegalArgumentException("No such type " + type);
        }
    }

    /**
     * @param timestamp
     * @param step
     * @return The value computed by accumulating values contributed by (a) all
     *         transactions having commit timestamps less than or equal to
     *         <code>timestamp</code>, and (b) all operations performed by the
     *         current transaction having step numbers less than
     *         <code>step</code>.
     */
    long getSnapshotValue(final long timestamp, final int step) {
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
        for (;;) {
            final long previous = _liveValue.get();
            final long updated = combine(previous, value);
            if (_liveValue.compareAndSet(previous, updated)) {
                break;
            }
        }
        /*
         * Add a Delta to the TransactionStatus
         */
        Delta delta = _transactionIndex.addDelta(status);
        delta.setValue(value);
        delta.setStep(step);
    }

    Tree getTree() {
        return _tree;
    }

    int getIndex() {
        return _index;
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
            _value = _accumulator.combine(_value, delta.getValue());
        }
    }

    @Override
    public String toString() {
        return String.format("Accumulator(tree=%s index=%d type=%s base=%,d live=%,d", _tree.getName(), _index, type(),
                _baseValue, _liveValue);
    }
}
