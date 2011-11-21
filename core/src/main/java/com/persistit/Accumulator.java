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

import java.util.ArrayList;

import com.persistit.exception.PersistitException;

/**
 * <p>
 * An Accumulator accumulates statistical information in the MVCC Transaction
 * environment without creating write-write dependency conflicts. An accumulator
 * can maintain an arbitrary number of numeric (long) fields. Each field is used
 * to keep a sum, a minimum or a maximum value. The contribution of each
 * concurrent transaction is accounted for separately until the transaction is
 * either committed or aborted <i>and<i> there are no other concurrently
 * executing transactions started before the commit timestamp. This mechanism is
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
 * There can be at most one Accumulator per tree. Its state is serialized at
 * checkpoints into the Tree's directory entry.
 * </p>
 * 
 * @author peter
 * 
 */
public final class Accumulator {
    private final static int INITIAL_FIELD_COUNT = 2;
    private final Tree _tree;

    public enum Operation {
        ADD, MIN, MAX, ALLOC
    }

    private Operation[] _ops = new Operation[INITIAL_FIELD_COUNT];

    private long[] _liveFields = new long[INITIAL_FIELD_COUNT];

    class Delta {

        private final long _versionHandle;
        
        private long[] _fields;
        
        private ArrayList<Delta> _deltas;

        Delta(final long versionHandle) {
            _versionHandle = versionHandle;
        }

        Delta(final Delta other) {
            _versionHandle = other._versionHandle;
            _fields = new long[other._fields.length];
            System.arraycopy(other._fields, 0, _fields, 0, _fields.length);
        }

        void add(int index, long delta) {
            if (_fields == null) {
                _fields = new long[_liveFields.length];
            } else if (_fields.length <= _liveFields.length) {
                long[] fields = new long[_liveFields.length];
                System.arraycopy(_fields, 0, fields, 0, _fields.length);
                _fields = fields;
            }
            _fields[index] = delta;
        }

        void combine(final Delta other) {
            for (int index = 0; index < _fields.length; index++) {
                if (_ops[index] != null) {
                    switch (_ops[index]) {
                    case ADD:
                        _fields[index] += other._fields[index];
                        break;

                    case MIN:
                        _fields[index] = Math.min(_fields[index], other._fields[index]);
                        break;

                    case MAX:
                        _fields[index] = Math.max(_fields[index], other._fields[index]);
                        break;

                    }
                }
            }
        }
    }

    Accumulator(final Tree tree) {
        _tree = tree;
    }

    public synchronized void defineField(final Operation op, final int index) {
        if (index >= _ops.length) {
            Operation[] ops = new Operation[index + 1];
            long[] fields = new long[index + 1];
            System.arraycopy(_ops, 0, ops, 0, _ops.length);
            System.arraycopy(_liveFields, 0, fields, 0, _liveFields.length);
            _ops = ops;
            _liveFields = fields;
        }
        if (_ops[index] != null && _ops[index] != op) {
            throw new IllegalStateException("Operation for field " + index + " is already defined: " + _ops[index]);
        }
        _ops[index] = op;
    }

    /**
     * Transactionally add the supplied delta value to a field, behaving as if
     * executing the code
     * 
     * <pre>
     * <code>
     * field[index] += delta;
     * </code>
     * </pre>
     * 
     * except that effects of this mutation are visible within other concurrent
     * transactions only as described above. The field identified by the
     * <code>index</code> must have previously been defined an
     * {@link Operation#ADD} field.
     * 
     * @param index
     *            within field array
     * @param delta
     *            quantity to add to the field.
     */
    public synchronized void add(final TransactionStatus status, final int index, final long delta) {
        if (_ops[index] != Operation.ADD) {
            throw new IllegalStateException("Field " + index + " is not defined as an ADD field");
        }
    }

    /**
     * Transactionally modify the value of the specified field to be the maximum
     * of the supplied and previous values, behaving as if executing the code
     * 
     * <pre>
     * <code>
     * field[index] = Math.max(field[item], value);
     * </code>
     * </pre>
     * 
     * except that effects of this mutation are visible within other concurrent
     * transactions only as described above. The field identified by the
     * <code>index</code> must have previously been defined as an
     * {@link Operation#MAX} field.
     * 
     * @param index
     *            within field array
     * @param value
     *            candidate to replace the maximum.
     */
    public synchronized void setMax(final TransactionStatus status, final int index, final long value) {

    }

    /**
     * Transactionally modify the value of the specified field to be the minimum
     * of the supplied and previous values, behaving as if executing the code
     * 
     * <pre>
     * <code>
     * field[index] = Math.min(field[item], value);
     * </code>
     * </pre>
     * 
     * except that effects of this mutation are visible within other concurrent
     * transactions only as described above. The field identified by the
     * <code>index</code> must have previously been defined as an
     * {@link Operation#MIN} field.
     * 
     * @param index
     *            within field array
     * @param value
     *            candidate to replace the minimum.
     */
    public synchronized void setMin(final TransactionStatus status, final int index, final long value) {

    }

    /**
     * <p>
     * Computes a proposed unique value with the following steps. Reads the
     * "live" value of the specified field (that is, the value that would exist
     * were all concurrently executing transactions committed), adds
     * <code>delta</code> and then performs the {@link #max} (if delta is
     * positive) or {@link Accumulator#min} (if delta is negative) operation on
     * the result. The net effect is that a unique value will be generated, and
     * the largest (if delta is positive) or smallest (if delta is negative)
     * such value will be reliably stored for recovery. This method is intended
     * to support auto-increment and implicit primary key generation.
     * </p>
     * <p>
     * The field identified by the <code>index</code> must have previously been
     * defined as a {@link Operation#MAX} or {@link Operation#MIN} field depending on
     * whether <code>delta</code> is positive or negative.
     * </p>
     * 
     * @param index
     * @param delta
     * @return
     */
    public long allocate(final TransactionStatus status, final int index, final long delta) {
        return -1;
    }

    /**
     * Return the value associated with the field at the supplied
     * <code>index</code>
     * 
     * @param index
     * @param timestamp
     */
    public long getSnapshotValue(final int index, final long timestamp) {
        return -1;
    }

    public long getLiveValue(final int index) {
        return -1;
    }

    /**
     * Load the current state of this Accumulator from the <code>Tree</code>
     * supplied in the constructor. This method should be called only during the
     * recovery process.
     * 
     * @throws PersistitException
     */
    synchronized void load() throws PersistitException {
        Exchange ex = _tree.getVolume().getStructure().directoryExchange();
        ex.clear().append(VolumeStructure.DIRECTORY_TREE_NAME).append(VolumeStructure.TREE_ACCUMULATOR);
        if (ex.getValue().isDefined()) {
            long[] values = ex.getValue().getLongArray();
            if (values.length != _liveFields.length) {
                throw new IllegalStateException("Stored Accumulator value has incorrect field count: " + values.length
                        + " should be: " + _liveFields.length);
            }
        }
    }

    /**
     * Save a snapshot of the state of this Accumulator to the
     * <code>Exchange</code> supplied in the constructor. The snapshot is
     * determined by the supplied timestamp. This method should be called only
     * by checkpoint manager.
     * 
     * @param timestamp
     * @throws PersistitException
     */
    synchronized void save(final long timestamp) throws PersistitException {

    }

}
