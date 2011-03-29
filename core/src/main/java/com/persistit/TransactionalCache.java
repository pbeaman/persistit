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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.exception.RollbackException;

/**
 * Abstract superclass of any object that needs transactional semantics while
 * maintaining state information in memory. For example, a concrete
 * implementation might maintain a statistical aggregation based on the contents
 * of a Persistit database. This abstract superclass provides the mechanism to
 * ensure the consistency of this state.
 * <p>
 * An application registers an instance TC of this class with Persistit prior to
 * calling the {@link Persistit#initialize()} method. During Persisit's startup
 * and recovery processing, the state of TC is modified to include the effects
 * of all previously committed transactions. During normal operation, code
 * performed within the scope of a Transaction calls methods of TC to read
 * and/or modify its state. The modifications remain private to the executing
 * transaction until the transaction commits. At that time the modifications are
 * record on the Persistit Journal and applied to a globally visible version of
 * TC's state. Operations that read from TC state are recorded in the
 * transaction's read set and verified for consistency during commit processing.
 * <p>
 * 
 * 
 * @author peter
 * 
 */
public abstract class TransactionalCache {

    final Persistit _persistit;

    Checkpoint _checkpoint;

    TransactionalCache _previousVersion;

    protected TransactionalCache(final Persistit persistit) {
        _persistit = persistit;
        _persistit.addTransactionalCache(this);
    }

    protected abstract Update createUpdate(final byte opCode);

    protected abstract static class Update {
        long _timestamp;
        byte _opCode;
        Object _arg;

        protected Update(byte opCode) {
            _opCode = opCode;
        }

        protected Update(byte opCode, Object arg) {
            _opCode = opCode;
            _arg = arg;
        }

        /**
         * Serialize this Update to an underlying ByteBuffer. Subclasses should
         * override {@link #writeArg(ByteBuffer)} to efficiently record the
         * argument value.
         * 
         * @param bb
         * @throws IOException
         */
        protected final void write(final ByteBuffer bb) throws IOException {
            bb.put(_opCode);
            writeArg(bb);
        }

        /**
         * Serialize the argument value to the supplied ByteBuffer. This is a
         * generic implementation that requires the argument value to be
         * non-null and serializable. Subclasses may override with more
         * efficient implementations.
         * 
         * @param bb
         * @throws IOException
         */
        protected void writeArg(final ByteBuffer bb) throws IOException {
            Serializable s = (Serializable) _arg;
            new ObjectOutputStream(new OutputStream() {

                @Override
                public void write(int b) throws IOException {
                    bb.put((byte) b);
                }

                @Override
                public void write(byte[] src, int offset, int length)
                        throws IOException {
                    bb.put(src, offset, length);
                }

            }).writeObject(s);
        }

        /**
         * Read an Update from the supplied ByteBuffer. Subclasses should
         * override {@link #readArg(ByteBuffer)} to optimize deserialization.
         * 
         * @param bb
         * @throws IOException
         */
        protected final void read(final ByteBuffer bb) throws IOException {
            _opCode = bb.get();
            readArg(bb);
        }

        /**
         * Deserialize the argument value from the supplied ByteBuffer. This is
         * a generic implementation that assume the value was written through
         * default Java serialization. Subclasses may override with more
         * efficient implementations.
         * 
         * @param bb
         * @throws IOException
         */
        protected void readArg(final ByteBuffer bb) throws IOException {
            try {
                _arg = new ObjectInputStream(new InputStream() {

                    @Override
                    public int read() throws IOException {
                        return bb.get() & 0xFF;
                    }

                    @Override
                    public int read(final byte[] bytes, final int offset,
                            final int length) throws IOException {
                        bb.get(bytes, offset, length);
                        return length;
                    }

                }).readObject();
            } catch (SecurityException e) {
                throw new IOException(e);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        /**
         * Attempt to combine this Update with a previously record update. For
         * example suppose the supplied Update and this Update each add 1 to the
         * same counter. Then this method modifies the supplied Update to add 2
         * and returns <code>true</code> to signify that this Update should not
         * be added to the pending update queue.
         * <P>
         * Default implementation does nothing and returns <code>false</code>.
         * Subclasses may override to provide more efficient behavior.
         * 
         * @param previous
         * @return <code>true</code> if this Update was successfully combined
         *         with <code>previous</code>.
         */
        protected boolean combine(final Update previous) {
            return false;
        }

        /**
         * Attempt to cancel this update with a previous update. For example,
         * suppose the supplied Update increments a counter, and this Update
         * decrements the same counter. Then this method could return
         * <code>true</code> to signify that both Updates can be removed from
         * the pending update queue.
         * <p>
         * Default implementation does nothing and returns <code>false</code>.
         * Subclasses may override to provide more efficient behavior.
         * 
         * @param previous
         * @return <code>true</code> if this Update successfully canceled the
         */
        protected boolean cancel(final Update previous) {
            return false;
        }

        /**
         * Apply the update to the state of the TransactionalCache. This method
         * is called during commit processing.
         */
        protected abstract void apply(final TransactionalCache tc);
    }

    /**
     * Read an Update from the supplied ByteBuffer. Subclasses should override
     * {@link #readArg(ByteBuffer)} to optimize deserialization.
     * 
     * @param bb
     * @throws IOException
     */
    protected final void read(final ByteBuffer bb) throws IOException {
        final byte opCode = bb.get();
        final Update update = createUpdate(opCode);
        update.readArg(bb);
    }

    /**
     * Return a globally unique serial number for a specific instance of a
     * <code>TransactionalCache</code> implementation. The value of
     * serialVersionUID generated for Java serialization is recommended. This
     * value is used during recovery processing to direct Update records to
     * particular cache instances.
     * 
     * @return a unique ID value
     */
    protected abstract long cacheId();

    /**
     * Read the value of a state variable, including all updates performed by
     * previously committed transactions. Note that any Updates performed within
     * the scope of the current {@link Transaction} are not included in the
     * returned value. Updates within the current <code>Transaction</code> are
     * applied only when the it commits.
     * 
     * @param key
     *            A key value used by concrete implementations to select a
     *            particular state variable.
     * @return The associated value.
     */
    public abstract Object get(final Object key) throws RollbackException;

    /**
     * Inserts the supplied {@link TransactionalCache#Update)} into the update
     * queue for this transaction. Subclasses provide convenience methods to
     * generate and enqueue updates. This method attempts to cancel or combine
     * the supplied <code>Update</code> with the previously enqueued
     * <code>Update</code>, and so may result in either removing or modifying
     * the previous <code>Update</code> rather than adding a new record.
     * 
     * @param update
     */
    protected final void update(final Update update) {
        final Transaction transaction = _persistit.getTransaction();
        if (!transaction.isActive()) {
            throw new IllegalStateException(
                    "TransactionalCache may be updated only within a transaction");
        }
        final List<Update> updates = transaction.updateList(this);
        if (!updates.isEmpty()) {
            final Update u = updates.get(updates.size() - 1);
            if (update.cancel(u)) {
                updates.remove(updates.size() - 1);
                return;
            } else if (update.combine(u)) {
                return;
            }
        }
        updates.add(update);
    }

    /**
     * Commit all the {@link TransactionalCache#Update} records. As a
     * side-effect, this method may create a pre-checkpoint copy of this
     * <code>TransactionalCache</code> to contain only values Updated prior to
     * the checkpoint.
     */
    final void commit() {
        final Transaction transaction = _persistit.getTransaction();
        final long timestamp = transaction.getCommitTimestamp();
        if (timestamp == -1) {
            throw new IllegalStateException("Method must be called from doCommit");
        }
        checkpoint(_persistit.getCurrentCheckpoint());
        TransactionalCache tc = this;
        final List<Update> updates = transaction.updateList(this);
        while (tc != null) {
            for (int index = 0; index < updates.size(); index++) {
                updates.get(index).apply(tc);
            }
            if (timestamp > tc._checkpoint.getTimestamp()) {
                break;
            }
            tc = tc._previousVersion;
        }
    }

    /**
     * Declares a new Checkpoint for this TransactionalCache.
     * 
     * @param checkpoint
     */
    public final synchronized void checkpoint(final Checkpoint checkpoint) {
        if (_checkpoint == null) {
            _checkpoint = checkpoint;
        } else if (checkpoint.getTimestamp() > _checkpoint.getTimestamp()) {
            _previousVersion = copy();
            _checkpoint = checkpoint;
        }
    }

    /**
     * Construct a copy of this <code>TransactionalCache</code>. The copy is
     * pinned to its declared checkpoint and will receive no updates issued
     * subsequent to that checkpoint.
     * <p>
     * Note that a subclass may use the {@link #clone()} method to create this
     * copy.
     * 
     * @return the copy
     */
    public abstract TransactionalCache copy();

    /**
     * Compute size in bytes of the space required to write a checkpoint of this
     * TransactionalCache's state. This method is called before the
     * {@link TransactionalCache#checkpoint(ByteBuffer, Checkpoint)} to ensure
     * sufficient space has been allocated in the journal's write buffer. The
     * implementation may return zero to indicate that the checkpoint will not
     * be written to the journal.
     * 
     * @see #checkpoint(ByteBuffer, Checkpoint)
     * 
     * @param checkpoint
     */
    public abstract void computeCheckpointSize(Checkpoint checkpoint);

    /**
     * Write a recoverable checkpoint of this TransactionalCache's state. The
     * state information may be recorded on the journal via the supplied
     * ByteBuffer. Alternatively the implementation may record state to B-Trees
     * and choose not to update the journal at all. In this case, the
     * {@link #checkpointSize(Checkpoint)} should return zero and this method
     * should do nothing.
     * 
     * @param byteBuffer
     * @param checkpoint
     * @throws Exception
     */
    public abstract void writeCheckpoint(ByteBuffer byteBuffer,
            Checkpoint checkpoint) throws Exception;

}
