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
import java.util.concurrent.atomic.AtomicBoolean;

import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;

/**
 * Abstract superclass of any object that needs transactional semantics while
 * maintaining state information in memory. For example, a concrete
 * implementation might maintain a statistical aggregation based on the contents
 * of a Persistit database. This abstract superclass provides the mechanism to
 * ensure the consistency of this state.
 * <p>
 * An application calls {@link #register()} to register an instance of this
 * class with Persistit prior to calling the {@link Persistit#initialize()}
 * method. The instance T must provide a unique, permanent ID value from its
 * {@link #cacheId()} method. During Persisit's startup and recovery processing,
 * the state of T is modified to include the effects of all previously committed
 * transactions. During normal operation, code performed within the scope of a
 * Transaction calls methods of T to read and/or modify its state. The
 * modifications remain private to the executing transaction until the
 * transaction commits. At that time the modifications are recorded on the
 * Persistit Journal and applied to a globally visible version of T's state.
 * <p>
 * A concrete <code>TransactionalCache</code> implementation must provide one or
 * more concrete implementations of the inner class
 * {@link TransactionalCache.Update}. These small objects record the
 * modifications being made to the state. A minimal implementation of
 * <code>Update</code> provides methods to apply the modification described in
 * the update to the cached state, and a methods used to serialize the update to
 * the journal. For convenience, the following more specialized subclasses are
 * provided:
 * <ul>
 * <li>{@link TransactionalCache.UpdateObject}</li>
 * <li>{@link TransactionalCache.UpdateInt}</li>
 * <li>{@link TransactionalCache.UpdateIntLong}</li>
 * <li>{@link TransactionalCache.UpdateLong}</li>
 * <li>{@link TransactionalCache.UpdateByteArray}</li>
 * <li>{@link TransactionalCache.UpdateIntArray}</li>
 * <li>{@link TransactionalCache.UpdateLongArray}</li>
 * </ul>
 * where the name of the class describes the structure of the data associated
 * with the update. Each of these specialized subclasses has a final concrete
 * implementation of the serialization methods and requires only the
 * {@link Update#apply(TransactionalCache)} method to be supplied. It is
 * recommended to extend one of these Update classes whenever possible.
 * <p>
 * Every <code>Update</code> class must be identified by a unique (within the
 * scope of the <code>TransactionalCache</code>), permanent byte value called
 * its <code>opCode</code>. This value identifies the class of the
 * <code>Update</code> for journal recovery. The recovery process reads the byte
 * code, invokes T's {@link #createUpdate(byte)} method to construct an
 * appropriate <code>Update</code>, then reads the update's argument value(s)
 * using its {@link Update#readArgs(ByteBuffer)} method. Finally, recovery calls
 * the update's {link {@link Update#apply(TransactionalCache)} method to apply
 * it to T's evolving state.
 * <p>
 * A <code>TransactionalCache</code> implementation must supply several other
 * methods:
 * <dl>
 * <dt>{@link #cacheId()}</dt>
 * <dd>Must provide a permanent unique ID for a <code>TransactionCache</code>
 * instance. This method is used during recovery to match journal records with
 * registered <code>TransactionCache</code> instances.</dd>
 * <dt>{@link #createUpdate(byte)}</dt>
 * <dd>Must construct an instance of an <code>Update</code> identified by the
 * byte-valued opCode.</dd>
 * <dt>{@link #copy()}</dt>
 * <dd>Must produce a copy of the <code>TransactionalCache</code>. The copy is
 * used to hold a version of the state constant for a checkpoint; hence a deep
 * copy of all state information is required. It is generally not sufficient to
 * invoke the {@link #clone()} method.</dd>
 * <dt>{@link #save()}</dt>
 * <dd>Must save the state of this <code>TransactionalCache</code> in a manner
 * that is recoverable during the recovery process. This method is called
 * whenever Persistit performs a {@link Persistit#checkpoint()} operation.
 * Generally the implementation should record its state in one or more Persistit
 * B-Trees. The method will be called within the scope of a special transaction
 * used to reload the state during recovery.</dd>
 * <dt>{@link #load()}</dt>
 * <dd>Must load the state information during recovery. This method should
 * restore the state information of this <code>TransactionalCache</code> to that
 * which was present when the {@link #save()} method was called.</dd>
 * </dl>
 * 
 * @author peter
 * 
 */
public abstract class TransactionalCache {

    private final static Update SAVED = new ReloadUpdate();

    protected Persistit _persistit;

    protected Checkpoint _checkpoint;

    protected TransactionalCache _previousVersion;

    private final AtomicBoolean _changed = new AtomicBoolean();

    protected TransactionalCache(final Persistit persistit) {
        _persistit = persistit;
    }

    protected TransactionalCache(final TransactionalCache tc) {
        this(tc._persistit);
        _changed.set(tc._changed.get());
        _checkpoint = tc._checkpoint;
        _previousVersion = tc._previousVersion;
    }

    protected abstract Update createUpdate(final byte opCode);

    public abstract static class Update {
        final byte _opCode;

        private Update() {
            _opCode = 0;
        }

        protected Update(byte opCode) {
            if (opCode == 0) {
                throw new IllegalArgumentException();
            }
            _opCode = opCode;
        }

        /**
         * Compute the number of bytes required to serialize this update, not
         * including the opcode. This method may return an overestimate.
         * 
         * @return number of bytes to reserve for serialization.
         */
        protected abstract int size();

        /**
         * Serialize this Update to an underlying ByteBuffer. Subclasses map
         * override {@link #writeArgs(ByteBuffer)} to efficiently record the
         * argument value.
         * 
         * @param bb
         * @throws IOException
         */
        final void write(final ByteBuffer bb) throws IOException {
            bb.put(_opCode);
            writeArgs(bb);
        }

        /**
         * Serialize the argument value to the supplied ByteBuffer.
         * 
         * @param bb
         * @throws IOException
         */
        protected abstract void writeArgs(final ByteBuffer bb) throws IOException;

        /**
         * Deserialize the argument value from the supplied ByteBuffer.
         * 
         * @param bb
         * @throws IOException
         */
        protected abstract void readArgs(final ByteBuffer bb) throws IOException;

        /**
         * Attempt to combine this Update with a previously recorded update. For
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
         * Apply the update to the state of the supplied TransactionalCache.
         * This method is called during commit processing.
         */
        protected abstract void apply(final TransactionalCache tc);
    }

    public abstract static class UpdateObject extends Update {

        protected UpdateObject(byte opCode) {
            super(opCode);
        }

        /**
         * Compute the number of bytes required to serialize this update, not
         * including the opcode. This method may return an overestimate.
         * 
         * @return number of bytes to reserve for serialization.
         */
        @Override
        protected abstract int size();

        /**
         * Serialize the argument value to the supplied ByteBuffer. This is a
         * generic implementation that requires the argument value to be
         * non-null and serializable. Subclasses may override with more
         * efficient implementations.
         * 
         * @param bb
         * @throws IOException
         */
        @Override
        protected void writeArgs(final ByteBuffer bb) throws IOException {
            Serializable s = (Serializable) getArg();
            new ObjectOutputStream(new OutputStream() {

                @Override
                public void write(int b) throws IOException {
                    bb.put((byte) b);
                }

                @Override
                public void write(byte[] src, int offset, int length) throws IOException {
                    bb.put(src, offset, length);
                }

            }).writeObject(s);
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
        @Override
        protected void readArgs(final ByteBuffer bb) throws IOException {
            try {
                setArg(new ObjectInputStream(new InputStream() {

                    @Override
                    public int read() throws IOException {
                        return bb.get() & 0xFF;
                    }

                    @Override
                    public int read(final byte[] bytes, final int offset, final int length) throws IOException {
                        bb.get(bytes, offset, length);
                        return length;
                    }

                }).readObject());
            } catch (SecurityException e) {
                throw new IOException(e);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        /**
         * Implementation is required for default object serialization.
         * 
         * @return the argument value as an Object.
         */
        protected abstract Object getArg();

        /**
         * Implementation is required for default object serialization. Set the
         * argument to the supplied Object.
         * 
         * @param arg
         */
        protected abstract void setArg(Object arg);

    }

    /**
     * Abstract superclass of any Update that holds a single int-valued
     * argument. This implements provides serialization code optimized for this
     * case.
     */
    public abstract static class UpdateInt extends Update {
        protected int _arg;

        protected UpdateInt(byte opCode) {
            super(opCode);
        }

        @Override
        protected void writeArgs(final ByteBuffer bb) throws IOException {
            bb.putInt(_arg);
        }

        @Override
        protected void readArgs(final ByteBuffer bb) throws IOException {
            _arg = bb.getInt();
        }

        @Override
        protected int size() {
            return 4;
        }
    }

    /**
     * Abstract superclass of any Update that holds an int and a long argument
     * value.
     */
    public abstract static class UpdateIntLong extends Update {
        protected int _arg1;
        protected long _arg2;

        protected UpdateIntLong(byte opCode) {
            super(opCode);
        }

        @Override
        protected void writeArgs(final ByteBuffer bb) throws IOException {
            bb.putInt(_arg1);
            bb.putLong(_arg2);
        }

        @Override
        protected void readArgs(final ByteBuffer bb) throws IOException {
            _arg1 = bb.getInt();
            _arg2 = bb.getLong();
        }

        @Override
        protected int size() {
            return 12;
        }

    }

    /**
     * Abstract superclass of any Update that holds a single long-valued
     * argument. This subclass provides serialization code optimized for this
     * case.
     */
    public abstract static class UpdateLong extends Update {
        protected long _arg;

        protected UpdateLong(byte opCode) {
            super(opCode);
        }

        @Override
        protected void writeArgs(final ByteBuffer bb) throws IOException {
            bb.putLong(_arg);
        }

        @Override
        protected void readArgs(final ByteBuffer bb) throws IOException {
            _arg = bb.getLong();
        }

        @Override
        protected int size() {
            return 8;
        }
    }

    /**
     * Abstract superclass of any Update that holds its argument in the form of
     * an array of up to 65535 bytes. This subclass provides serialization code
     * optimized for this case.
     */
    public abstract static class UpdateByteArray extends Update {
        protected byte[] _args;

        protected UpdateByteArray(byte opCode) {
            super(opCode);
        }

        @Override
        protected void writeArgs(final ByteBuffer bb) throws IOException {
            bb.putChar((char) _args.length);
            for (int index = 0; index < _args.length; index++) {
                bb.put(_args[index]);
            }
        }

        @Override
        protected void readArgs(final ByteBuffer bb) throws IOException {
            int length = bb.getChar();
            _args = new byte[length];
            for (int index = 0; index < _args.length; index++) {
                _args[index] = bb.get();
            }
        };

        @Override
        protected int size() {
            return _args.length + 2;
        }

    }

    /**
     * Abstract superclass of any Update that holds its argument in the form of
     * an array of up to 65535 int values. This subclass provides serialization
     * code optimized for this case.
     */
    public abstract static class UpdateIntArray extends Update {
        protected int[] _args;

        protected UpdateIntArray(byte opCode) {
            super(opCode);
        }

        @Override
        protected void writeArgs(final ByteBuffer bb) throws IOException {
            bb.putChar((char) _args.length);
            for (int index = 0; index < _args.length; index++) {
                bb.putInt(_args[index]);
            }
        }

        @Override
        protected void readArgs(final ByteBuffer bb) throws IOException {
            int length = bb.getChar();
            _args = new int[length];
            for (int index = 0; index < _args.length; index++) {
                _args[index] = bb.getInt();
            }
        };

        @Override
        protected int size() {
            return _args.length * 4 + 2;
        }
    }

    /**
     * Abstract superclass of any Update that holds its argument in the form of
     * an array of up to 65535 long values. This subclass provides serialization
     * code optimized for this case.
     */
    public abstract static class UpdateLongArray extends Update {
        protected long[] _args;

        protected UpdateLongArray(byte opCode) {
            super(opCode);
        }

        @Override
        protected void writeArgs(final ByteBuffer bb) throws IOException {
            bb.putChar((char) _args.length);
            for (int index = 0; index < _args.length; index++) {
                bb.putLong(_args[index]);
            }
        }

        @Override
        protected void readArgs(final ByteBuffer bb) throws IOException {
            int length = bb.getChar();
            _args = new long[length];
            for (int index = 0; index < _args.length; index++) {
                _args[index] = bb.getLong();
            }
        };

        @Override
        protected int size() {
            return _args.length * 8 + 2;
        }
    }

    /**
     * Special marker Update used during recovery.
     */
    public final static class ReloadUpdate extends Update {

        protected ReloadUpdate() {
            super();
        }

        @Override
        protected void writeArgs(final ByteBuffer bb) throws IOException {

        }

        @Override
        protected void readArgs(final ByteBuffer bb) throws IOException {

        };

        @Override
        protected int size() {
            return 0;
        }

        @Override
        protected boolean combine(final Update update) {
            if (update instanceof ReloadUpdate) {
                return true;
            }
            return false;
        }

        @Override
        protected void apply(TransactionalCache tc) {
            // Does nothing during normal processing - causes
            // reload from saved checkpoint during recovery
        }
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof TransactionalCache) {
            return ((TransactionalCache) object).cacheId() == cacheId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (int) (cacheId() >>> 32) ^ (int) (cacheId());
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
            throw new IllegalStateException("TransactionalCache may be updated only within a transaction");
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

    private synchronized void updateCheckpoint() {
        final Checkpoint checkpoint = _persistit.getCurrentCheckpoint();
        if (_checkpoint == null) {
            _checkpoint = checkpoint;
        } else if (checkpoint.getTimestamp() > _checkpoint.getTimestamp()) {
            _previousVersion = copy();
            _checkpoint = checkpoint;
        }
    }

    /**
     * Commit all the {@link TransactionalCache#Update} records. As a
     * side-effect, this method may create a pre-checkpoint copy of this
     * <code>TransactionalCache</code> to contain only values Updated prior to
     * the checkpoint.
     */
    final boolean commit(final Transaction transaction) {
        final List<Update> updates = transaction.updateList(this);
        if (updates.isEmpty()) {
            return false;
        }
        final long timestamp = transaction.getCommitTimestamp();
        if (timestamp == -1) {
            throw new IllegalStateException("Must be called from doCommit");
        }
        updateCheckpoint();
        TransactionalCache tc = this;
        while (tc != null) {
            for (int index = 0; index < updates.size(); index++) {
                updates.get(index).apply(tc);
            }
            if (timestamp > tc._checkpoint.getTimestamp()) {
                break;
            }
            tc = tc._previousVersion;
        }
        updates.clear();
        _changed.set(true);
        return true;
    }

    final void recoverUpdates(final ByteBuffer bb, final long timestamp) throws PersistitException {
        while (bb.hasRemaining()) {
            final byte opCode = bb.get();
            if (opCode == 0) {
                load();
                _checkpoint = new Checkpoint(timestamp, System.currentTimeMillis());
            } else {
                final Update update = createUpdate(opCode);
                try {
                    update.readArgs(bb);
                    update.apply(this);
                } catch (IOException e) {
                    throw new PersistitIOException(e);
                }
            }
        }
    }

    /**
     * Get the version of this <code>TransactionalCache</code> that was valid at
     * the specified timestamp.
     * 
     * @param checkpoint
     * @return
     */
    final TransactionalCache version(final Checkpoint checkpoint) {
        TransactionalCache tc = this;
        while (tc != null) {
            if (tc._checkpoint.getTimestamp() <= checkpoint.getTimestamp()) {
                return tc;
            }
            tc = tc._previousVersion;
        }
        return tc;
    }

    final void save(final Checkpoint checkpoint) throws PersistitException {
        updateCheckpoint();
        TransactionalCache tc = this;
        TransactionalCache newer = null;
        while (tc != null) {
            if (tc._checkpoint != null
            // The checkpoint value in a TransactionalCache version is the
            // beginning, not the end of its era.
                    && tc._checkpoint.getTimestamp() < checkpoint.getTimestamp()) {
                tc.save();
                update(SAVED);
                if (newer != null) {
                    newer._previousVersion = null;
                }
                break;
            }
            newer = tc;
            tc = tc._previousVersion;
        }
    }

    /**
     * Register this TransactionalCache instance with Persistit. This call must
     * occur before Persistit is initialized.
     */
    public final void register() {
        _persistit.addTransactionalCache(this);
    }

    /**
     * Construct a copy of this <code>TransactionalCache</code>. Any data
     * structures used to hold state information must be deep-copied. The copy
     * will be pinned to its declared checkpoint and will receive no updates
     * issued by transactions committing after that checkpoint, while the
     * original TransactionalCache will continue to receive new updates.
     * 
     * @return the copy
     */
    protected abstract TransactionalCache copy();

    /**
     * Save the state of this <code>TransactionalCache</code> such that it will
     * be recoverable, typically by writing its state to backing store in
     * Persistit trees.
     */
    protected abstract void save() throws PersistitException;

    /**
     * Load the state of this <code>TransactionalCache</code>, typically from
     * backing store in Persistit trees.
     */
    protected abstract void load() throws PersistitException;

    /**
     * Release all memory and other resources held by this
     * <code>TransactionalCache</code>. Subclasses may extend this method, but
     * should always call <code>super.close()</code>.
     */
    protected void close() {
        _persistit = null;
    }
}
