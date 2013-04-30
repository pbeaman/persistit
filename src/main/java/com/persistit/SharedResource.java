/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.persistit.exception.PersistitInterruptedException;
import com.persistit.util.Util;

/**
 * Base class for objects that need synchronization across threads. The methods
 * {@link #claim(boolean)} and {@link #register()} establish and release either
 * exclusive or non-exclusive access to the object. Numerous other status flags
 * are also managed by this class, such as {@link #isDirty()} and
 * {@link #isValid()}.
 * <p>
 * The implementation of this class uses a {@link AbstractQueuedSynchronizer}
 * and is similar to {@link ReentrantReadWriteLock}. The synchronization policy
 * is a non-strict fair policy which is necessary and sufficient to prevent
 * starvation on busy system.
 * 
 * See {@link Buffer}, {@link Tree} and {@link Volume}.
 * 
 * @author peter
 */
abstract class SharedResource {

    /**
     * Default maximum time to wait for access to this resource. Methods throw
     * an InUseException when this time is exceeded.
     */
    public final static long DEFAULT_MAX_WAIT_TIME = 60000L;

    /**
     * Mask for count of Threads holding a reader or writer claim (0-32767)
     */
    final static int CLAIMED_MASK = 0x00007FFF;
    /**
     * Mask for count of Threads holding a writer claim (0 or 1)
     */
    final static int WRITER_MASK = 0x00008000;
    /**
     * Mask for field indicating the resource needs to be written
     */
    final static int DIRTY_MASK = 0x00010000;

    /**
     * Status field mask for valid bit. The bit is set if the contents of the
     * buffer accurately reflects the status of the page.
     */
    final static int VALID_MASK = 0x00020000;

    /**
     * Status field mask for a resource that is dirty but not required to be
     * written with any checkpoint.
     */
    final static int TEMPORARY_MASK = 0x00400000;

    /**
     * Status field mask indicating a resource has been touched. Used by
     * clock-based page replacement algorithm.
     */
    final static int TOUCHED_MASK = 0x08000000;

    /**
     * Mask for bit field indicating that resource (a Buffer) should not be
     * replaced. The buffer houses a Volume's head page.
     */
    final static int FIXED_MASK = 0x40000000;

    final static AtomicLong ACQUIRE_LOOPS = new AtomicLong();
    final static AtomicLong RELEASE_LOOPS = new AtomicLong();
    final static AtomicLong SET_BIT_LOOPS = new AtomicLong();
    final static AtomicLong CLEAR_BIT_LOOPS = new AtomicLong();

    /**
     * Extension of {@link AbstractQueuedSynchronizer} with Persistit semantics.
     */
    private static class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean tryAcquire(final int arg) {
            assert arg == 1;
            //
            // Implement non-strict fairness doctrine, as suggested in
            // download.oracle.com/javase/6/docs/api/java/util/concurrent/locks/AbstractQueuedSynchronizer.html
            //
            final Thread thisThread = Thread.currentThread();
            for (;;) {
                final Thread queuedThread = getFirstQueuedThread();
                if (queuedThread != null && queuedThread != thisThread && getExclusiveOwnerThread() != thisThread) {
                    return false;
                }
                final int state = getState();
                if (!isAvailable(state)) {
                    return false;
                } else if (compareAndSetState(state, (state | WRITER_MASK) + 1)) {
                    setExclusiveOwnerThread(thisThread);
                    return true;
                }
                ACQUIRE_LOOPS.incrementAndGet();
            }
        }

        @Override
        protected int tryAcquireShared(final int arg) {
            assert arg == 1;
            //
            // Implement non-strict fairness doctrine, as suggested in
            // download.oracle.com/javase/6/docs/api/java/util/concurrent/locks/AbstractQueuedSynchronizer.html
            //
            final Thread thisThread = Thread.currentThread();
            for (;;) {
                final Thread queuedThread = getFirstQueuedThread();
                if (queuedThread != null && queuedThread != thisThread && getExclusiveOwnerThread() != thisThread) {
                    return -1;
                }
                final int state = getState();
                if (!isAvailableShared(state)) {
                    return -1;
                } else if (compareAndSetState(state, state + 1)) {
                    return CLAIMED_MASK - (state & CLAIMED_MASK) - 1;
                }
                ACQUIRE_LOOPS.incrementAndGet();
            }
        }

        /**
         * Attempt to convert shared to exclusive. The caller must already have
         * acquired shared access. This method upgrades the state to exclusive,
         * but only if there is exactly one shared acquire.
         * 
         * TODO - prove that caller already a reader claim
         * 
         * @return
         */
        private boolean tryUpgrade() {
            for (;;) {
                final int state = getState();
                final Thread thisThread = Thread.currentThread();
                if ((state & CLAIMED_MASK) != 1 || ((state & WRITER_MASK) != 0)
                        && getExclusiveOwnerThread() != thisThread) {
                    return false;
                } else if (compareAndSetState(state, state | WRITER_MASK)) {
                    setExclusiveOwnerThread(thisThread);
                    return true;
                }
                ACQUIRE_LOOPS.incrementAndGet();
            }
        }

        @Override
        protected boolean tryRelease(final int arg) {
            return (releaseState(arg) & WRITER_MASK) == 0;
        }

        @Override
        protected boolean tryReleaseShared(final int arg) {
            return (releaseState(arg) & WRITER_MASK) == 0;
        }

        @Override
        protected boolean isHeldExclusively() {
            return (getState() & WRITER_MASK) != 0;
        }

        private boolean isAvailable(final int state) {
            return (state & CLAIMED_MASK) < CLAIMED_MASK
                    && ((state & CLAIMED_MASK) == 0 || getExclusiveOwnerThread() == Thread.currentThread());
        }

        private boolean isAvailableShared(final int state) {
            return (state & CLAIMED_MASK) < CLAIMED_MASK
                    && ((state & WRITER_MASK) == 0 || getExclusiveOwnerThread() == Thread.currentThread());
        }

        private int releaseState(final int count) {
            assert count == 0 || count == 1;
            for (;;) {
                final int state = getState();
                if ((state & CLAIMED_MASK) == 1) {
                    final int newState = (state - count) & ~WRITER_MASK;
                    // Do this first so that another thread setting
                    // a writer claim does not lose its copy.
                    setExclusiveOwnerThread(null);
                    if (compareAndSetState(state, newState)) {
                        return newState;
                    }
                } else if ((state & CLAIMED_MASK) > 1) {
                    if (count == 0) {
                        return state;
                    }
                    final int newState = state - 1;
                    if (compareAndSetState(state, newState)) {
                        return newState;
                    }
                } else {
                    throw new IllegalMonitorStateException("Unmatched attempt to release " + this);
                }
                RELEASE_LOOPS.incrementAndGet();
            }
        }

        // Visible to outer class
        private int state() {
            return getState();
        }

        private Thread writerThread() {
            return getExclusiveOwnerThread();
        }

        private boolean setBitsInState(final int mask) {
            for (;;) {
                final int state = getState();
                final int newState = state | mask;
                if (compareAndSetState(state, newState)) {
                    return state != newState;
                }
                SET_BIT_LOOPS.incrementAndGet();
            }
        }

        private boolean clearBitsInState(final int mask) {
            for (;;) {
                final int state = getState();
                final int newState = state & ~mask;
                if (compareAndSetState(state, newState)) {
                    return state != newState;
                }
                CLEAR_BIT_LOOPS.incrementAndGet();
            }
        }

        private boolean testBitsInState(final int mask) {
            return (getState() & mask) != 0;
        }
    }

    protected final Persistit _persistit;

    private final Sync _sync = new Sync();

    /**
     * A counter that increments every time the resource is changed.
     */
    private final AtomicLong _generation = new AtomicLong();

    protected SharedResource(final Persistit persistit) {
        _persistit = persistit;
    }

    public boolean isAvailable(final boolean writer) {
        return writer ? _sync.isAvailable(_sync.state()) : _sync.isAvailableShared(_sync.state());
    }

    boolean isDirty() {
        return _sync.testBitsInState(DIRTY_MASK);
    }

    public boolean isValid() {
        return _sync.testBitsInState(VALID_MASK);
    }

    public boolean isTemporary() {
        return _sync.testBitsInState(TEMPORARY_MASK);
    }

    boolean isFixed() {
        return _sync.testBitsInState(FIXED_MASK);
    }

    boolean isWriter() {
        return _sync.testBitsInState(WRITER_MASK);
    }

    /**
     * Indicates whether this Thread has a writer claim on this page.
     * 
     * @return <i>true</i> if this Thread has a writer claim on this page.
     */
    boolean isOwnedAsWriterByMe() {
        return (_sync.writerThread() == Thread.currentThread());
    }

    boolean isOwnedAsWriterByOther() {
        final Thread t = _sync.writerThread();
        return t != null && t != Thread.currentThread();
    }

    boolean claim(final boolean writer) throws PersistitInterruptedException {
        return claim(writer, DEFAULT_MAX_WAIT_TIME);
    }

    boolean claim(final boolean writer, final long timeout) throws PersistitInterruptedException {
        if (timeout == 0) {
            if (writer) {
                return _sync.tryAcquire(1);
            } else {
                return _sync.tryAcquireShared(1) >= 0;
            }
        } else {
            final long ns = Math.min(timeout, Long.MAX_VALUE / Util.NS_PER_MS) * Util.NS_PER_MS;
            try {
                if (writer) {
                    if (_sync.tryAcquireNanos(1, ns)) {
                        return true;
                    }
                } else {
                    if (_sync.tryAcquireSharedNanos(1, ns)) {
                        return true;
                    }
                }
            } catch (final InterruptedException e) {
                throw new PersistitInterruptedException(e);
            }
            return false;
        }
    }

    boolean upgradeClaim() {
        return _sync.tryUpgrade();
    }

    void release() {
        _sync.release(1);
    }

    void releaseWriterClaim() {
        _sync.release(0);
    }

    boolean setDirty() {
        return _sync.setBitsInState(DIRTY_MASK);
    }

    boolean clearDirty() {
        return _sync.clearBitsInState(DIRTY_MASK);
    }

    void setTouched() {
        _sync.setBitsInState(TOUCHED_MASK);
    }

    void clearTouched() {
        _sync.clearBitsInState(TOUCHED_MASK);
    }

    void setFixed() {
        _sync.setBitsInState(FIXED_MASK);
    }

    void clearFixed() {
        _sync.clearBitsInState(FIXED_MASK);
    }

    boolean isTouched() {
        return _sync.testBitsInState(TOUCHED_MASK);
    }

    public long getGeneration() {
        return _generation.get();
    }

    void setTemporary() {
        _sync.setBitsInState(TEMPORARY_MASK);
    }

    void clearTemporary() {
        _sync.clearBitsInState(TEMPORARY_MASK);
    }

    void setValid() {
        _sync.setBitsInState(VALID_MASK);
    }

    void clearValid() {
        _sync.clearBitsInState(VALID_MASK);
    }

    void bumpGeneration() {
        _generation.incrementAndGet();
    }

    int getStatus() {
        return _sync.state();
    }

    Persistit getPersistit() {
        return _persistit;
    }

    /**
     * Sets bits in the state. This method does not change the bits used by the
     * synchronizer to maintain lock state.
     * 
     * @param mask
     */
    void setStatus(final SharedResource resource) {
        final int mask = resource.getStatus();
        _sync.clearBitsInState(~(WRITER_MASK | CLAIMED_MASK));
        _sync.setBitsInState(mask & ~(WRITER_MASK | CLAIMED_MASK));
    }

    public String getStatusCode() {
        return getStatusCode(getStatus());
    }

    public String getStatusDisplayString() {
        final Thread writerThread = _sync.writerThread();
        final int state = _sync.state();
        if (writerThread == null) {
            return getStatusCode(state);
        } else {
            return getStatusCode(state) + " <" + writerThread.getName() + ">";
        }
    }

    public static String getStatusCode(final int state) {
        // Common cases so we don't have to construct new StringBuilders
        switch (state) {
        case 0:
            return "";
        case VALID_MASK:
            return "v";
        case VALID_MASK | DIRTY_MASK:
            return "vd";
        case VALID_MASK | 1:
            return "vr1";
        case VALID_MASK | WRITER_MASK | 1:
            return "vwr1";
        case VALID_MASK | DIRTY_MASK | 1:
            return "vdr1";
        case VALID_MASK | WRITER_MASK | DIRTY_MASK | 1:
            return "vdwr1";
        default:
            final StringBuilder sb = new StringBuilder(8);

            if ((state & VALID_MASK) != 0) { // TODO chars
                sb.append("v");
            }
            if ((state & DIRTY_MASK) != 0) {
                sb.append("d");
            }
            if ((state & TEMPORARY_MASK) != 0) {
                sb.append("t");
            }
            if ((state & WRITER_MASK) != 0) {
                sb.append("w");
            }
            if ((state & CLAIMED_MASK) != 0) {
                sb.append("r");
                sb.append(state & CLAIMED_MASK);
            }
            return sb.toString();
        }
    }

    public Thread getWriterThread() {
        return _sync.writerThread();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + this.hashCode() + " status=" + getStatusDisplayString();
    }
}
