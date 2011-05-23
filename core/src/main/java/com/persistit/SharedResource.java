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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @author pbeaman
 * 
 */
class SharedResource {

    /**
     * Default maximum time to wait for access to this resource. Methods throw
     * an InUseException when this time is exceeded.
     */
    final static long DEFAULT_MAX_WAIT_TIME = 60000L;

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
     * Status field mask for deleted status. This bit is set if the page belongs
     * to a Volume that is being deleted.
     */
    final static int DELETE_MASK = 0x00040000;

    /**
     * Status field mask for a resource that is dirty and must be recovered
     * concurrently with its checkpoint -- e.g., a buffer containing a page that
     * has been split.
     */
    final static int STRUCTURE_MASK = 0x00100000;

    /**
     * Status field mask for a resource that is dirty but not required to be
     * written with any checkpoint.
     */
    final static int TRANSIENT_MASK = 0x00400000;

    /**
     * Status field mask indicating a resource has been touched. Used by
     * clock-based page replacement algorithm.
     */
    final static int TOUCHED_MASK = 0x08000000;

    /**
     * Mask for bit field indicating that updates are suspended
     */
    final static int SUSPENDED_MASK = 0x10000000;

    /**
     * Mask for bit field indicating that the resource is closing
     */
    final static int CLOSING_MASK = 0x20000000;

    /**
     * Mask for bit field indicating that resource should be mounted in a fixed
     * location -- e.g., a Volume's head page.
     */
    final static int FIXED_MASK = 0x40000000;

    /**
     * Status field mask for bits that indicate this Buffer is unavailable.
     */
    final static int UNAVAILABLE_MASK = FIXED_MASK | CLAIMED_MASK | WRITER_MASK;

    private static class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean tryAcquire(int arg) {
            assert arg == 1;
            //
            // Implement non-strict fairness doctrine, as suggested in
            // download.oracle.com/javase/6/docs/api/java/util/concurrent/locks/AbstractQueuedSynchronizer.html
            //
            final Thread thisThread = Thread.currentThread();
            for (;;) {
                final Thread queuedThread = getFirstQueuedThread();
                if (queuedThread != null && queuedThread != thisThread
                        && getExclusiveOwnerThread() != thisThread) {
                    return false;
                }
                int state = getState();
                if (!isAvailable(state)) {
                    return false;
                } else if (compareAndSetState(state, (state | WRITER_MASK) + 1)) {
                    setExclusiveOwnerThread(thisThread);
                    return true;
                }
            }
        }

        @Override
        protected int tryAcquireShared(int arg) {
            assert arg == 1;
            //
            // Implement non-strict fairness doctrine, as suggested in
            // download.oracle.com/javase/6/docs/api/java/util/concurrent/locks/AbstractQueuedSynchronizer.html
            //
            final Thread thisThread = Thread.currentThread();
            for (;;) {
                final Thread queuedThread = getFirstQueuedThread();
                if (queuedThread != null && queuedThread != thisThread
                        && getExclusiveOwnerThread() != thisThread) {
                    return -1;
                }
                int state = getState();
                if (!isAvailableShared(state)) {
                    return -1;
                } else if (compareAndSetState(state, state + 1)) {
                    return CLAIMED_MASK - (state & CLAIMED_MASK) - 1;
                }
            }
        }

        /**
         * Attempt to convert shared to exclusive. The caller must already have
         * acquired shared access. This method upgrades the state to exclusive,
         * but only if there is exactly one shared acquire.
         * 
         * @return
         */
        private boolean tryUpgrade() {
            for (;;) {
                int state = getState();
                final Thread thisThread = Thread.currentThread();
                if ((state & CLAIMED_MASK) != 1 || ((state & WRITER_MASK) != 0)
                        && getExclusiveOwnerThread() != thisThread) {
                    return false;
                } else if (compareAndSetState(state, state | WRITER_MASK)) {
                    setExclusiveOwnerThread(thisThread);
                    return true;
                }
            }
        }

        @Override
        protected boolean tryRelease(int arg) {
            return (releaseState(arg) & WRITER_MASK) == 0;
        }

        @Override
        protected boolean tryReleaseShared(int arg) {
            return (releaseState(arg) & WRITER_MASK) == 0;
        }

        @Override
        protected boolean isHeldExclusively() {
            return (getState() & WRITER_MASK) != 0;
        }

        private boolean isAvailable(final int state) {
            return (state & CLAIMED_MASK) < CLAIMED_MASK
                    && ((state & CLAIMED_MASK) == 0 || getExclusiveOwnerThread() == Thread
                            .currentThread());
        }

        private boolean isAvailableShared(final int state) {
            return (state & CLAIMED_MASK) < CLAIMED_MASK
                    && ((state & WRITER_MASK) == 0 || getExclusiveOwnerThread() == Thread
                            .currentThread());
        }

        private int releaseState(final int count) {
            assert count == 0 || count == 1;
            for (;;) {
                int state = getState();
                if ((state & CLAIMED_MASK) == 1) {
                    int newState = (state - count) & ~WRITER_MASK;
                    // Do this first so that another thread setting
                    // a writer claim does not lose it's copy.
                    setExclusiveOwnerThread(null);
                    if (compareAndSetState(state, newState)) {
                        return newState;
                    }
                } else if ((state & CLAIMED_MASK) > 1) {
                    if (count == 0) {
                        return state;
                    }
                    int newState = state - 1;
                    if (compareAndSetState(state, newState)) {
                        return newState;
                    }
                } else {
                    throw new IllegalMonitorStateException(
                            "Unmatched attempt to release " + this);
                }
            }
        }

        // Visible to outer class
        private int state() {
            return getState();
        }

        private Thread writerThread() {
            return getExclusiveOwnerThread();
        }

        private void setBitsInState(final int mask) {
            for (;;) {
                final int state = getState();
                if (compareAndSetState(state, state | mask)) {
                    break;
                }
            }
        }

        private void clearBitsInState(final int mask) {
            for (;;) {
                final int state = getState();
                if (compareAndSetState(state, state & ~mask)) {
                    break;
                }
            }
        }

        private boolean testBitsInState(final int mask) {
            return (getState() & mask) != 0;
        }
    }

    protected final Persistit _persistit;

    private final Sync _sync = new Sync();

    private List<Thread> _threads = new ArrayList<Thread>();

    /**
     * A counter that increments every time the resource is changed.
     */
    protected AtomicLong _generation = new AtomicLong();

    protected SharedResource(final Persistit persistit) {
        this._persistit = persistit;
    }

    public boolean isAvailable(boolean writer) {
        return writer ? _sync.isAvailable(_sync.state()) : _sync
                .isAvailableShared(_sync.state());
    }

    boolean isDirty() {
        return _sync.testBitsInState(DIRTY_MASK);
    }

    public boolean isValid() {
        return _sync.testBitsInState(VALID_MASK);
    }

    public boolean isDeleted() {
        return _sync.testBitsInState(DELETE_MASK);
    }

    public boolean isStructure() {
        return _sync.testBitsInState(STRUCTURE_MASK);
    }

    public boolean isTransient() {
        return _sync.testBitsInState(TRANSIENT_MASK);
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
    boolean isMine() {
        return (_sync.writerThread() == Thread.currentThread());
    }

    void setFixed(boolean b) {
        if (b) {
            _sync.setBitsInState(FIXED_MASK);
        } else {
            _sync.clearBitsInState(FIXED_MASK);
        }
    }

    boolean claim(boolean writer) {
        return claim(writer, DEFAULT_MAX_WAIT_TIME);
    }

    boolean claim(boolean writer, long timeout) {
        if (timeout == 0) {
            if (writer) {
                return _sync.tryAcquire(1);
            } else {
                return _sync.tryAcquireShared(1) >= 0;
            }
        } else {
            try {
                if (writer) {
                    if (_sync.tryAcquireNanos(1, timeout * 1000000)) {
                        return true;
                    }
                } else {
                    if (_sync.tryAcquireSharedNanos(1, timeout * 1000000)) {
                        return true;
                    }
                }
            } catch (InterruptedException e) {
            }
            Debug.debug1(true);
            return false;
        }
    }

    /**
     * For debugging resource protocol issues only
     */
    synchronized void register() {
        _threads.add(Thread.currentThread());
    }

    synchronized boolean unregister() {
        final Thread t = Thread.currentThread();
        for (int index = _threads.size(); --index >= 0;) {
            if (_threads.get(index) == t) {
                _threads.remove(index);
                return true;
            }
        }
        return false;
    }

    synchronized boolean verifyReleased() {
        final Thread t = Thread.currentThread();
        for (int index = _threads.size(); --index >= 0;) {
            if (_threads.get(index) == t) {
                Debug.debug1(true);
                return false;
            }
        }
        return true;
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

    void setClean() {
        _sync.clearBitsInState(DIRTY_MASK | STRUCTURE_MASK);
    }

    void setDirty() {
        _sync.setBitsInState(DIRTY_MASK);
    }

    void setDirtyStructure() {
        _sync.setBitsInState(DIRTY_MASK | STRUCTURE_MASK);
    }

    void setTouched() {
        _sync.setBitsInState(TOUCHED_MASK);
    }

    void clearTouched() {
        _sync.clearBitsInState(TOUCHED_MASK);
    }

    boolean isTouched() {
        return _sync.testBitsInState(TOUCHED_MASK);
    }

    public long getGeneration() {
        return _generation.get();
    }

    void setTransient(final boolean b) {
        if (b) {
            _sync.setBitsInState(TRANSIENT_MASK);
        } else {
            _sync.clearBitsInState(TRANSIENT_MASK);
        }
    }

    void setValid(boolean b) {
        if (b) {
            _sync.setBitsInState(VALID_MASK);
        } else {
            _sync.clearBitsInState(VALID_MASK);
        }
    }

    public boolean isAvailable() {
        return _sync.testBitsInState(UNAVAILABLE_MASK);

    }

    void bumpGeneration() {
        _generation.incrementAndGet();
    }

    public int getStatus() {
        return _sync.state();
    }

    /**
     * Sets bits in the state. This method does not change the bits used by the
     * synchronizer to maintain lock state.
     * 
     * @param mask
     */
    void setStatus(final int mask) {
        _sync.clearBitsInState(~(WRITER_MASK | CLAIMED_MASK));
        _sync.setBitsInState(mask & ~(WRITER_MASK | CLAIMED_MASK));
    }

    public String getStatusCode() {
        return getStatusCode(getStatus());
    }

    public String getStatusDisplayString() {
        Thread writerThread = _sync.writerThread();
        int state = _sync.state();
        if (writerThread == null) {
            return getStatusCode(state);
        } else {
            return getStatusCode(state) + " <" + writerThread.getName() + ">";
        }
    }

    public static String getStatusCode(int state) {
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
            StringBuilder sb = new StringBuilder(8);
            if ((state & SUSPENDED_MASK) != 0) {
                sb.append("s");
            }
            if ((state & CLOSING_MASK) != 0) {
                sb.append("c");
            }
            if ((state & VALID_MASK) != 0) {
                sb.append("v");
            }
            if ((state & DIRTY_MASK) != 0) {
                sb.append("d");
            }
            if ((state & TRANSIENT_MASK) != 0) {
                sb.append("t");
            }
            if ((state & STRUCTURE_MASK) != 0) {
                sb.append("s");
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
        return "SharedResource" + this.hashCode() + " status="
                + getStatusDisplayString();
    }
}
