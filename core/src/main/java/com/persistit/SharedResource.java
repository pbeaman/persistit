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

import com.persistit.exception.PersistitException;

/**
 * @author pbeaman
 * 
 */
class SharedResource extends WaitingThreadManager {

    /**
     * Default maximum time to wait for access to this resource. Methods throw
     * an InUseException when this time is exceeded.
     */
    final static long DEFAULT_MAX_WAIT_TIME = 60000L;

    /**
     * Mask for count of Threads holding a reader or claim (0-32767)
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
     * Status field mask for valid bit. The bit is set it if the contents of the
     * buffer accurately reflects the status of the page.
     */
    final static int VALID_MASK = 0x00020000;

    /**
     * Status field mask for deleted status. This bit is set if the page belongs
     * to a Volume that is being deleted.
     */
    final static int DELETE_MASK = 0x00040000;

    /**
     * Status field mask for resource that is enqueued to be written
     */
    final static int ENQUEUED_MASK = 0x00080000;

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

    /**
     * Status indicating whether this resource has been claimed. Note:
     * subclasses can (carefully) overload this to use remaining bits.
     */
    protected volatile int _status;

    /**
     * A counter that increments every time the resource is changed.
     */
    protected AtomicLong _generation = new AtomicLong();

    /**
     * The Thread that holds a writer claim on this resource.
     */
    private Thread _writerThread = null;

    /**
     * An Object used for synchronization
     */
    protected Object _lock = new Object();

    SharedResource(final Persistit persistit) {
        super(persistit);
    }

    public boolean isAvailable(boolean writer) {
        synchronized (_lock) {
            return isAvailableSync(writer, 0);
        }
    }

    private boolean isAvailableSync(boolean writer, int count) {

        // Available - No claims        }

        if ((_status & (WRITER_MASK | CLAIMED_MASK)) == 0) {
            return true;
        }

        // This thread already claims it
        if (_writerThread == Thread.currentThread()
                && (_status & CLAIMED_MASK) < CLAIMED_MASK) {
            return true;
        }

        // Handles upgradeClaim when count > 0
        if (writer) {
            return ((_status & WRITER_MASK) == 0 && (_status & CLAIMED_MASK) <= count);
        }

        // Caller wants a reader claim (see if statement above), there are other
        // readers, and either no other waiting writer, or this thread already
        // has a reader claim
        if (((_status & WRITER_MASK) == 0)
                && ((_status & CLAIMED_MASK) < CLAIMED_MASK)
                && (!isWriterWaiting() || _persistit.getLockManager().isMine(
                        this))) {
            return true;
        }

        return false;

    }

    boolean isClean() {
        return !isDirty();
    }

    boolean isDirty() {
        return isSet(DIRTY_MASK);
    }

    public boolean isValid() {
        return isSet(VALID_MASK);
    }

    public boolean isDeleted() {
        return isSet(DELETE_MASK);
    }

    public boolean isStructure() {
        return isSet(STRUCTURE_MASK);
    }

    public boolean isTransient() {
        return isSet(TRANSIENT_MASK);
    }

    boolean isFixed() {
        return isSet(FIXED_MASK);
    }

    boolean isSet(final int mask) {
        return (_status & mask) == mask;
    }

    /**
     * Indicates whether this Thread has a writer claim on this page.
     * 
     * @return <i>true</i> if this Thread has a writer claim on this page.
     */
    boolean isMine() {
        synchronized (_lock) {
            if (Debug.ENABLED)
                Debug.$assert(_writerThread == null
                        || (_status & WRITER_MASK) != 0);
            return (_writerThread == Thread.currentThread());
        }
    }

    void setFixed(boolean b) {
        if (b) {
            _status |= FIXED_MASK;
        } else {
            _status &= ~FIXED_MASK;
        }
    }

    boolean claim(boolean writer) {
        return claim(writer, DEFAULT_MAX_WAIT_TIME);
    }

    boolean claim(boolean writer, long timeout) {
        WaitingThread wt = null;

        synchronized (_lock) {
            if (isAvailableSync(writer, 0)) {
                _status++;

                if (writer) {
                    _status |= WRITER_MASK;
                    _writerThread = Thread.currentThread();
                }

                _persistit.getLockManager().register(this);

                return true;
            } else if (timeout == 0) {
                return false;
            }
            if (Debug.ENABLED) {
                Debug.$assert(checkWaitQueue());
            }

            //
            // We're committed to waiting for the page
            // Set up a WaitingThread object so that the
            // Thread that releases this buffer knows which
            // other Thread to notify.
            //
            wt = allocateWaitingThread();
            wt.setup(this, writer);

            // Link our WaitingThread to the end of
            // the WaitingThread queue.
            //
            enqueue(wt);
            if (Debug.ENABLED)
                Debug.$assert(checkWaitQueue());
        }

        // Perform this wait without synchronization on _lock
        boolean claimed;
        try {
            // Normally the WaitingThread will be awakened by another
            // thread when it performs a release() operation. Normally
            // this call will return a value of true. There is a race
            // condition, however, in the event that the releasing thread is
            // attempting to wake this thread at the same time this thread's
            // mediatedWait operation is timing out. To ensure that there
            // there really is no release operation pending, we need to
            // re-synchronize against _lock and reread the notified flag.
            //
            claimed = wt.mediatedWait(timeout);
        } catch (InterruptedException ie) {
            claimed = false;
        }

        if (!claimed) {
            synchronized (_lock) {
                claimed = wt.isNotified();
                if (!claimed) {
                    // Do this with synchronization against the enqueue()
                    // operation.
                    removeWaitingThread(wt);
                }
            }
            if (!claimed) {
                if (Debug.ENABLED) {
                    Debug.setSuspended(true);
                    System.out.println("*** " + Thread.currentThread()
                            + ": claim failed on " + this + "***");
                    Debug.setSuspended(false);
                    Debug.debug1(true);
                }
            }
        }

        if (claimed) {
            _persistit.getLockManager().register(this);
        }

        releaseWaitingThread(wt);
        return claimed;
    }

    boolean upgradeClaim() throws PersistitException {
        synchronized (_lock) {
            if (isAvailableSync(true, 1)) {
                _status |= WRITER_MASK;
                _writerThread = Thread.currentThread();

                return true;
            } else {
                return false;
            }
        }
    }

    void release() {
        synchronized (_lock) {
            if ((_status & CLAIMED_MASK) == 0) {
                if (Debug.ENABLED) {
                    Debug.debug2(true);
                }
                throw new IllegalStateException(
                        "Release on unclaimed resource " + this);
            }
            _persistit.getLockManager().unregister(this);
            _status--;

            boolean writer = (_status & WRITER_MASK) != 0;
            boolean free = (_status & CLAIMED_MASK) == 0;

            if (writer && free) {
                _status &= ~WRITER_MASK;
                _writerThread = null;
            }

            //
            // Dequeue and wake up each WaitingThread that could now execute
            //
            if (free) {
                if (Debug.ENABLED) {
                    Debug.$assert(_writerThread == null);
                    Debug.$assert(checkWaitQueue());
                }

                while ((_status & WRITER_MASK) == 0) {
                    WaitingThread wt = dequeue((_status & CLAIMED_MASK) == 0);
                    if (wt == null) {
                        break;
                    }
                    boolean exclusive = wt.isExclusive();
                    // Here we post the claim on behalf of the waiting process.
                    _status++;
                    if (exclusive) {
                        _status |= WRITER_MASK;
                        if (Debug.ENABLED) {
                            Debug.$assert(_writerThread == null);
                        }
                        _writerThread = wt.getThread();
                        if (Debug.ENABLED) {
                            Debug.$assert(_writerThread != null);
                        }
                    }
                    wt.wake();
                }
                if (Debug.ENABLED) {
                    Debug.$assert(checkWaitQueue());
                }
            }
        }
    }

    void releaseWriterClaim() {
        synchronized (_lock) {
            if ((_status & CLAIMED_MASK) == 0 || (_status & WRITER_MASK) == 0) {
                if (Debug.ENABLED)
                    Debug.debug2(true);
                throw new IllegalStateException(
                        "Release on unclaimed resource " + this);
            }

            if (Debug.ENABLED)
                Debug.$assert((_status & CLAIMED_MASK) > 0
                        && (_status & WRITER_MASK) != 0);

            if (Debug.ENABLED)
                Debug.$assert(checkWaitQueue());

            _status &= ~WRITER_MASK;
            _writerThread = null;

            while ((_status & WRITER_MASK) == 0) {
                WaitingThread wt = dequeue((_status & CLAIMED_MASK) == 0);
                if (wt == null)
                    break;
                boolean exclusive = wt.isExclusive();
                // Here we post the claim on behalf of the waiting process.
                _status++;
                if (exclusive) {
                    _status |= WRITER_MASK;
                    if (Debug.ENABLED) {
                        Debug.$assert(_writerThread == null);
                    }
                    _writerThread = wt.getThread();
                    if (Debug.ENABLED) {
                        Debug.$assert(_writerThread != null);
                    }
                }
                wt.wake();
            }
            if (Debug.ENABLED)
                Debug.$assert(checkWaitQueue());
        }
    }

    void setClean() {
        synchronized (_lock) {
            _status &= ~(DIRTY_MASK | STRUCTURE_MASK);
        }
    }

    void setDirty() {
        synchronized (_lock) {
            _status |= DIRTY_MASK;
        }
    }

    void setDirtyStructure() {
        synchronized (_lock) {
            _status |= (DIRTY_MASK | STRUCTURE_MASK);
        }
    }

    public long getGeneration() {
        return _generation.get();
    }

    void setTransient(final boolean transientBuffer) {
        synchronized (_lock) {
            if (transientBuffer) {
                _status |= TRANSIENT_MASK;
            } else {
                _status &= ~TRANSIENT_MASK;
            }
        }
    }

    void setValid(boolean valid) {
        synchronized (_lock) {
            if (valid) {
                _status |= VALID_MASK;
            } else {
                _status &= ~VALID_MASK;
            }
        }
    }

    public boolean isAvailable() {
        synchronized (_lock) {
            return (_status & UNAVAILABLE_MASK) == 0;
        }
    }

    void bumpGeneration() {
        _generation.incrementAndGet();
    }

    public int getStatus() {
        synchronized (_lock) {
            return _status;
        }
    }

    public String getStatusCode() {
        final int status;
        synchronized (_lock) {
            status = _status;
        }
        return getStatusCode(status);
    }

    public String getStatusDisplayString() {
        final int status;
        synchronized (_lock) {
            status = _status;
        }
        if (_writerThread == null) {
            return getStatusCode(status);
        } else {
            return getStatusCode(status) + " <" + _writerThread.getName() + ">";
        }
    }

    public static String getStatusCode(int status) {
        // Common cases so we don't have to construct new StringBuilders
        switch (status) {
        case 0:
            return "";
        case VALID_MASK:
            return "v";
        case VALID_MASK | DIRTY_MASK:
            return "vd";
        case VALID_MASK | DIRTY_MASK | ENQUEUED_MASK:
            return "vde";
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
            if ((status & SUSPENDED_MASK) != 0) {
                sb.append("s");
            }
            if ((status & CLOSING_MASK) != 0) {
                sb.append("c");
            }
            if ((status & VALID_MASK) != 0) {
                sb.append("v");
            }
            if ((status & DIRTY_MASK) != 0) {
                sb.append("d");
            }
            if ((status & ENQUEUED_MASK) != 0) {
                sb.append("e");
            }
            if ((status & TRANSIENT_MASK) != 0) {
                sb.append("t");
            }
            if ((status & STRUCTURE_MASK) != 0) {
                sb.append("s");
            }
            if ((status & WRITER_MASK) != 0) {
                sb.append("w");
            }
            if ((status & CLAIMED_MASK) != 0) {
                sb.append("r");
                sb.append(status & CLAIMED_MASK);
            }
            return sb.toString();
        }
    }

    public Thread getWriterThread() {
        synchronized (_lock) {
            return _writerThread;
        }
    }

    @Override
    public String toString() {
        return "SharedResource" + this.hashCode() + " status="
                + getStatusDisplayString();
    }
}
