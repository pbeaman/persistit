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

/**
 * Base class for resources that are contended for by client Threads. Note: the
 * calling methods must provide synchronization.
 * <p>
 * This class implements a FIFO queue of Threads waiting for a resource.
 */
class WaitingThreadManager {
    /**
     * First WaitingThread to awaken. Head of FIFO queue.
     */
    private WaitingThread _waitingHead = null;
    /**
     * Last WaitingThread added to queue. To add a new WaitingThread wt, link
     * _waitingTail to it and then reset _waitingTail to wt.
     */
    private WaitingThread _waitingTail = null;

    protected final Persistit _persistit;

    WaitingThreadManager(final Persistit persistit) {
        _persistit = persistit;
    }

    protected WaitingThread allocateWaitingThread() {
        WaitingThread wt;

        if (_persistit != null) {
            ThreadLocal wttl = _persistit.getWaitingThreadThreadLocal();
            wt = (WaitingThread) wttl.get();
            if (wt == null) {
                wt = new WaitingThread(_persistit);
                wttl.set(wt);
            }
            return wt;
        }
        return new WaitingThread(_persistit);
    }

    /**
     * Relinquish a WaitingThread. Using ThreadLocals, this does nothing.
     * 
     * @param wt
     */
    protected void releaseWaitingThread(WaitingThread wt) {
    }

    /**
     * Diagnostic routine to check queue integrity
     * 
     * @return <tt>true</tt> if the wait queue is ok
     */
    protected boolean checkWaitQueue() {
        WaitingThread wt1 = _waitingHead;
        if (wt1 == null) {
            return _waitingTail == null;
        }

        for (int count = 1000; count-- > 0 && wt1 != null;) {
            WaitingThread wt2 = wt1._next;
            if ((wt2 == null) != (wt1 == _waitingTail))
                return false;
            wt1 = wt2;
        }

        return wt1 == null;
    }

    protected void enqueue(WaitingThread wt) {
        if (Debug.ENABLED)
            Debug.$assert(wt._next == null);

        if (_waitingTail == null) {
            _waitingHead = wt;
            _waitingTail = wt;
        } else {
            _waitingTail._next = wt;
            _waitingTail = wt;
        }
    }

    /**
     * Wakes up all of the enqueued <tt>WaitingThread</tt>s.
     */
    protected void wakeAll() {
        WaitingThread wt = dequeue(true);
        while (wt != null) {
            wt.wake();
            wt = dequeue(true);
        }
    }

    /**
     * @return The first WaitingThread on the queue, without dequeuing it
     */
    protected WaitingThread head() {
        return _waitingHead;
    }

    /**
     * @return <tt>True</tt> if there is a waiting writer thread
     */
    protected boolean isWriterWaiting() {
        WaitingThread wt = _waitingHead;
        while (wt != null) {
            if (wt.isExclusive()) {
                return true;
            }
            wt = wt._next;
        }
        return false;
    }

    /**
     * Removes and returns the first WaitingThread on the queue, depending on
     * whether that WaitingThread is needs exclusive access, and whether the
     * caller can grant exclusive access.
     * 
     * @param exclusive
     *            Determines whether this method will dequeue a Waiting Thread
     *            that requires exclusive (WRITER) access. If this parameter is
     *            <i>false</i> then only <tt>WaitingThread</tt>s that are not
     *            waiting for
     * @return The next WaitingThread on the queue, or <i>null</i> if there is
     *         none, or if the next WaitingThread needs exclusivity and the
     *         <tt>exclusive</tt> is false.
     */
    protected WaitingThread dequeue(boolean exclusive) {
        WaitingThread wt = _waitingHead;
        if (wt != null) {
            if (!exclusive && wt._writer)
                return null;
            _waitingHead = wt._next;
            if (_waitingHead == null) {
                _waitingTail = null;
            }
            wt._next = null;
        }
        return wt;
    }

    /**
     * Remove a WaitingThread object from the queue. This should get called only
     * if we believe the WaitingThread never received a call to wake() - i.e.,
     * it timed out.
     * 
     * @param wt
     *            The WaitingThread to remove.
     */

    protected boolean removeWaitingThread(WaitingThread wt) {
        if (_waitingHead == wt) {
            _waitingHead = wt._next;
            if (_waitingTail == wt) {
                _waitingTail = _waitingHead;
                return true;
            }
        } else {
            WaitingThread wt1 = _waitingHead;
            while (wt1 != null) {
                WaitingThread wt2 = wt1._next;
                if (wt2 == wt) {
                    wt1._next = wt._next;
                    wt._next = null;
                    if (_waitingTail == wt)
                        _waitingTail = wt1._next;
                    if (_waitingTail == null)
                        _waitingTail = wt1;
                    return true;
                } else
                    wt1 = wt2;
            }
        }
        return false;
    }

    protected static class WaitingThread {
        private final Persistit _persistit;
        /**
         * The Thread that owns this WaitingThread
         */
        private final Thread _thread;
        /**
         * Set at by another thread when notifying this WaitingThread that it
         * should wake up. If this flag gets set before this WaitingThread
         * begins its wait, then {#link mediatedWait} returns immediately
         * without waiting.
         */
        private boolean _notified = false;
        /**
         * Indicates whether the waiting Thread needs an exclusive claim.
         */
        private boolean _writer;
        /**
         * The time at which the Thread started waiting.
         */
        private Object _resource;
        /**
         * Singly-linked list of WaitingThreads. These are consumed in FIFO
         * order so that we don't get starvation.
         */
        private WaitingThread _next = null;
        /**
         * The Thread that woke this one up last
         */
        private Thread _wokenBy = null;

        /**
         * Private constructor. Only the allocate method can construct a
         * WaitingThread.
         */
        private WaitingThread(final Persistit persistit) {
            _persistit = persistit;
            _thread = Thread.currentThread();
        }

        /**
         * @return <i>true</i> if this WaitingThread needs exclusive access to
         *         the resource.
         */
        protected boolean isExclusive() {
            return _writer;
        }

        /**
         * @return The resource for which this WaitingThread is waiting
         */
        protected Object getResource() {
            return _resource;
        }

        /**
         * @return The <tt>Thread</tt> that is waiting
         */
        protected Thread getThread() {
            return _thread;
        }

        @Override
        public String toString() {
            return "WaitingThread for " + _thread + " on resource " + _resource
                    + " " + (_writer ? "W" : "R") + " notified=" + _notified
                    + " wokenBy=" + _wokenBy;
        }

        /**
         * Waits for another Thread to call wake(), or for timeout expiration
         * 
         * @param timeout
         * @return <i>true</i> if another thread called wake, or <i>false</i> if
         *         the timeout expired.
         * @throws InterruptedException
         */
        protected synchronized boolean mediatedWait(long timeout)
                throws InterruptedException {
            _wokenBy = null;
            while (!_notified && timeout > 0) {
                wait(timeout < 2000 ? timeout : 2000);
                timeout -= 2000;
            }
            _resource = null; // avoid memory leak
            _wokenBy = null; // avoid memory leak
            return _notified; // return false if timed out
        }

        /**
         * Wakes up a WaitingThread. If this method is called before the
         * WaitingThread actually starts waiting, the WaitingThread will not
         * wait - its call to mediatedWait will return immediately.
         */
        protected synchronized void wake() {
            _wokenBy = Thread.currentThread();
            _notified = true;
            notify();
        }

        /**
         * Initialize this WaitingThread
         * 
         * @param resource
         *            The awaited resource
         * @param writer
         *            <i>true</i> if the waiting Thread needs exclusive access
         *            to the resource.
         */
        protected void setup(Object resource, boolean writer) {
            _resource = resource;
            _writer = writer;
            _notified = false;
            _next = null;
        }

        protected synchronized boolean isNotified() {
            return _notified;
        }
    }

}
