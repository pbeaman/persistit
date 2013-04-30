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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * Identity key for a session. Normally there is one session per thread, but
 * applications that need to maintain session context across multiple network
 * requests serviced on different threads can access and carefully modify their
 * session contexts using SessionID. See {@link Persistit#getSessionId()} and
 * {@link Persistit#setSessionId(SessionId)}.
 * </p>
 * <p>
 * A SessionId instance holds a reference to the <code>Thread</code> currently
 * associated with it; initially this this the thread that created the
 * SessionId. The <code>setSessionId</code> method reassigns the thread field.
 * The {@link #isAlive()} method indicates whether the associated thread is
 * alive. The {@link Persistit#cleanup()} method cleans up all transaction
 * resources for <code>SessionId</code> instances whose threads are no longer
 * alive.
 * </p>
 * <p>
 * A session is used to maintain state, including the {@link Transaction}
 * context and map of cached {@link Exchange} maintained by
 * {@link Persistit#getExchange(String, String, boolean)}. Therefore care must
 * be taken to limit the maximum number of <code>SessionId</code> instances
 * created during the lifetime of a <code>Persistit</code> instance and to
 * manage them appropriately.
 * </p>
 * 
 * @author peter
 * 
 */
public class SessionId {

    private final static AtomicInteger counter = new AtomicInteger(1);

    private final int _id = counter.getAndIncrement();

    private final AtomicReference<Thread> _owner = new AtomicReference<Thread>();

    public SessionId() {
        assign();
    }

    @Override
    public boolean equals(final Object id) {
        if (id == null || !(id instanceof SessionId)) {
            return false;
        }
        return this._id == ((SessionId) id)._id;
    }

    @Override
    public int hashCode() {
        return _id;
    }

    /**
     * @return Status of the associated {@link Thread}.
     */
    public boolean isAlive() {
        return _owner.get().isAlive();
    }

    @Override
    public String toString() {
        return "[" + _id + (!isAlive() ? "*]" : "]");
    }

    void assign() {
        _owner.set(Thread.currentThread());
    }

    boolean interrupt() {
        final Thread t = _owner.get();
        if (t != null && t != Thread.currentThread()) {
            t.interrupt();
            return true;
        } else {
            return false;
        }
    }

    public String ownerName() {
        final Thread t = _owner.get();
        if (t == null) {
            return "null";
        } else {
            return t.getName();
        }
    }

}
