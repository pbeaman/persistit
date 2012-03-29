/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Identity key for a session. Normally there is one session per thread, but
 * applications that need to maintain session context across multiple network
 * requests serviced on different threads can access and carefully modify their
 * session contexts using SessionID. See {@link Persistit#getSessionId()} and
 * {@link Persistit#setSessionId(SessionId)}.
 * <p />
 * A SessionId instance hold a reference to the <code>Thread</code> currently
 * associated with it; initially this this the thread that created the
 * SessionId. The <code>setSessionId</code> method reassigns the thread field.
 * The {@link #isAlive()} method indicates whether the associated thread is
 * alive. The {@link Persistit#cleanup()} method cleans up all transaction
 * resources for <code>SessionId</code> instances whose threads are no longer
 * alive.
 * 
 * @author peter
 * 
 */
public class SessionId {

    private final static AtomicInteger counter = new AtomicInteger(1);

    private final int _id = counter.getAndIncrement();

    private AtomicReference<Thread> _owner = new AtomicReference<Thread>();

    SessionId() {
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

}
