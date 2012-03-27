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

package com.persistit.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.persistit.Buffer;

/**
 * Tracks use of {@link Buffer} by thread. Normally this code is unused and
 * should eventually be removed. Here now for debugging purposes when needed.
 * 
 * @author peter
 * 
 */
public class LockManager {
    final static int MAX_INDEX = 10;

    final Map<Thread, Holder[]> _map = new ConcurrentHashMap<Thread, Holder[]>();

    static class Holder {
        volatile int _readers;
        volatile boolean _writer;
    }

    public void registerClaim(final Buffer buffer, final boolean writer) {
        if (buffer.getIndex() < MAX_INDEX) {
            Thread t = Thread.currentThread();
            int p = (int) buffer.getIndex();
            Holder[] holders = _map.get(t);
            if (holders == null) {
                holders = new Holder[MAX_INDEX];
                for (int q = 0; q < MAX_INDEX; q++) {
                    holders[q] = new Holder();
                }
                _map.put(t, holders);
            }
            Holder h = holders[p];
            h._readers++;
            h._writer |= writer;
            Debug.$assert1.t(h._readers < 100 && h._readers >= 0);
        }
    }

    public void unregisterClaim(final Buffer buffer) {
        if (buffer.getIndex() < MAX_INDEX) {
            Thread t = Thread.currentThread();
            int p = (int) buffer.getIndex();
            Holder[] holders = _map.get(t);
            Debug.$assert1.t(holders != null);
            Holder h = holders[p];
            h._readers--;
            h._writer = false;
            Debug.$assert1.t(h._readers < 100 && h._readers >= 0);
        }
    }

    public void registerUpgrade(final Buffer buffer) {
        if (buffer.getIndex() < MAX_INDEX) {
            Thread t = Thread.currentThread();
            int p = (int) buffer.getIndex();
            Holder[] holders = _map.get(t);
            Debug.$assert1.t(holders != null);
            Holder h = holders[p];
            h._writer = true;
            Debug.$assert1.t(h._readers < 100 && h._readers >= 0);
        }
    }

    public void registerDowngrade(final Buffer buffer) {
        if (buffer.getIndex() < MAX_INDEX) {
            Thread t = Thread.currentThread();
            int p = (int) buffer.getIndex();
            Holder[] holders = _map.get(t);
            Debug.$assert1.t(holders != null);
            Holder h = holders[p];
            h._writer = false;
            Debug.$assert1.t(h._readers < 100 && h._readers >= 0);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int p = 0; p < MAX_INDEX; p++) {
            boolean added = false;
            for (final Map.Entry<Thread, Holder[]> entry : _map.entrySet()) {
                Thread t = entry.getKey();
                Holder h = entry.getValue()[p];
                if (h._readers != 0 || h._writer) {
                    if (!added) {
                        sb.append(String.format("%4d: ", p));
                    }
                    added = true;
                    sb.append(" " + t.getName() + " <" + h._readers + (h._writer ? "*" : "") + ">");
                }
            }
            if (added) {
                sb.append(Util.NEW_LINE);
            }
        }
        return sb.toString();
    }

    public void verify() {
        Thread t = Thread.currentThread();
        Holder[] holders = _map.get(t);
        for (int p = 0; p < MAX_INDEX; p++) {
            Holder h = holders[p];
            Debug.$assert1.t(h._readers == 0 && !h._writer);
        }
    }
}
