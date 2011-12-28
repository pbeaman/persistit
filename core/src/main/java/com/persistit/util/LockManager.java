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
package com.persistit.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.persistit.Buffer;

public class LockManager {
    final static int MAX_PAGE_ADDRESS = 100;

    final Map<Thread, Holder[]> _map = new ConcurrentHashMap<Thread, Holder[]>();

    static class Holder {
        volatile int _readers;
        volatile boolean _writer;
    }

    public void registerClaim(final Buffer buffer, final boolean writer) {
        if (buffer.getPageAddress() < MAX_PAGE_ADDRESS) {
            Thread t = Thread.currentThread();
            int p = (int) buffer.getPageAddress();
            Holder[] holders = _map.get(t);
            if (holders == null) {
                holders = new Holder[MAX_PAGE_ADDRESS];
                for (int q = 0; q < MAX_PAGE_ADDRESS; q++) {
                    holders[q] = new Holder();
                }
                _map.put(t, holders);
            }
            Holder h = holders[p];
            h._readers++;
            h._writer |= writer;
        }
    }

    public void unregisterClaim(final Buffer buffer) {
        if (buffer.getPageAddress() < MAX_PAGE_ADDRESS) {
            Thread t = Thread.currentThread();
            int p = (int) buffer.getPageAddress();
            Holder[] holders = _map.get(t);
            Debug.$assert1.t(holders != null);
            Holder h = holders[p];
            h._readers--;
            h._writer = false;
        }
    }

    public void registerUpgrade(final Buffer buffer) {
        if (buffer.getPageAddress() < MAX_PAGE_ADDRESS) {
            Thread t = Thread.currentThread();
            int p = (int) buffer.getPageAddress();
            Holder[] holders = _map.get(t);
            Debug.$assert1.t(holders != null);
            Holder h = holders[p];
            h._writer = true;
        }
    }

    public void registerDownrade(final Buffer buffer) {
        if (buffer.getPageAddress() < MAX_PAGE_ADDRESS) {
            Thread t = Thread.currentThread();
            int p = (int) buffer.getPageAddress();
            Holder[] holders = _map.get(t);
            Debug.$assert1.t(holders != null);
            Holder h = holders[p];
            h._writer = false;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int p = 0; p < MAX_PAGE_ADDRESS; p++) {
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
                if (added) {
                    sb.append(Util.NEW_LINE);
                }
            }
        }
        return sb.toString();
    }
}
