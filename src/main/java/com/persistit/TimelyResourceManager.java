/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

/**
 * Manage a collection of {@link TimelyResource} instances as a map.
 * 
 * @author peter
 * 
 * @param <T>
 */
public class TimelyResourceManager {

    private final static int HASH_TABLE_SIZE = 64;

    private final static Entry[] _hashTable = new Entry[HASH_TABLE_SIZE];

    private final static Object[] _locks = new Object[HASH_TABLE_SIZE];
    
    {
        for (int i = 0; i < HASH_TABLE_SIZE; i++) {
            _locks[i] = new Object();
        }
    }

    private final Persistit _persistit;

    private static class Entry {
        private final WeakReference<TimelyResource<? extends PrunableResource>> _ref;
        private Entry _next;

        private Entry(final TimelyResource<? extends PrunableResource> tr) {
            _ref = new WeakReference<TimelyResource<? extends PrunableResource>>(tr);
        }
    }

    TimelyResourceManager(final Persistit persistit) {
        _persistit = persistit;
    }

    public TimelyResource<? extends PrunableResource> timelyResource(final Object key) {
        final int index = key.hashCode() % HASH_TABLE_SIZE;
        synchronized (_locks[index]) {
            final Entry previous = null;
            for (Entry entry = _hashTable[index]; entry != null; entry = entry._next) {
                final TimelyResource<? extends PrunableResource> tr = entry._ref.get();
                if (tr == null) {
                    if (previous == null) {
                        _hashTable[index] = entry._next;
                    } else {
                        previous._next = entry._next;
                    }
                } else if (key.equals(tr.getKey())) {
                    return tr;
                }
            }
            @SuppressWarnings({ "rawtypes", "unchecked" })
            final TimelyResource<? extends PrunableResource> tr = new TimelyResource(_persistit, key);
            final Entry entry = new Entry(tr);
            entry._next = _hashTable[index];
            _hashTable[index] = entry;
            return tr;
        }
    }

    void prune() {
        final List<TimelyResource<? extends PrunableResource>> list = new ArrayList<TimelyResource<? extends PrunableResource>>();
        for (int index = 0; index < HASH_TABLE_SIZE; index++) {
            synchronized (_locks[index]) {
                Entry previous = null;
                for (Entry entry = _hashTable[index]; entry != null; entry = entry._next) {
                    final TimelyResource<? extends PrunableResource> tr = entry._ref.get();
                    if (tr == null) {
                        if (previous == null) {
                            _hashTable[index] = entry._next;
                        } else {
                            previous._next = entry._next;
                        }
                    } else {
                        list.add(tr);
                        previous = entry;
                    }
                }
            }
        }
        for (final TimelyResource<? extends PrunableResource> tr : list) {
            try {
                tr.prune();
            } catch (final Exception e) {
                _persistit.getLogBase().timelyResourcePruneException.log(e, tr);

            }
        }
    }

}
