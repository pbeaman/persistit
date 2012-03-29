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

/**
 * A minimal HashSet-like collection that allows lookup by just hash code. This
 * lets us find candidate objects in the collection without actually creating a
 * new instance of the object. All objects in this collection must extend the
 * inner class InternalHashSet.Entry.
 * 
 * @author pbeaman
 * @version 1.0
 */
public class InternalHashSet {
    //
    // Must always be power of 2
    //
    private final static int INITIAL_SIZE = 64; // TODO
    //
    // If size is greater than this on clear operation, then reduce to
    // this size.
    //
    private final static int TRIM_SIZE = 64; // TODO
    //
    // Must always be _headEntries.length - 1;
    //
    private int _mask = INITIAL_SIZE - 1;
    //
    // Count of entries
    //
    private int _count;
    //
    // Length must be power of two
    //
    private Entry[] _entries = new Entry[INITIAL_SIZE];

    public abstract static class Entry {
        private Entry _next;
        private int _hashCode = -1;

        public Entry getNext() {
            return _next;
        }

        private int getHashCode() {
            if (_hashCode == -1)
                _hashCode = hashCode() & 0x7FFFFFFF;
            return _hashCode;
        }

        @Override
        public abstract int hashCode();
    }

    public Entry lookup(int hashCode) {
        return _entries[hashCode & _mask];
    }

    public void put(Entry newEntry) {
        int index = newEntry.getHashCode() & _mask;
        Entry entry = _entries[index];
        newEntry._next = entry;
        _entries[index] = newEntry;
        _count++;
        if (_count > _entries.length / 2) {
            grow();
        }
    }

    void grow() {
        Entry[] temp = new Entry[_entries.length * 2];
        _mask = temp.length - 1;
        for (int index = 0; index < _entries.length; index++) {
            Entry entry = _entries[index];
            while (entry != null) {
                int newIndex = entry.getHashCode() & _mask;
                Entry newEntry = entry;

                entry = entry._next;
                newEntry._next = temp[newIndex];
                temp[newIndex] = newEntry;
            }
        }
        _entries = temp;
    }

    public Entry next(Entry entry) {
        if (entry != null && entry._next != null) {
            return entry._next;
        } else {
            int index = -1;
            if (entry != null) {
                index = (entry.getHashCode() & 0x7FFFFFFF) % _entries.length;
            }
            while (++index < _entries.length) {
                if (_entries[index] != null)
                    return _entries[index];
            }
            return null;
        }
    }

    public int size() {
        return _count;
    }

    public void clear() {
        if (_count == 0) {
            return;
        }
        if (_entries.length > TRIM_SIZE) {
            _entries = new Entry[TRIM_SIZE];
        } else {
            for (int index = 0; index < _entries.length; index++) {
                _entries[index] = null;
            }
        }
        _count = 0;
        _mask = _entries.length - 1;
    }
}
