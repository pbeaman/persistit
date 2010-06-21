/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */
package com.persistit;

/**
 * A minimal HashSet-like collection that allows lookup by just hash code. This
 * lets us find candidate objects in the collection without actually creating a
 * new instance of the object. All objects in this collection must extend the
 * inner class InternalHashSet.Entry.
 * 
 * @author pbeaman
 * @version 1.0
 */
class InternalHashSet {
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

    protected abstract static class Entry {
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

    Entry lookup(int hashCode) {
        return _entries[hashCode & _mask];
    }

    void put(Entry newEntry) {
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

    Entry next(Entry entry) {
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

    int size() {
        return _count;
    }

    void clear() {
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
