/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

import java.io.ObjectStreamClass;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;

/**
 * <p>
 * A singleton that associates <code>Class</code>es with persistent handles used
 * to refer to them in Persistit&trade; {@link Value}'s and {@link Key}s. When
 * <code>Value</code> encodes an <code>Object</code>, rather than recording the
 * object's full class name, it stores an integer-valued handle. The handle is
 * associated by the <code>ClassIndex</code> to the class name. This mechanism
 * minimizes the storage of redundant information in the potentially numerous
 * stored instances of the same class.
 * </p>
 * <p>
 * By default, the persistent storage for this association is located in a tree
 * called <code>"_classIndex"</code> of the
 * {@link com.persistit.Persistit#getSystemVolume system volume}.
 * </p>
 * <p>
 * Note that certain handles for common classes are pre-assigned, and therefore
 * are not translated through this class. See {@link Value} for details.
 * </p>
 * <p>
 * Implementation note: this class implements a specialized hash table in which
 * entries are never removed. Almost always this class returns a value by
 * finding it in the hash table; the lookup is carefully designed to be
 * threadsafe without requiring synchronization. Only in the event of a cache
 * miss is there execution of a synchronized block which may read and/or update
 * the _classIndex tree.
 * </p>
 * 
 * @version 1.1
 */
final class ClassIndex {
    final static int HANDLE_BASE = 64;
    private final static int INITIAL_CAPACITY = 123;

    private final static String BY_HANDLE = "byHandle";
    private final static String BY_NAME = "byName";
    private final static String NEXT_ID = "nextId";
    private final static int EXTRA_FACTOR = 2;

    final static String CLASS_INDEX_TREE_NAME = "_classIndex";

    private final AtomicInteger _size = new AtomicInteger();
    private final Persistit _persistit;
    private final SessionId _sessionId = new SessionId();

    private volatile AtomicReferenceArray<ClassInfoEntry> _hashTable = new AtomicReferenceArray<ClassInfoEntry>(
            INITIAL_CAPACITY);

    private int _testIdFloor = Integer.MIN_VALUE;
    private final AtomicInteger _cacheMisses = new AtomicInteger();
    private final AtomicInteger _discardedDuplicates = new AtomicInteger();

    /**
     * A structure holding a ClassInfo, plus links to other related
     * <code>ClassInfoEntry</code>s.
     */
    private static class ClassInfoEntry {
        final ClassInfoEntry _next;
        final ClassInfo _classInfo;

        ClassInfoEntry(final ClassInfo ci, final ClassInfoEntry next) {
            _classInfo = ci;
            _next = next;
        }
    }

    /**
     * Package-private constructor used only by {@link Persistit} during
     * initialization.
     * 
     * @param persistit
     *            Owning Persistit instance.
     * @throws PersistitException
     */
    ClassIndex(final Persistit persistit) {
        _persistit = persistit;
    }

    void initialize() throws PersistitException {
        /*
         * Called during Persistit initialization. This has the desired
         * side-effect of the class index tree outside of a transaction so that
         * its existence is primordial.
         */
        getExchange();
    }

    /**
     * @return Number of <code>ClassInfo</code> objects currently stored in this
     *         ClassIndex.
     */
    public int size() {
        return _size.get();
    }

    /**
     * Look up and return the ClassInfo for an integer handle. This is used when
     * decoding an <code>Object</code> from a <code>com.persistit.Value</code>
     * to associate the encoded integer handle value with the corresponding
     * class.
     * 
     * @param handle
     *            The handle
     * @return The associated ClassInfo, or <i>null</i> if there is none.
     */
    public ClassInfo lookupByHandle(final int handle) {
        final AtomicReferenceArray<ClassInfoEntry> hashTable = _hashTable;
        ClassInfoEntry cie = hashTable.get(handle % hashTable.length());
        while (cie != null) {
            if (cie._classInfo.getHandle() == handle)
                return cie._classInfo;
            cie = cie._next;
        }

        _cacheMisses.incrementAndGet();

        synchronized (this) {
            _sessionId.assign();
            Exchange ex = null;
            try {
                ex = getExchange();
                final Transaction txn = ex.getTransaction();
                txn.begin();
                try {
                    ex.clear().append(BY_HANDLE).append(handle).fetch();
                    txn.commit();
                } catch (final Exception e) {
                    _persistit.getLogBase().exception.log(e);
                    throw new ConversionException(e);
                } finally {
                    txn.end();
                }
                final Value value = ex.getValue();
                if (value.isDefined()) {
                    value.setStreamMode(true);
                    final int storedId = value.getInt();
                    final String storedName = value.getString();
                    final long storedSuid = value.getLong();
                    if (storedId != handle) {
                        throw new IllegalStateException("ClassInfo stored for handle=" + handle
                                + " has invalid stored handle=" + storedId);
                    }
                    final Class<?> cl = Class
                            .forName(storedName, false, Thread.currentThread().getContextClassLoader());

                    long suid = 0;
                    final ObjectStreamClass osc = ObjectStreamClass.lookupAny(cl);
                    if (osc != null)
                        suid = osc.getSerialVersionUID();
                    if (storedSuid != suid) {
                        throw new ConversionException("Class " + cl.getName() + " persistent SUID=" + storedSuid
                                + " does not match current class SUID=" + suid);
                    }
                    final ClassInfo ci = new ClassInfo(cl, suid, handle, osc);
                    hashClassInfo(ci);
                    return ci;
                } else {
                    final ClassInfo ci = new ClassInfo(null, 0, handle, null);
                    hashClassInfo(ci);
                    return ci;
                }
            } catch (final ClassNotFoundException cnfe) {
                throw new ConversionException(cnfe);
            } catch (final PersistitException pe) {
                throw new ConversionException(pe);
            } finally {
                if (ex != null) {
                    releaseExchange(ex);
                }
            }
        }
    }

    /**
     * Look up and return the ClassInfo for a class. This is used when encoding
     * an <code>Object</code> into a <code>com.persistit.Value</code>.
     * 
     * @param clazz
     *            The <code>Class</code>
     * @return The ClassInfo for the specified Class.
     */
    public ClassInfo lookupByClass(final Class<?> clazz) {
        final AtomicReferenceArray<ClassInfoEntry> hashTable = _hashTable;

        ObjectStreamClass osc = null;
        long suid = 0;

        final int nh = clazz.getName().hashCode() & 0x7FFFFFFF;
        ClassInfoEntry cie = hashTable.get(nh % hashTable.length());

        while (cie != null) {
            if (clazz.equals(cie._classInfo.getDescribedClass())) {
                return cie._classInfo;
            }
            if (cie._classInfo.getDescribedClass() != null && cie._classInfo.getName().equals(clazz.getName())) {
                if (osc == null) {
                    osc = ObjectStreamClass.lookupAny(clazz);
                    if (osc != null) {
                        suid = osc.getSerialVersionUID();
                    }
                }
                if (suid == cie._classInfo.getSUID()) {
                    return cie._classInfo;
                }
            }
            cie = cie._next;
        }

        if (osc == null) {
            osc = ObjectStreamClass.lookupAny(clazz);
        }
        if (osc != null) {
            suid = osc.getSerialVersionUID();
        }

        _cacheMisses.incrementAndGet();

        /**
         * To update the tree, this class uses a unique SessionId and results in
         * using a unique Transaction context unrelated to the application
         * context. Therefore if an application does this:
         * 
         * <pre>
         * <code> 
         * txn.begin(); 
         * value.put(new SomeClass()); 
         * txn.rollback();
         * txn.end(); 
         * </code>
         * </pre>
         * 
         * the class SomeClass will be registered even though the enclosing
         * transaction rolled back. This is important because other concurrent
         * threads may have started using the handle for SomeClass. Therefore
         * this class ensures that a non-nested transaction to insert the new
         * ClassInfo into the system volume has committed before adding the
         * handle to the hash table. </p>
         */

        synchronized (this) {
            final SessionId saveSessionId = _persistit.getSessionId();
            Exchange ex = null;
            try {
                _persistit.setSessionId(_sessionId);
                ex = getExchange();
                final Transaction txn = ex.getTransaction();
                final ClassInfo ci;
                final int handle;
                txn.begin();
                ex.clear().append(BY_NAME).append(clazz.getName()).append(suid).fetch();
                final Value value = ex.getValue();
                try {
                    if (value.isDefined()) {
                        value.setStreamMode(true);

                        handle = value.getInt();
                        final String storedName = value.getString();
                        final long storedSuid = value.getLong();

                        if (storedSuid != suid || !clazz.getName().equals(storedName)) {
                            throw new ConversionException("Class " + clazz.getName() + " persistent SUID=" + storedSuid
                                    + " does not match current class SUID=" + suid);
                        }
                        ci = new ClassInfo(clazz, suid, handle, osc);
                    } else {
                        //
                        // Store a new ClassInfo record
                        //
                        ex.clear().append(NEXT_ID).fetch();
                        handle = Math.max(_testIdFloor, value.isDefined() ? value.getInt() : HANDLE_BASE) + 1;
                        value.clear().put(handle);
                        ex.store();

                        value.clear();
                        value.setStreamMode(true);
                        value.put(handle);
                        value.put(clazz.getName());
                        value.put(suid);

                        ex.clear().append(BY_NAME).append(clazz.getName()).append(suid).store();

                        ex.clear().append(BY_HANDLE).append(handle).store();

                        ci = new ClassInfo(clazz, suid, handle, osc);
                    }
                    txn.commit();
                    hashClassInfo(ci);
                    return ci;
                } finally {
                    txn.end();
                }
            } catch (final PersistitException pe) {
                throw new ConversionException(pe);
            } finally {
                if (ex != null) {
                    releaseExchange(ex);
                }
                _persistit.setSessionId(saveSessionId);
            }
        }
    }

    /**
     * Registers a <code>Class</code>, which binds it permanently with a handle.
     * The order in which classes are first registered governs the order in
     * which <code>Key</code> values containing objects of the classes are
     * sorted. See {@link com.persistit.encoding.CoderManager} for further
     * information.
     * 
     * @param clazz
     *            Class instance to register.
     */
    public void registerClass(final Class<?> clazz) {
        lookupByClass(clazz);
    }

    private void hashClassInfo(final ClassInfo ci) {
        final int size = _size.get();
        if (size * EXTRA_FACTOR > _hashTable.length()) {
            final int discarded = _discardedDuplicates.get();
            final AtomicReferenceArray<ClassInfoEntry> newHashTable = new AtomicReferenceArray<ClassInfoEntry>(
                    EXTRA_FACTOR * 2 * size);
            for (int i = 0; i < _hashTable.length(); i++) {
                ClassInfoEntry cie = _hashTable.get(i);
                while (cie != null) {
                    addHashEntry(newHashTable, cie._classInfo);
                    cie = cie._next;
                }
            }
            _hashTable = newHashTable;
            _discardedDuplicates.set(discarded);
        }
        addHashEntry(_hashTable, ci);
    }

    private void addHashEntry(final AtomicReferenceArray<ClassInfoEntry> hashTable, final ClassInfo ci) {
        final int hh = ci.getHandle() % hashTable.length();
        final int nh = ci.getDescribedClass() == null ? -1
                : ((ci.getDescribedClass().getName().hashCode() & 0x7FFFFFFF) % hashTable.length());
        boolean added = addHashEntry(hashTable, ci, hh);
        if (nh != -1 && nh != hh) {
            added |= addHashEntry(hashTable, ci, nh);
        }
        if (!added) {
            _discardedDuplicates.incrementAndGet();
        }
    }

    private boolean addHashEntry(final AtomicReferenceArray<ClassInfoEntry> hashTable, final ClassInfo ci,
            final int hash) {
        ClassInfoEntry cie = hashTable.get(hash);
        while (cie != null) {
            if (ci.equals(cie._classInfo)) {
                return false;
            }
            cie = cie._next;
        }
        cie = hashTable.get(hash);
        final ClassInfoEntry newCie = new ClassInfoEntry(ci, cie);
        hashTable.set(hash, newCie);
        _size.incrementAndGet();
        return true;
    }

    private Exchange getExchange() throws PersistitException {
        try {
            final Volume volume = _persistit.getSystemVolume();
            return _persistit.getExchange(volume, CLASS_INDEX_TREE_NAME, true);
        } catch (final PersistitException pe) {
            throw new ConversionException(pe);
        }
    }

    private void releaseExchange(final Exchange ex) {
        _persistit.releaseExchange(ex);
    }

    /**
     * For unit tests only. Next class ID handle will be at least as large as
     * this.
     * 
     * @param id
     */
    void setTestIdFloor(final int id) {
        _testIdFloor = id;
    }

    /**
     * For unit tests only. Clears all entries.
     * 
     * @throws PersistitException
     */
    void clearAllEntries() throws PersistitException {
        getExchange().removeAll();
        _cacheMisses.set(0);
        _discardedDuplicates.set(0);
    }

    /**
     * For unit tests only.
     * 
     * @return count (since beginning or last call to {@link #clearAllEntries()}
     *         ) of cache misses.
     */
    int getCacheMisses() {
        return _cacheMisses.get();
    }

    /**
     * For unit tests only.
     * 
     * @return count (since beginning or last call to {@link #clearAllEntries()}
     *         ) of discarded duplicates.
     */
    int getDiscardedDuplicates() {
        return _discardedDuplicates.get();
    }
}
