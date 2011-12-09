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

import java.io.ObjectStreamClass;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;

/**
 * <p>
 * A singleton that associates <code>Class</code>es with persistent handles used
 * to refer to them in Persistit&trade; {@link Value}'s and {@link Key}s. When
 * <code>Value</code> encodes an <code>Object</code>, rather than recording the
 * object's full class name, it stores an integer-valued handle. The handle is
 * associated by the <code>ClassIndex</code> to the class name. This mechanism
 * minimizes the storage of redudant information in the potentially numerous
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
 * 
 * @version 1.1
 */
class ClassIndex {
    private final static int INITIAL_CAPACITY = 123;

    private final static String BY_HANDLE = "byHandle";
    private final static String BY_NAME = "byName";
    final static String CLASS_INDEX_TREE_NAME = "_classIndex";

    private AtomicInteger _size = new AtomicInteger();
    private Persistit _persistit;

    private AtomicReferenceArray<ClassInfoEntry> _hashById = new AtomicReferenceArray<ClassInfoEntry>(INITIAL_CAPACITY);
    private AtomicReferenceArray<ClassInfoEntry> _hashByName = new AtomicReferenceArray<ClassInfoEntry>(
            INITIAL_CAPACITY);
    private ClassInfoEntry _knownNull = null;

    /**
     * A structure holding a ClassInfo, plus links to other related
     * <code>ClassInfoEntry</code>s.
     */
    private static class ClassInfoEntry {
        ClassInfoEntry _nextByIdHash;
        ClassInfoEntry _nextByNameHash;

        ClassInfo _classInfo;

        ClassInfoEntry(ClassInfo ci) {
            _classInfo = ci;
        }
    }

    /**
     * Package-private constructor used only by {@link Persistit} during
     * initialization.
     * 
     * @param persistit
     *            Owning Persistit instance.
     */
    ClassIndex(Persistit persistit) {
        _persistit = persistit;
    }

    /**
     * @return Number of <code>ClassInfo</code> objects currently stored in this
     *         ClassIndex.
     */
    public int size() {
        return _size.get();
    }

    /**
     * Looks up and returns the ClassInfo for an integer handle. This is used
     * when decoding an <code>Object</code> from a
     * <code>com.persistit.Value</code> to associate the encoded integer handle
     * value with the corresponding class.
     * 
     * @param handle
     *            The handle
     * @return The associated ClassInfo, or <i>null</i> if there is none.
     */
    public ClassInfo lookupByHandle(int handle) {
        ClassInfoEntry cie = _hashById.get(handle % _hashById.length());
        while (cie != null) {
            if (cie._classInfo.getHandle() == handle)
                return cie._classInfo;
            cie = cie._nextByIdHash;
        }
        cie = _knownNull;
        while (cie != null) {
            if (cie._classInfo.getHandle() == handle)
                return null;
            cie = cie._nextByIdHash;
        }
        synchronized (this) {
            Exchange ex = null;
            try {
                ex = getExchange();
                Transaction txn = ex.getTransaction();
                txn.begin();
                try {
                    ex.clear().append(BY_HANDLE).append(handle).fetch();
                    txn.commit();
                } catch (Exception e) {
                    _persistit.getLogBase().exception.log(e);
                    throw e;
                } finally {
                    txn.end();
                }
                Value value = ex.getValue();
                if (value.isDefined()) {
                    value.setStreamMode(true);
                    int storedId = value.getInt();
                    String storedName = value.getString();
                    long storedSuid = value.getLong();
                    if (storedId != handle) {
                        throw new IllegalStateException("ClassInfo stored for handle=" + handle
                                + " has invalid stored handle=" + storedId);
                    }
                    Class<?> cl = Class.forName(storedName, false, Thread.currentThread().getContextClassLoader());

                    long suid = 0;
                    ObjectStreamClass osc = ObjectStreamClass.lookup(cl);
                    if (osc != null)
                        suid = osc.getSerialVersionUID();
                    if (storedSuid != suid) {
                        throw new ConversionException("Class " + cl.getName() + " persistent SUID=" + storedSuid
                                + " does not match current class SUID=" + suid);
                    }
                    ClassInfo ci = new ClassInfo(cl, suid, handle, osc);
                    hashClassInfo(ci);
                    return ci;
                } else {
                    ClassInfo ci = new ClassInfo(null, 0, handle, null);
                    cie = _knownNull;
                    _knownNull = new ClassInfoEntry(ci);
                    _knownNull._nextByIdHash = cie;
                }
            } catch (ClassNotFoundException cnfe) {
                throw new ConversionException(cnfe);
            } catch (PersistitException pe) {
                throw new ConversionException(pe);
            } finally {
                if (ex != null)
                    releaseExchange(ex);
            }
            // TODO - lookup in ClassIndex Exchange
            return null;
        }
    }

    /**
     * Looks up and returns a ClassInfo for a class. This is used when encoding
     * an <code>Object</code> into a <code>com.persistit.Value</code>.
     * 
     * @param clazz
     *            The <code>Class</code>
     * @return The ClassInfo for the specified Class.
     */
    public ClassInfo lookupByClass(Class<?> clazz) {

        ObjectStreamClass osc = null;
        long suid = 0;

        int nh = clazz.getName().hashCode() & 0x7FFFFFFF;
        ClassInfoEntry cie = _hashByName.get(nh % _hashById.length());

        while (cie != null) {
            if (cie._classInfo.getDescribedClass().equals(clazz)) {
                return cie._classInfo;
            }
            if (cie._classInfo.getName().equals(clazz.getName())) {
                if (osc == null) {
                    osc = ObjectStreamClass.lookup(clazz);
                    if (osc != null) {
                        suid = osc.getSerialVersionUID();
                    }
                }
                if (suid == cie._classInfo.getSUID()) {
                    return cie._classInfo;
                }
            }
            cie = cie._nextByNameHash;
        }

        synchronized (this) {
            if (osc == null) {
                osc = ObjectStreamClass.lookup(clazz);
            }
            if (osc != null) {
                suid = osc.getSerialVersionUID();
            }
            Exchange ex = null;
            try {
                ex = getExchange();
                final ClassInfo ci;
                final int handle;
                Transaction txn = ex.getTransaction();
                txn.begin();
                ex.clear().append(BY_NAME).append(clazz.getName()).append(suid).fetch();
                Value value = ex.getValue();
                try {
                    if (value.isDefined()) {
                        value.setStreamMode(true);

                        handle = value.getInt();
                        String storedName = value.getString();
                        long storedSuid = value.getLong();

                        if (storedSuid != suid || !clazz.getName().equals(storedName)) {
                            throw new ConversionException("Class " + clazz.getName() + " persistent SUID=" + storedSuid
                                    + " does not match current class SUID=" + suid);
                        }
                        ci = new ClassInfo(clazz, suid, handle, osc);
                    } else {
                        //
                        // Store a new ClassInfo record
                        //
                        ex.clear().append("nextId").fetch();
                        handle = value.isDefined() ? value.getInt() + 1 : 65;
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
            } catch (PersistitException pe) {
                throw new ConversionException(pe);
            } finally {
                if (ex != null)
                    releaseExchange(ex);
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
    public void registerClass(Class<?> clazz) {
        lookupByClass(clazz);
    }

    private void hashClassInfo(ClassInfo ci) {
        int size = _size.incrementAndGet();
        if (size * 2 > _hashById.length()) {
            AtomicReferenceArray<ClassInfoEntry> hashById = new AtomicReferenceArray<ClassInfoEntry>(size * 2);
            AtomicReferenceArray<ClassInfoEntry> hashByName = new AtomicReferenceArray<ClassInfoEntry>(size * 2);
            for (int i = 0; i < _hashById.length(); i++) {
                ClassInfoEntry cie = _hashById.get(i);
                while (cie != null) {
                    addHashEntry(cie._classInfo, hashById, hashByName);
                    cie = cie._nextByIdHash;
                }
            }
            _hashById = hashById;
            _hashByName = hashByName;
        }
        addHashEntry(ci, _hashById, _hashByName);
    }

    private void addHashEntry(final ClassInfo ci, final AtomicReferenceArray<ClassInfoEntry> hashById,
            final AtomicReferenceArray<ClassInfoEntry> hashByName) {
        ClassInfoEntry cie = new ClassInfoEntry(ci);
        int handle = ci.getHandle();
        int nh = ci.getName().hashCode() & 0x7FFFFFFF;

        ClassInfoEntry cie1 = hashById.get(handle % hashById.length());
        ClassInfoEntry cie2 = hashByName.get(nh % hashByName.length());

        cie._nextByIdHash = cie1;
        cie._nextByNameHash = cie2;

        hashById.set(handle % hashById.length(), cie);
        hashByName.set(nh % hashByName.length(), cie);

        ClassInfoEntry cie3 = _knownNull;
        while (cie3 != null) {
            if (cie3._classInfo.getHandle() != handle) {
                _knownNull = cie3._nextByIdHash;
            }
            cie3 = cie3._nextByIdHash;
        }
    }

    private Exchange getExchange() throws PersistitException {
        try {
            Volume volume = _persistit.getSystemVolume();
            return _persistit.getExchange(volume, CLASS_INDEX_TREE_NAME, true);
        } catch (PersistitException pe) {
            throw new ConversionException(pe);
        }
    }

    private void releaseExchange(Exchange ex) {
        _persistit.releaseExchange(ex);
    }

}
