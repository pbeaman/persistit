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

package com.persistit.ref;

import java.lang.ref.WeakReference;

/**
 * <p>
 * An abstract superclass for implementations of {@link PersistitReference} that
 * implement weak reference semantics. A concrete subclass implements the
 * method: <blockquote>
 * 
 * <pre>
 *      public Object lookup(Object id)
 * </pre>
 * 
 * </blockquote> which should return the Object associated with the supplied id,
 * or <code>null</code> if there is none. The meaning of the identifier, and the
 * mechanism used to look up and deserialize the associated object (the
 * referent), are implementation-specific.
 * </p>
 * <p>
 * This implementation differs from {@link AbstractReference} in that the
 * referent object is referenced through a
 * <code>java.lang.ref.WeakReference</code>. Thus the garbage collector may
 * choose to discard the referent value, in which case the <code>get</code>
 * method will simply look it up again on demand.
 * </p>
 * 
 * @version 1.0
 */
public abstract class AbstractWeakReference implements PersistitReference {
    protected Object _id;
    protected transient WeakReference _weakReference;
    protected boolean _knownNull;

    /**
     * No-arg constructor supplied for object serialization/deserialization.
     * 
     */
    protected AbstractWeakReference() {
    }

    /**
     * Construct a reference to the referent Object with the supplied persistent
     * identifier. For correct operation, the referent Object must be equivalent
     * to the object that would result from invoking <code>lookup</code> on the
     * persistent identifier. the object that would be returned by the lookup
     * 
     * @param id
     *            The persistent identifier. The value of the id must be
     *            associated with a unique referent object, and must be stable
     *            over time.
     * @param referent
     *            The object identified by the id
     */
    protected AbstractWeakReference(Object id, Object referent) {
        _id = id;
        if (referent == null)
            _knownNull = true;
        else
            _weakReference = new WeakReference(referent);
    }

    /**
     * Construct a reference using the persistent identity of an object. A
     * subsequent invocation of the <code>get</code> method will cause the
     * object to be looked up and instantiated.
     * 
     * @param id
     */
    protected AbstractWeakReference(Object id) {
        _id = id;
        _weakReference = null;
        _knownNull = false;
    }

    /**
     * Gets the referent object. If the object has already been looked up, or if
     * this reference was created using the two-argument constructor, then this
     * merely returns the object. Otherwise this method attempts to look up and
     * instantiate the object using its persistent identifier.
     * 
     * @return The referent object.
     */
    @Override
    public Object get() {
        if (_id == null) {
            throw new IllegalStateException("identifier not initialized");
        }
        if (_knownNull)
            return null;
        Object referent = null;
        if (_weakReference != null)
            referent = _weakReference.get();
        if (referent == null) {
            referent = lookup(_id);
            if (referent == null)
                _knownNull = true;
            else
                _weakReference = new WeakReference(referent);
        }
        return _weakReference;
    }

    /**
     * Look up and instantiate an object using its persistent identifier.
     * Typically this will be done by setting up a
     * {@link com.persistit.Exchange} and fetching its value.
     * 
     * @param id
     *            The identifier
     * @return The referent object.
     */
    protected abstract Object lookup(Object id);

}
