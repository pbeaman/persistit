/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.ref;

/**
 * An abstract superclass for implementations of {@link PersistitReference}. A
 * concrete subclass implements the method: <blockquote>
 * 
 * <pre>
 *      public Object lookup(Object id)
 * </pre>
 * 
 * </blockquote> which should return the Object associated with the supplied id,
 * or <code>null</code> if there is none. The meaning of the identifier, and the
 * mechanism used to look up and deserialize the associated object (the
 * referent), are implementation-specific.
 * 
 * @version 1.0
 */
public abstract class AbstractReference implements PersistitReference {
    protected Object _id;
    protected boolean _knownNull;
    protected transient Object _referent;

    /**
     * No-arg constructor supplied for object serialization/deserialization.
     */
    protected AbstractReference() {
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
    protected AbstractReference(final Object id, final Object referent) {
        _id = id;
        _referent = referent;
        _knownNull = referent == null;
    }

    /**
     * Construct a reference using the persistent identity of an object. A
     * subsequent invocation of the <code>get</code> method will cause the
     * object to be looked up and instantiated.
     * 
     * @param id
     */
    protected AbstractReference(final Object id) {
        _id = id;
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
        if (_referent != null)
            return _referent;
        if (_knownNull)
            return null;
        _referent = lookup(_id);
        if (_referent == null)
            _knownNull = true;
        return _referent;
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
