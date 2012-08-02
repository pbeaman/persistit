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

import java.io.Serializable;

/**
 * <p>
 * Interface for reference classes used to isolate object references during
 * serialization.
 * </p>
 * <p>
 * Any subclass of <code>java.lang.ref.Reference</code> implements this
 * interface; thus applications can use this interface, but implement it using a
 * subclasses of <code>java.lang.ref.WeakReference</code> (for example).
 * </p>
 * <p>
 * The general problem this interface is intended to solve is to control and
 * reduce the scope of serialization of an object. In a typical application some
 * object A may need to refer to some other object B that has a large graph of
 * connected objects. Using Java's standard serialization mechanism, serializing
 * A also serializes B and all the objects it refers to. This set may be very
 * large, and further, it may be semantically incorrect to serialize the state
 * of B with A because the state of B may change independently of A.
 * </p>
 * <p>
 * A <code>PersistitReference</code> may be used to break the serialization
 * relationship of B to A. Instead of having a field of A directly containing an
 * instance of B, the field in A would hold a <code>PersistitReference</code>
 * that in turn refers to B. Implementations of PersistitReference are intended
 * to implement it in such a way that only a persistent object identifier for B
 * is stored in the serialized form of the <code>PersistitReference</code>
 * rather than a fully serialization of B. The <code>get</code> method is
 * intended to deserialize (if necessary) and return the object referred to be
 * that persistent identifier. {@link AbstractReference} provides such an
 * implementation.
 * </p>
 * 
 */
public interface PersistitReference extends Serializable {
    /**
     * @return The <code>Object</code> this <code>Reference</code> refers to,
     *         known as the <i>referent</i>.
     */
    public Object get();
}
