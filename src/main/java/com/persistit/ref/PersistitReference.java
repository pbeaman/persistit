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
