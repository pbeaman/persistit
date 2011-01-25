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
 * 
 * Created on Jun 10, 2004
 */
package com.persistit.ref;

import java.io.Serializable;

/**
 * <p>
 * Interface for reference classes used to isolate object references during
 * serialization.
 * </p>
 * <p>
 * Any subclass of <tt>java.lang.ref.Reference</tt> implements this interface;
 * thus applications can use this interface, but implement it using a subclasses
 * of <tt>java.lang.ref.WeakReference</tt> (for example).
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
 * A <tt>PersistitReference</tt> may be used to break the serialization
 * relationship of B to A. Instead of having a field of A directly containing an
 * instance of B, the field in A would hold a <tt>PersistitReference</tt> that
 * in turn refers to B. Implementations of PersistitReference are intended to
 * implement it in such a way that only a persistent object identifier for B is
 * stored in the serialized form of the <tt>PersistitReference</tt> rather than
 * a fully serialization of B. The <tt>get</tt> method is intended to
 * deserialize (if necessary) and return the object referred to be that
 * persistent identifier. {@link AbstractReference} provides such an
 * implementation.
 * </p>
 * 
 */
public interface PersistitReference extends Serializable {
    /**
     * @return The <tt>Object</tt> this <tt>Reference</tt> refers to, known as
     *         the <i>referent</i>.
     */
    public Object get();
}
