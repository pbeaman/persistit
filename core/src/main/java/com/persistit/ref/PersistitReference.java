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
