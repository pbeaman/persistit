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

package com.persistit;

import java.io.ObjectStreamClass;

/**
 * A simple structure that holds a tuple consisting of a class, its serial
 * version UID and its Persistit handle.
 * 
 */

class ClassInfo {
    private Class<?> _class;
    private long _suid;
    private int _handle;
    private ObjectStreamClass _osc;

    ClassInfo(Class<?> cl, long suid, int handle, ObjectStreamClass osc) {
        _class = cl;
        _suid = suid;
        _handle = handle;
        _osc = osc;
    }

    public boolean equals(final Object object) {
        if (object instanceof ClassInfo) {
            ClassInfo ci = (ClassInfo) object;
            if (_class.equals(ci._class) && _handle == ci._handle && _suid == ci._suid) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return _class.hashCode() ^ _handle ^ (int) _suid;
    }

    /**
     * @return The <code>Class</code>
     */
    public Class<?> getDescribedClass() {
        return _class;
    }

    /**
     * @return The <code>Class</code>'s name
     */
    public String getName() {
        return _class.getName();
    }

    /**
     * @return The serial version UID of the <code>Class</code>
     */
    public long getSUID() {
        return _suid;
    }

    /**
     * @return The handle used to identify the <code>Class</code> in values
     *         stored by Persistit.
     */
    public int getHandle() {
        return _handle;
    }

    /**
     * @return The ObjectStreamClass for the described class, or
     *         <code>null</code> if there is none.
     */
    public ObjectStreamClass getClassDescriptor() {
        return _osc;
    }
}