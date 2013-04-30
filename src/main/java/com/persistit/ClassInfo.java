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

/**
 * A simple structure that holds a tuple consisting of a class, its serial
 * version UID and its Persistit handle.
 * 
 */

class ClassInfo {
    private final Class<?> _class;
    private final long _suid;
    private final int _handle;
    private final ObjectStreamClass _osc;

    ClassInfo(final Class<?> cl, final long suid, final int handle, final ObjectStreamClass osc) {
        _class = cl;
        _suid = suid;
        _handle = handle;
        _osc = osc;
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof ClassInfo) {
            final ClassInfo ci = (ClassInfo) object;
            if (equals(_class, ci._class) && _handle == ci._handle && _suid == ci._suid) {
                return true;
            }
        }
        return false;
    }

    @Override
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

    private boolean equals(final Class<?> a, final Class<?> b) {
        return a == null ? b == null : a.equals(b);
    }
}
