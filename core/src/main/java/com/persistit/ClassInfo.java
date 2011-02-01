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
 * Created on Jun 11, 2004
 */
package com.persistit;

import java.io.ObjectStreamClass;

/**
 * A simple structure that holds a tuple consiting of a class, its serial
 * version UID and its Persistit handle.
 * 
 */

public class ClassInfo {
    private Class _class;
    private long _suid;
    private int _handle;
    private ObjectStreamClass _osc;

    ClassInfo(Class cl, long suid, int handle, ObjectStreamClass osc) {
        _class = cl;
        _suid = suid;
        _handle = handle;
        _osc = osc;
    }

    /**
     * @return The <tt>Class</tt>
     */
    public Class getDescribedClass() {
        return _class;
    }

    /**
     * @return The <tt>Class</tt>'s name
     */
    public String getName() {
        return _class.getName();
    }

    /**
     * @return The serial version UID of the <tt>Class</tt>
     */
    public long getSUID() {
        return _suid;
    }

    /**
     * @return The handle used to identify the <tt>Class</tt> in values stored
     *         by Persistit.
     */
    public int getHandle() {
        return _handle;
    }

    /**
     * @return The ObjectStreamClass for the described class, or <tt>null</tt>
     *         if there is none.
     */
    public ObjectStreamClass getClassDescriptor() {
        return _osc;
    }
}