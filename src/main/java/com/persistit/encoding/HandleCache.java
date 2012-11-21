/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.encoding;

/**
 * <p>
 * Interface for an object that can receive and supply an int-valued handle. The
 * implementation should return 0 if no handle has been set, a non-zero value if
 * the handle has been set, and should throw an IllegalStateException if an
 * attempt is made to change the handle from one non-zero value to another.
 * </p>
 * 
 * @version 1.0
 */
public interface HandleCache {

    /**
     * Store a supplied handle.
     * 
     * @param handle
     * @throws IllegalStateException
     *             if there already is a non-matching non-zero handle stored
     */
    void setHandle(final int handle);

    /**
     * @return the stored handle value
     */
    int getHandle();
}
