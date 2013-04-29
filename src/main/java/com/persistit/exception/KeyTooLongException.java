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

package com.persistit.exception;

/**
 * Thrown by {@link com.persistit.Key} when an attempt to append a key segment
 * exceeds the maximum size of the key.
 * 
 * @version 1.0
 */
public class KeyTooLongException extends RuntimeException {

    private static final long serialVersionUID = -4657691754665822537L;

    public KeyTooLongException() {
        super();
    }

    public KeyTooLongException(final String msg) {
        super(msg);
    }

    public KeyTooLongException(final Throwable t) {
        super(t);
    }
}
