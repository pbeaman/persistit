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
 * Thrown by decoding methods of the {@link com.persistit.Value} class when the
 * serialized byte array is corrupt. This is a catastrophic failure that
 * signifies external volume file corruption.
 * 
 * @version 1.0
 */
public class MalformedValueException extends RuntimeException {
    private static final long serialVersionUID = 5868710861424952291L;

    public MalformedValueException() {
        super();
    }

    public MalformedValueException(final String msg) {
        super(msg);
    }
}
