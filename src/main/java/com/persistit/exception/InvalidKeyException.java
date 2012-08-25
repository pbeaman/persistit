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
 * Thrown by {@link com.persistit.Exchange} on an attempt to store an invalid
 * {@link com.persistit.Key}.
 * 
 * @version 1.0
 */
public class InvalidKeyException extends PersistitException {
    private static final long serialVersionUID = -2379023919957561093L;

    public InvalidKeyException() {
        super();
    }

    public InvalidKeyException(final String msg) {
        super(msg);
    }

}
