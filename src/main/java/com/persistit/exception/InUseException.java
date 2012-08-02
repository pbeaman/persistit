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
 * Thrown when a Persistit database operation fails to establish a claim on a
 * resource within a reasonable length of time. The timeout is a property of the
 * {@link com.persistit.Exchange} that initiated the failed operation.
 * 
 * @version 1.0
 */
public class InUseException extends PersistitException {
    private static final long serialVersionUID = -4898002482348605103L;

    public InUseException(String msg) {
        super(msg);
    }
}
