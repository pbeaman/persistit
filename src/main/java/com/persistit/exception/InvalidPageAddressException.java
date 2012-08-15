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
 * Thrown when Persistit detects an invalid page pointer. This is a catastrophic
 * failure that signifies corruption of a volume file.
 * 
 * @version 1.0
 */
public class InvalidPageAddressException extends PersistitException {
    private static final long serialVersionUID = -2586054997298158793L;

    public InvalidPageAddressException(final String msg) {
        super(msg);
    }

}
