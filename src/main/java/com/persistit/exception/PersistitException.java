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
 * The superclass for all checked Persistit&trade; Exceptions. This class can
 * also serve as a wrapper for other Exception types.
 * 
 * @version 1.0
 */
public class PersistitException extends Exception {

    private static final long serialVersionUID = -2971539608220570084L;

    protected PersistitException() {
        super();
    }

    protected PersistitException(String msg) {
        super(msg);
    }

    public PersistitException(Throwable t) {
        super(t);
    }

    public PersistitException(String msg, Throwable t) {
        super(msg, t);
    }
}
