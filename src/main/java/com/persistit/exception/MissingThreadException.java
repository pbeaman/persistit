/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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
 * Thrown by tests that intentionally stop background threads.
 * 
 * @version 1.0
 */
public class MissingThreadException extends PersistitException {

    private static final long serialVersionUID = -8988155339592216974L;

    protected MissingThreadException() {
        super();
    }

    public MissingThreadException(String msg) {
        super(msg);
    }

}
