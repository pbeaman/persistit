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
 * Thrown by {@link com.persistit.Transaction} when
 * {@link com.persistit.Transaction#commit commit} fails, or when
 * {@link com.persistit.Transaction#rollback rollback} is invoked.
 * 
 * @version 1.0
 */
public class RollbackException extends RuntimeException {
    private static final long serialVersionUID = 4146025543886888181L;

    public RollbackException() {
        super();
    }

    public RollbackException(String msg) {
        super(msg);
    }

    public RollbackException(Throwable t) {
        super(t);
    }
}
