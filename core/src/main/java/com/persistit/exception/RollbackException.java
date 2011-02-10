/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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

    Throwable _cause;

    public RollbackException() {
        super();
    }

    public RollbackException(String msg) {
        super(msg);
    }

    public RollbackException(Throwable t) {
        _cause = t;
    }

    /**
     * Provides an implementation for JDK1.3 and below. This simply overrides
     * the JDK1.4 implementation of this method.
     */
    public Throwable getCause() {
        return _cause;
    }

}
