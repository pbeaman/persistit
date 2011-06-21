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
 * Thrown by {@link com.persistit.Transaction} when a transaction fails due to
 * excessive rollbacks.
 * 
 * @version 1.0
 */
public class TransactionFailedException extends PersistitException {
    private static final long serialVersionUID = 4333338222080863109L;

    Throwable _cause;

    public TransactionFailedException() {
        super();
    }

    public TransactionFailedException(String msg) {
        super(msg);
    }

    public TransactionFailedException(Throwable t) {
        _cause = t;
    }

    /**
     * Provides an implementation for JDK1.3 and below. This simply overrides
     * the JDK1.4 implementation of this method.
     */
    @Override
    public Throwable getCause() {
        return _cause;
    }

}
