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
 * The superclass for all checked Persistit&trade; Exceptions.
 * 
 * @version 1.0
 */
public abstract class PersistitException extends Exception {
    private Exception _exception;

    protected PersistitException() {
        super();
    }

    protected PersistitException(String msg) {
        super(msg);
    }

    protected PersistitException(Exception exception) {
        super();
        _exception = exception;
    }

    protected PersistitException(String msg, Exception exception) {
        super(msg);
        _exception = exception;
    }

    /**
     * Provides an implementation for JDK1.3 and below. This simply overrides
     * the JDK1.4 implementation of this method.
     */
    public Throwable getCause() {
        return _exception;
    }
}
