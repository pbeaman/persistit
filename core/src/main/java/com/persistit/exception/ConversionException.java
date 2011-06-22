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
 * Thrown by encoding and decoding methods of the {@link com.persistit.Value}
 * class when conversion to or from an value's serialized form cannot be
 * completed.
 * 
 * @version 1.0
 */
public class ConversionException extends RuntimeException {
    private static final long serialVersionUID = -5255687227174752145L;

    private Exception _exception;

    public ConversionException() {
        super();
    }

    public ConversionException(String msg) {
        super(msg);
    }

    public ConversionException(Exception exception) {
        super();
        _exception = exception;
    }

    public ConversionException(String msg, Exception exception) {
        super(msg);
        _exception = exception;
    }

    /**
     * Provides an implementation for JDK1.3 and below. This simply overrides
     * the JDK1.4 implementation of this method.
     */
    @Override
    public Throwable getCause() {
        return _exception;
    }
}
