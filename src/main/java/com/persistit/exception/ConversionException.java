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
 * Thrown by encoding and decoding methods of the {@link com.persistit.Value}
 * and {@link com.persistit.Key} classes when conversion to or from an value's
 * serialized form cannot be completed.
 * 
 * @version 1.0
 */
public class ConversionException extends RuntimeException {
    private static final long serialVersionUID = -5255687227174752145L;

    public ConversionException() {
        super();
    }

    public ConversionException(final String msg) {
        super(msg);
    }

    public ConversionException(final Throwable t) {
        super(t);
    }

    public ConversionException(final String msg, final Throwable t) {
        super(msg, t);
    }

}
