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
 * Thrown if a multi-version value is malformed.
 * 
 * @version 1.0
 */
public class CorruptValueException extends PersistitException {
    private static final long serialVersionUID = -5397911019132612370L;

    public CorruptValueException(final String msg) {
        super(msg);
    }
}
