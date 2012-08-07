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
 * Thrown by {@link com.persistit.Exchange} when a <code>Thread</code> other
 * than the thread that created the Exchange attempts to perform an operation.
 * 
 * @version 1.0
 */
public class PersistitClosedException extends PersistitException {

    private static final long serialVersionUID = -1539273671667528188L;

    public PersistitClosedException() {
        super();
    }

    public PersistitClosedException(String msg) {
        super(msg);
    }

}
