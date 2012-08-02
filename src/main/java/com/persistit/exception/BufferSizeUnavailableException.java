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
 * Thrown if there is no {@link com.persistit.BufferPool} with buffers matching
 * the page size of a {@link com.persistit.Volume} being opened.
 * 
 * @version 1.0
 */
public class BufferSizeUnavailableException extends PersistitException {
    private static final long serialVersionUID = 7231838587511494996L;

    public BufferSizeUnavailableException(String msg) {
        super(msg);
    }

}
