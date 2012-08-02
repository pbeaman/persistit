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
 * This is a wrapper for an {@link InterruptedException}. If a Persistit thread
 * is interrupted when sleeping or waiting, it throws an instance of this
 * Exception. This allows the caller of a Persistit method to catch
 * {@link PersistitException}s without also needing to catch
 * InterruptedExceptions.
 * <p />
 * Before throwing a PersistitInterruptedException Persistit reasserts the
 * <code>interrupted</code> status of the current Thread so that a subsequent
 * call to sleep or wait will once again be interrupted.
 * 
 * @version 1.0
 */
public class PersistitInterruptedException extends PersistitException {
    private static final long serialVersionUID = 9108205596568958490L;

    public PersistitInterruptedException(InterruptedException ioe) {
        super(ioe);
        Thread.currentThread().interrupt();
    }

}

