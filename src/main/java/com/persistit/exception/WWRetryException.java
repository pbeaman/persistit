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
 * Thrown when a method must back off and retry.
 * 
 * @version 2.1
 */
public class WWRetryException extends PersistitException {

    private static final long serialVersionUID = -8001684710494122369L;

    final long _versionHandle;

    public WWRetryException(final long handle) {
        _versionHandle = handle;
    }

    public long getVersionHandle() {
        return _versionHandle;
    }
}
