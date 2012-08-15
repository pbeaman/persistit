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
 * Thrown when the Persistit configuration specifies the <code>createOnly</code>
 * flag for a volume that already exists, or when a the configuration specifies
 * a page size for a volume that does not the page size of an existing volume.
 * 
 * @version 1.0
 */
public class VolumeAlreadyExistsException extends PersistitException {
    private static final long serialVersionUID = -5943311415307074464L;

    public VolumeAlreadyExistsException() {
        super();
    }

    public VolumeAlreadyExistsException(final String msg) {
        super(msg);
    }
}
