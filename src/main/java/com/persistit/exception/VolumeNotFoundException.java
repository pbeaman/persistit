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
 * Thrown when the Persistit configuration species a volume file that does not
 * exist, and does not specify the <code>create</code> or
 * <code>createOnly</code> flag.
 * 
 * @version 1.0
 */
public class VolumeNotFoundException extends PersistitException {
    private static final long serialVersionUID = 153323091297427862L;

    public VolumeNotFoundException() {
        super();
    }

    public VolumeNotFoundException(String msg) {
        super(msg);
    }
}

