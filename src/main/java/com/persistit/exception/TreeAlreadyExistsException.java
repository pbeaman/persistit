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

import java.io.IOException;

/**
 * Thrown when attempting to create new {@link com.persistit.Tree} if another
 * <code>Tree</code> having the same name already exists.
 * 
 * @version 1.0
 */
public class TreeAlreadyExistsException extends IOException {
    private static final long serialVersionUID = 4038415783689624531L;

    public TreeAlreadyExistsException() {
        super();
    }

    public TreeAlreadyExistsException(final String msg) {
        super(msg);
    }
}
