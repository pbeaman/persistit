/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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
 * Unchecked wrapper for an {@link IOException} used in a context where the
 * {@link Appendable#append} operation throws an IOException. Since most uses of
 * methods that take an <code>Appendable</code> will operate on
 * <code>StringBuilder</code>s, for which an IOException is never thrown, it is
 * desirable for calling code not to have to catch and handle
 * <code>IOException</code>. Therefore any <code>IOException</code> from
 * invoking append on a different <code>Appendable</code> implementation is
 * caught and thrown as this unchecked type.
 * 
 * @version 1.0
 */
public class AppendableIOException extends RuntimeException {

    private static final long serialVersionUID = -2096632389635542578L;

    public AppendableIOException(IOException ioe) {
        super(ioe);
    }

}
