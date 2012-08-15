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
 * Thrown when the Persistit is unable to find the properties file specified by
 * the {@link com.persistit.Persistit#initialize(java.lang.String) initialize}
 * method.
 * 
 * @version 1.0
 */
public class PropertiesNotFoundException extends PersistitException {
    private static final long serialVersionUID = -1864473416316472113L;

    public PropertiesNotFoundException() {
        super();
    }

    public PropertiesNotFoundException(final String msg) {
        super(msg);
    }
}
