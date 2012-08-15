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
 * Thrown to signify an internal condition that requires special handling during
 * deletion rebalancing. This exception should always be caught and handled
 * within the Persistit implementation, and never thrown to application code.
 * 
 * @version 1.0
 */
public class RebalanceException extends PersistitException {
    private static final long serialVersionUID = 5712813170520119517L;

    public final static RebalanceException SINGLETON = new RebalanceException("Singleton");

    public RebalanceException() {
    }

    public RebalanceException(final String msg) {
        super(msg);
    }
}
