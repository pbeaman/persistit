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

import com.persistit.Task;

/**
 * Thrown when a {@link Task} performing within the context of a task controller
 * is stopped.
 * 
 * @version 1.0
 */
public class TaskEndedException extends RuntimeException {
    private static final long serialVersionUID = 6439247775639387615L;

    public TaskEndedException() {
        super();
    }

    public TaskEndedException(final String msg) {
        super(msg);
    }
}
