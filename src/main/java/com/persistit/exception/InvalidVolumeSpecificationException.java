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
 * Thrown by {@link com.persistit.Volume} on an attempt to create a Volume with
 * invalid buffer size, initial size, extension size or maximum size..
 * 
 * @version 1.0
 */
public class InvalidVolumeSpecificationException extends IllegalArgumentException {
    private static final long serialVersionUID = 5310678046457279454L;

    public InvalidVolumeSpecificationException() {
        super();
    }

    public InvalidVolumeSpecificationException(String msg) {
        super(msg);
    }

}
