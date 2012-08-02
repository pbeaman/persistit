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
 * Thrown by {@link com.persistit.Volume} if the volume file has a different
 * internal ID value than expected. This condition can signify that a volume
 * file has been renamed or the wrong file has been restored to a configuration.
 * 
 */
public class WrongVolumeException extends PersistitException {
    private static final long serialVersionUID = 9119544306031815864L;

    public WrongVolumeException() {
        super();
    }

    public WrongVolumeException(String msg) {
        super(msg);
    }

}
