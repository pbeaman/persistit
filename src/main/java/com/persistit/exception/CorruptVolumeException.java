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

import com.persistit.util.Debug;

/**
 * Thrown if Persistit attempts to open a corrupt volume file. Generally it will
 * be necessary to restore a backup copy of the volume file to resolve this.
 * 
 * @version 1.0
 */
public class CorruptVolumeException extends PersistitException {
    private static final long serialVersionUID = 9119544306031815864L;

    public CorruptVolumeException() {
        super();
    }

    public CorruptVolumeException(String msg) {
        super(msg);
        Debug.$assert0.t(false);
    }

}
