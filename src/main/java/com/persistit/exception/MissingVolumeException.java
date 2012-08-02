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

/**
 * Thrown if the journal files refer to a volume that is no longer present in
 * the system. Generally this condition is irrecoverable because without the
 * missing volume a consistent database state cannot be restored. However, in
 * the event the removal of the volume is intentional, it is possible to specify
 * a mode in which pages and transactions destined for missing volumes are
 * ignored.
 * 
 * @version 1.0
 */
public class MissingVolumeException extends CorruptJournalException {
    private static final long serialVersionUID = -9014051945087375523L;
    private final String _volumeName;

    public MissingVolumeException(final String msg, final String volumeName) {
        super(msg);
        _volumeName = volumeName;
    }
    
    public String getVolumeName() {
        return _volumeName;
    }
}
