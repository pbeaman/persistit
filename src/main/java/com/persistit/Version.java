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

package com.persistit;

import com.persistit.exception.PersistitException;

/**
 * Marker interface implemented by objects managed within the
 * {@link TimelyResource} framework.
 * 
 * @author peter
 */
interface Version {
    /**
     * Sub-interface describing <code>Version</code> types that require pruning
     * when obsolete.
     * 
     * @author peter
     */
    interface PrunableVersion extends Version {
        /**
         * Clean up any state held by this resource. For example, when a
         * {@link Tree} is pruned, all pages allocated to its content are
         * deallocated. This method is called when a newer
         * <code>TimelyResource</code> has been created and is visible to all
         * active transactions.
         * 
         * @return <code>true</code> if all pruning work for this resource has
         *         been completed, <code>false</code> if the prune method should
         *         be called again later
         */
        boolean prune() throws PersistitException;

        /**
         * Called after the last known <code>Version</code> managed by a
         * <code>TimelyResource</code> has been pruned.
         * 
         * @throws PersistitException
         */
        void vacate() throws PersistitException;

    }

    /**
     * Interface for a factory that creates Versions of the specified type.
     * 
     * @author peter
     * 
     * @param <T>
     * @param <V>
     */
    interface VersionCreator<T, V> {
        V createVersion(final TimelyResource<T, ? extends V> resource) throws PersistitException;
    }

}