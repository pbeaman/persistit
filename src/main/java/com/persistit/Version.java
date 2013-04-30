/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    interface VersionCreator<V> {
        V createVersion(final TimelyResource<? extends V> resource) throws PersistitException;
    }

}
