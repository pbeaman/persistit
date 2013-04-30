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

package com.persistit.mxbeans;

import javax.management.Descriptor;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.StandardMBean;

/**
 * Wrapper for MXBean to enable the {@link Description} annotation.
 * 
 * @see <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4853303">Bug
 *      4853303</a>
 * 
 * @author peter
 * 
 * @param <T>
 *            The actual MXBean type
 */
public class MXBeanWrapper<T> extends StandardMBean implements NotificationEmitter {
    private final NotificationEmitter _emitter;

    public MXBeanWrapper(final T mbeanInterface, final Class<T> implementation, final NotificationEmitter emitter)
            throws NotCompliantMBeanException {
        super(mbeanInterface, implementation, true);
        _emitter = emitter;
    }

    @Override
    public String getDescription(final MBeanAttributeInfo info) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue("Description") != null) {
            return (String) descriptor.getFieldValue("Description");
        } else {
            return info.getDescription();
        }
    }

    @Override
    public String getDescription(final MBeanOperationInfo info) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue("Description") != null) {
            return (String) descriptor.getFieldValue("Description");
        } else {
            return info.getDescription();
        }
    }

    @Override
    public String getDescription(final MBeanOperationInfo operation, final MBeanParameterInfo info, final int sequence) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue("Description") != null) {
            return (String) descriptor.getFieldValue("Description");
        } else {
            return info.getDescription();
        }
    }

    @Override
    public String getParameterName(final MBeanOperationInfo operation, final MBeanParameterInfo info, final int sequence) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue(PName.PNAME) != null) {
            return (String) descriptor.getFieldValue(PName.PNAME);
        } else {
            return info.getName();
        }
    }

    @Override
    public void addNotificationListener(final NotificationListener listener, final NotificationFilter filter,
            final Object handback) throws IllegalArgumentException {
        if (_emitter != null) {
            _emitter.addNotificationListener(listener, filter, handback);
        }
    }

    @Override
    public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException {
        if (_emitter != null) {
            _emitter.removeNotificationListener(listener);
        }
    }

    @Override
    public void removeNotificationListener(final NotificationListener listener, final NotificationFilter filter,
            final Object handback) throws ListenerNotFoundException {
        if (_emitter != null) {
            _emitter.removeNotificationListener(listener, filter, handback);
        }
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        if (_emitter != null) {
            return _emitter.getNotificationInfo();
        } else {
            return new MBeanNotificationInfo[] {};
        }
    }

}
