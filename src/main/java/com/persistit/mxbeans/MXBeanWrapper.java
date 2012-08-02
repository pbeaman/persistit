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
 * @see <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4853303">Bug 4853303</a>
 * 
 * @author peter
 * 
 * @param <T>
 *            The actual MXBean type
 */
public class MXBeanWrapper<T> extends StandardMBean implements NotificationEmitter {
    private NotificationEmitter _emitter;

    public MXBeanWrapper(T mbeanInterface, Class<T> implementation, final NotificationEmitter emitter)
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
    public String getDescription(MBeanOperationInfo info) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue("Description") != null) {
            return (String) descriptor.getFieldValue("Description");
        } else {
            return info.getDescription();
        }
    }

    @Override
    public String getDescription(MBeanOperationInfo operation, MBeanParameterInfo info, int sequence) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue("Description") != null) {
            return (String) descriptor.getFieldValue("Description");
        } else {
            return info.getDescription();
        }
    }

    @Override
    public String getParameterName(MBeanOperationInfo operation, MBeanParameterInfo info, int sequence) {
        final Descriptor descriptor = info.getDescriptor();
        if (descriptor != null && descriptor.getFieldValue(PName.PNAME) != null) {
            return (String) descriptor.getFieldValue(PName.PNAME);
        } else {
            return info.getName();
        }
    }

    @Override
    public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
            throws IllegalArgumentException {
        if (_emitter != null) {
            _emitter.addNotificationListener(listener, filter, handback);
        }
    }

    @Override
    public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
        if (_emitter != null) {
            _emitter.removeNotificationListener(listener);
        }
    }

    @Override
    public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
            throws ListenerNotFoundException {
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
