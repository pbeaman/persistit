/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
