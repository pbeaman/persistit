/*
 * Copyright (c) 2005 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Nov 28, 2005
 */

package com.persistit.jmx;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.StringTokenizer;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenMBeanAttributeInfo;
import javax.management.openmbean.OpenMBeanConstructorInfo;
import javax.management.openmbean.OpenMBeanConstructorInfoSupport;
import javax.management.openmbean.OpenMBeanInfoSupport;
import javax.management.openmbean.OpenMBeanOperationInfo;
import javax.management.openmbean.OpenMBeanParameterInfo;
import javax.management.openmbean.OpenMBeanParameterInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import com.persistit.LogBase;
import com.persistit.Management;
import com.persistit.Persistit;
import com.persistit.logging.AbstractPersistitLogger;
import com.persistit.logging.LogTemplate;

/**
 * <p>
 * Provides a JMX instrumentation MBean for managing Persistit. This
 * implementation conforms to the Open MBean specification so that Persistit can
 * be managed from third-party management consoles. See the <a
 * href="http://java.sun.com/j2se/1.5.0/docs/api/javax/management/openmbean/package-summary.html#package_description">
 * javax.management.openmbean</a> package documentation for further information
 * on Open MBeans.
 * </p>
 * <p>
 * Note: This class and is packaged in the ancillary JAR file
 * <tt>persistit_jsa<i>NNN</i>_jmx.jar</tt>.
 * </p>
 * <p>
 * The simplest way to create and register an instance of this MBean is to
 * specify the property
 * 
 * <pre><code>
 * jmx = true
 * </code></pre>
 * 
 * in the Persistit configuration properties files or add the command line
 * switch </tt>-Dcom.persistit.jmx=true</tt>. For J2SE 5.0 and above, this
 * property causes Persistit to register a <tt>PersistitOopenMBean</tt> on the
 * platform MBean server.
 * </p>
 * <p>
 * To use a different MBean server, use the static method
 * {@link #setup(MBeanServer)} or {@link #register(MBeanServer)} to create and
 * register an instance of this MBean on an MBean server, and use
 * {@link #unregister(MBeanServer)} to remove it.
 * </p>
 * 
 * @version 1.1
 */
public class PersistitOpenMBean implements DynamicMBean, Serializable {
	private static final long serialVersionUID = 1476010318564941812L;

	private final static String THIS_CLASS_NAME = PersistitOpenMBean.class
			.getName();

	private static ResourceBundle _persistitMBeanBundle;

	private static Object[] SIMPLE_TYPE_ARRAY = { "boolean",
			SimpleType.BOOLEAN, "byte", SimpleType.BYTE, "char",
			SimpleType.CHARACTER, "double", SimpleType.DOUBLE, "float",
			SimpleType.FLOAT, "int", SimpleType.INTEGER, "long",
			SimpleType.LONG, "short", SimpleType.SHORT, "void",
			SimpleType.VOID, "String", SimpleType.STRING, "Date",
			SimpleType.DATE, };

	private static Object[] IMPACT_NAME_ARRAY = { "ACTION", new Integer(1),
			"INFO", new Integer(0), "ACTION_INFO", new Integer(2), "UNKNOWN",
			new Integer(3), };

	private static Object[] SIMPLE_TYPE_TO_CLASS = { SimpleType.STRING,
			String.class, SimpleType.VOID, void.class, SimpleType.BOOLEAN,
			boolean.class, SimpleType.BYTE, byte.class, SimpleType.CHARACTER,
			char.class, SimpleType.DATE, Date.class, SimpleType.DOUBLE,
			double.class, SimpleType.FLOAT, float.class, SimpleType.INTEGER,
			int.class, SimpleType.LONG, long.class, SimpleType.SHORT,
			short.class, SimpleType.BIGDECIMAL, BigDecimal.class,
			SimpleType.BIGINTEGER, BigInteger.class, };

	static {
		try {
			_persistitMBeanBundle = ResourceBundle.getBundle(THIS_CLASS_NAME);
		} catch (MissingResourceException mre) {
			// Should never happen because the properties file is in the
			// same JAR.
			throw new RuntimeException(mre);
		}
	}

	private static ObjectName _registeredObjectName;

	private final static String _className = PersistitOpenMBean.class.getName();

	private final static String _description = "Open MBean implementation of the Persistit Management interface";

	private final Persistit _persistitInstance;

	private final List _attrList = new ArrayList();

	private final List _methodList = new ArrayList();

	private final Map _adapterMap = new HashMap();

	private final Map _attributeMap = new HashMap();

	private final Map _methodMap = new HashMap();

	private OpenMBeanInfoSupport _mBeanInfo = null;

	/**
	 * Constructs and configures an MBean instance, but does not register it
	 * with an MBeanServer. See {@link #setup(MBeanServer)} and
	 * {@link #register(MBeanServer)}.
	 * 
	 * @throws OpenDataException
	 */
	PersistitOpenMBean(final Persistit persistit) throws OpenDataException {
		_persistitInstance = persistit;
		synchronized (PersistitOpenMBean.class) {
			buildDynamicMBeanInfo();
		}
	}

	static String getProperty(String propertyName) {
		if (_persistitMBeanBundle == null)
			return null;
		try {
			return _persistitMBeanBundle.getString(propertyName);
		} catch (MissingResourceException mre) {
			return null;
		}
	}

	/**
	 * Obtain the value of a specific attribute of the Dynamic MBean.
	 * <p>
	 * See <a
	 * href="http://java.sun.com/j2se/1.5.0/docs/api/javax/management/DynamicMBean.html">
	 * <tt>javax.management.DynamicMBean</tt></a>.
	 * </p>
	 * 
	 * @param attributeName
	 *            The name of the attribute to be retrieved
	 * 
	 * @return The value of the attribute retrieved.
	 * 
	 * @exception AttributeNotFoundException
	 * @exception MBeanException
	 *                Wraps a <CODE>java.lang.Exception</CODE> thrown by the
	 *                MBean's getter.
	 * @exception ReflectionException
	 *                Wraps a <CODE>java.lang.Exception</CODE> thrown while
	 *                trying to invoke the getter.
	 * 
	 * @see #setAttribute
	 */
	public Object getAttribute(String attributeName)
			throws AttributeNotFoundException, MBeanException,
			ReflectionException {
		AttributeReflector adapter = (AttributeReflector) _attributeMap
				.get(attributeName);
		if (adapter != null) {
			return adapter.get(getManagement());
		} else {
			throw new AttributeNotFoundException("Cannot find " + attributeName
					+ " attribute in " + _className);
		}
	}

	/**
	 * Set the value of a specific attribute of the Dynamic MBean.
	 * <p>
	 * See <a
	 * href="http://java.sun.com/j2se/1.5.0/docs/api/javax/management/DynamicMBean.html">
	 * <tt>javax.management.DynamicMBean</tt></a>.
	 * </p>
	 * 
	 * @param attribute
	 *            The identification of the attribute to be set and the value it
	 *            is to be set to.
	 * 
	 * @exception AttributeNotFoundException
	 * @exception InvalidAttributeValueException
	 * @exception MBeanException
	 *                Wraps a <CODE>java.lang.Exception</CODE> thrown by the
	 *                MBean's setter.
	 * @exception ReflectionException
	 *                Wraps a <CODE>java.lang.Exception</CODE> thrown while
	 *                trying to invoke the MBean's setter.
	 * 
	 * @see #getAttribute
	 */
	public void setAttribute(Attribute attribute)
			throws AttributeNotFoundException, InvalidAttributeValueException,
			MBeanException, ReflectionException {

		String name = attribute.getName();
		Object value = attribute.getValue();

		AttributeReflector adapter = (AttributeReflector) _attributeMap
				.get(name);
		if (adapter != null) {
			adapter.set(getManagement(), value);
		} else {
			// unrecognized attribute name:
			throw new AttributeNotFoundException("Attribute " + name
					+ " not found in " + _className);
		}
	}

	private Management getManagement() {
		try {
			return _persistitInstance.getManagement();
		} catch (Exception e) {
			throw new PersistitJmxRuntimeException(e);
		}
	}

	/**
	 * Get the values of several attributes of the Dynamic MBean.
	 * <p>
	 * See <a
	 * href="http://java.sun.com/j2se/1.5.0/docs/api/javax/management/DynamicMBean.html">
	 * <tt>javax.management.DynamicMBean</tt></a>.
	 * </p>
	 * 
	 * @param attributeNames
	 *            An array of names of attributes to be retrieved.
	 * 
	 * @return The list of attributes retrieved.
	 * 
	 * @see #setAttributes
	 */
	public AttributeList getAttributes(String[] attributeNames) {
		// Check attributeNames is not null to avoid NullPointerException
		// later on
		//
		if (attributeNames == null) {
			throw new RuntimeOperationsException(new IllegalArgumentException(
					"attributeNames[] cannot be null"),
					"Cannot invoke a getter of " + _className);
		}
		AttributeList resultList = new AttributeList();

		// If attributeNames is empty, return an empty result list
		//
		if (attributeNames.length == 0) {
			return resultList;
		}

		// Build the result attribute list
		//
		for (int i = 0; i < attributeNames.length; i++) {
			try {
				Object value = getAttribute((String) attributeNames[i]);
				resultList.add(new Attribute(attributeNames[i], value));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return resultList;
	}

	/**
	 * Sets the values of several attributes of the Dynamic MBean.
	 * <p>
	 * See <a
	 * href="http://java.sun.com/j2se/1.5.0/docs/api/javax/management/DynamicMBean.html">
	 * <tt>javax.management.DynamicMBean</tt></a>.
	 * </p>
	 * 
	 * @param attributes
	 *            A list of attributes: The identification of the attributes to
	 *            be set and the values they are to be set to.
	 * 
	 * @return The list of attributes that were set, with their new values.
	 * 
	 * @see #getAttributes
	 */
	public AttributeList setAttributes(AttributeList attributes) {
		// Check attributes is not null to avoid NullPointerException later on
		//
		if (attributes == null) {
			throw new RuntimeOperationsException(new IllegalArgumentException(
					"AttributeList attributes cannot be null"),
					"Cannot invoke a setter of " + _className);
		}
		AttributeList resultList = new AttributeList();

		// If attributeNames is empty, nothing more to do
		//
		if (attributes.isEmpty()) {
			return resultList;
		}

		// For each attribute, try to set it and add to the result list if
		// successfull
		//
		for (Iterator i = attributes.iterator(); i.hasNext();) {
			Attribute attr = (Attribute) i.next();
			try {
				setAttribute(attr);
				String name = attr.getName();
				Object value = getAttribute(name);
				resultList.add(new Attribute(name, value));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return resultList;
	}

	/**
	 * Allows an action to be invoked on the Dynamic MBean.
	 * <p>
	 * See <a
	 * href="http://java.sun.com/j2se/1.5.0/docs/api/javax/management/DynamicMBean.html">
	 * <tt>javax.management.DynamicMBean</tt></a>.
	 * </p>
	 * 
	 * @param operationName
	 *            The name of the action to be invoked.
	 * 
	 * @param params
	 *            An array containing the parameters to be set when the action
	 *            is invoked.
	 * 
	 * @param signature
	 *            An array containing the signature of the action. The class
	 *            objects will be loaded through the same class loader as the
	 *            one used for loading the MBean on which the action is invoked.
	 * 
	 * @return The object returned by the action, which represents the result of
	 *         invoking the action on the MBean specified.
	 * 
	 * @exception MBeanException
	 *                Wraps a <CODE>java.lang.Exception</CODE> thrown by the
	 *                MBean's invoked method.
	 * 
	 * @exception ReflectionException
	 *                Wraps a <CODE>java.lang.Exception</CODE> thrown while
	 *                trying to invoke the method
	 */
	public Object invoke(String operationName, Object params[],
			String signature[]) throws MBeanException, ReflectionException {

		// Check operationName is not null to avoid NullPointerException
		// later on
		//
		if (operationName == null) {
			throw new RuntimeOperationsException(new IllegalArgumentException(
					"Operation name cannot be null"),
					"Cannot invoke a null operation in " + _className);
		}
		MethodReflector reflector = (MethodReflector) _methodMap
				.get(operationName);
		if (reflector != null) {
			return reflector.invoke(getManagement(), params);
		}

		throw new ReflectionException(new NoSuchMethodException(operationName),
				"Cannot find the operation " + operationName + " in "
						+ _className);
	}

	/**
	 * Provides the exposed attributes and actions of the Dynamic MBean using an
	 * MBeanInfo object.
	 * <p>
	 * See <a
	 * href="http://java.sun.com/j2se/1.5.0/docs/api/javax/management/DynamicMBean.html">
	 * <tt>javax.management.DynamicMBean</tt></a>.
	 * </p>
	 * 
	 * @return An instance of <CODE>MBeanInfo</CODE> allowing all attributes
	 *         and actions exposed by this Dynamic MBean to be retrieved.
	 * 
	 */
	public MBeanInfo getMBeanInfo() {

		// Return the information we want to expose for management:
		// the _mBeanInfo private field has been built at instanciation time
		//
		return _mBeanInfo;
	}

	static Class openTypeToClass(OpenType type) {
		if (type instanceof ArrayType) {
			OpenType elementType = ((ArrayType) type).getElementOpenType();
			Class elementClass = openTypeToClass(elementType);
			Class arrayClass = (Array.newInstance(elementClass, 1)).getClass();
			return arrayClass;
		}
		for (int index = 0; index < SIMPLE_TYPE_TO_CLASS.length; index += 2) {
			if (type == SIMPLE_TYPE_TO_CLASS[index]) {
				return (Class) SIMPLE_TYPE_TO_CLASS[index + 1];
			}
		}
		return null;
	}

	Adapter typeAdapter(String type) throws OpenDataException {
		boolean isArray = false;
		boolean isComposite = false;
		Adapter result = null;

		if (type.endsWith("[]")) {
			isArray = true;
			type = type.substring(0, type.length() - 2);
		}

		if (type.startsWith("$")) {
			isComposite = true;
			type = type.substring(1);
		}
		result = (Adapter) _adapterMap.get(type);

		if (result == null && !isComposite) {
			// Simple linear search because this happens only while loading the
			// schema and is not performance-critical.
			for (int index = 0; index < SIMPLE_TYPE_ARRAY.length; index += 2) {
				if (SIMPLE_TYPE_ARRAY[index].equals(type)) {
					result = new SimpleAdapter(
							(OpenType) SIMPLE_TYPE_ARRAY[index + 1]);

					_adapterMap.put(type, result);
					break;
				}
			}
		}

		if (result == null)
			return null;

		if (isArray) {
			return new ArrayAdapter(1, result);
		} else {
			return result;
		}
	}

	private int impact(String impactName) {
		for (int index = 0; index < IMPACT_NAME_ARRAY.length; index += 2) {
			if (impactName.equals(IMPACT_NAME_ARRAY[index])) {
				return ((Integer) IMPACT_NAME_ARRAY[index + 1]).intValue();
			}
		}
		return -1;
	}

	private CompositeTypeAdapter buildCompositeDataAdapter(int n)
			throws OpenDataException {
		String rootName = "type." + n;
		String s = getProperty(rootName);
		if (s == null)
			return null;
		StringTokenizer st = new StringTokenizer(s, "|");

		String name = st.nextToken();
		String description = st.nextToken();
		List itemNames = new ArrayList();
		List itemDescriptions = new ArrayList();
		List itemTypes = new ArrayList();

		for (int paramIndex = 1;; paramIndex++) {
			s = getProperty(rootName + "." + paramIndex);
			if (s == null)
				break;
			st = new StringTokenizer(s, "|");

			itemNames.add(st.nextElement());
			itemTypes.add(typeAdapter((String) st.nextElement()).getOpenType());
			itemDescriptions.add(st.nextElement());
		}
		int size = itemNames.size();
		String actualClassName = "com.persistit.Management$" + name;
		Class actualClass = null;
		try {
			actualClass = Class.forName(actualClassName);
		} catch (ClassNotFoundException cnfe) {
			throw new OpenDataException("Can't load class " + actualClassName
					+ ": " + cnfe);
		}

		return new CompositeTypeAdapter(name, description, (String[]) itemNames
				.toArray(new String[size]), (String[]) itemDescriptions
				.toArray(new String[size]), (OpenType[]) itemTypes
				.toArray(new OpenType[size]), actualClass);
	}

	private AttributeReflector buildAttributeReflector(int n)
			throws OpenDataException {
		String name = "attribute." + n;
		String s = getProperty(name);
		if (s == null)
			return null;
		StringTokenizer st = new StringTokenizer(s, "|");
		String attrName = st.nextToken();
		String typeName = st.nextToken();
		String description = st.nextToken();
		AttributeReflector adapter = new AttributeReflector(attrName,
				description, typeAdapter(typeName));
		return adapter;
	}

	private MethodReflector buildMethodReflector(int n)
			throws OpenDataException {
		String name = "method." + n;
		String s = getProperty(name);
		if (s == null)
			return null;
		StringTokenizer st = new StringTokenizer(s, "|");
		String methodName = st.nextToken();
		String methodType = st.nextToken();
		String impactName = st.nextToken();
		String methodDescription = st.nextToken();
		List paramList = new ArrayList();
		for (int m = 1;; m++) {
			String param = name + "." + m;
			String s2 = getProperty(param);
			if (s2 == null)
				break;
			StringTokenizer st2 = new StringTokenizer(s2, "|");
			String paramName = st2.nextToken();
			Adapter typeAdapter = typeAdapter(st2.nextToken());
			String paramDescription = st2.nextToken();
			OpenMBeanParameterInfo info = new OpenMBeanParameterInfoSupport(
					paramName, paramDescription, typeAdapter.getOpenType());
			paramList.add(info);
		}
		MethodReflector reflector = new MethodReflector(methodName,
				methodDescription, (OpenMBeanParameterInfo[]) paramList
						.toArray(new OpenMBeanParameterInfo[paramList.size()]),
				typeAdapter(methodType), impact(impactName));

		return reflector;
	}

	/**
	 * Build the private _mBeanInfo field, which represents the management
	 * interface exposed by the MBean, that is, the set of attributes,
	 * constructors, operations and notifications which are available for
	 * management.
	 * 
	 * A reference to the _mBeanInfo object is returned by the getMBeanInfo()
	 * method of the DynamicMBean interface. Note that, once constructed, an
	 * MBeanInfo object is immutable.
	 */
	private void buildDynamicMBeanInfo() throws OpenDataException {

		for (int index = 1;; index++) {
			CompositeTypeAdapter adapter = buildCompositeDataAdapter(index);
			if (adapter != null) {
				_adapterMap.put(adapter.getOpenType().getTypeName(), adapter);
			} else {
				break;
			}
		}

		for (int index = 1;; index++) {
			AttributeReflector reflector = buildAttributeReflector(index);

			if (reflector != null) {
				_attrList.add(reflector);
				_attributeMap.put(reflector.getName(), reflector);
			} else {
				break;
			}
		}

		for (int index = 1;; index++) {
			MethodReflector reflector = buildMethodReflector(index);

			if (reflector != null) {
				_methodList.add(reflector);
				_methodMap.put(reflector.getName(), reflector);
			} else {
				break;
			}
		}

		int attrCount = _attrList.size();
		OpenMBeanAttributeInfo[] attributes = new OpenMBeanAttributeInfo[attrCount];

		for (int index = 0; index < attrCount; index++) {
			attributes[index] = ((AttributeReflector) _attrList.get(index))
					.getAttributeInfo();
		}

		int methodCount = _methodList.size();
		OpenMBeanOperationInfo[] operations = new OpenMBeanOperationInfo[methodCount];

		for (int index = 0; index < methodCount; index++) {
			operations[index] = ((MethodReflector) _methodList.get(index))
					.getOperationInfo();
		}

		OpenMBeanConstructorInfo[] constructors = new OpenMBeanConstructorInfo[] { new OpenMBeanConstructorInfoSupport(
				"PersistitOpenMBean", "Constructs a PerisitOpenMBean object",
				new OpenMBeanParameterInfoSupport[0]), };

		MBeanNotificationInfo[] notifications = new MBeanNotificationInfo[0];

		_mBeanInfo = new OpenMBeanInfoSupport(_className, _description,
				attributes, constructors, operations, notifications);
	}

	/**
	 * Attempts to register a PersistitOpenMBean instance on the specified
	 * {@link MBeanServer}. The object name for the MBean is
	 * 
	 * <pre><code>
	 *      com.persistit.PersistitOpenMBean:type=Persistit
	 * </code></pre>
	 * 
	 * @param persistitInstance
	 *            The Persistit instance to be monitored by this MBean
	 * @param server
	 *            The MBeanServer
	 * @return <tt>true</tt> if the a new PersistitOpenMBean instance was
	 *         successfully registered, <tt>false</tt> if there already was a
	 *         registered instance.
	 * @throws MalformedObjectNameException
	 * @throws MBeanRegistrationException
	 * @throws InstanceAlreadyExistsException
	 * @throws NotCompliantMBeanException
	 * @throws OpenDataException
	 */
	public synchronized static boolean register(
			final Persistit persistitInstance, final MBeanServer server)
			throws MalformedObjectNameException, MBeanRegistrationException,
			InstanceAlreadyExistsException, NotCompliantMBeanException,
			OpenDataException {
		if (_registeredObjectName != null)
			return false;

		PersistitOpenMBean pm = new PersistitOpenMBean(persistitInstance);
		ObjectName objectName = new ObjectName(
				"com.persistit.PersistitOpenMBean:type=Persistit");
		server.registerMBean(pm, objectName);
		_registeredObjectName = objectName;
		return true;
	}

	/**
	 * Unregisters the previously registered PersisitOpenMBean.
	 * 
	 * @param server
	 *            The MBeanServer
	 * @return <tt>true</tt> if there was a PersistitOpenMBean to unregister,
	 *         else <tt>false</tt>.
	 * 
	 * @throws MalformedObjectNameException
	 * @throws MBeanRegistrationException
	 * @throws InstanceAlreadyExistsException
	 * @throws NotCompliantMBeanException
	 * @throws InstanceNotFoundException
	 */
	public synchronized static boolean unregister(MBeanServer server)
			throws MalformedObjectNameException, MBeanRegistrationException,
			InstanceAlreadyExistsException, NotCompliantMBeanException,
			InstanceNotFoundException {
		if (_registeredObjectName == null)
			return false;
		server.unregisterMBean(_registeredObjectName);
		_registeredObjectName = null;
		return false;
	}

	/**
	 * Returns the platform MBean server if there for Java Runtime Environments
	 * at the Java 5 level and above. For earlier JVMs this method returns
	 * <tt>null</tt>.
	 * 
	 * @return The platform MBeanServer or <tt>null</tt> if there is none.
	 */
	public static MBeanServer getPlatformMBeanServer() {
		try {
			Class cl = Class.forName("java.lang.management.ManagementFactory");
			if (cl != null) {
				Method method = cl.getMethod("getPlatformMBeanServer",
						new Class[0]);
				return (MBeanServer) method.invoke(null, new Object[0]);
			}
		} catch (Exception ex) {
		}
		return null;
	}

	/**
	 * Attempts to construct and register an instance of this MBean using the
	 * platform MBean server. This method succeeds only on J2SE 5.0 and
	 * compatible platforms because it relies on the class
	 * <tt>java.lang.management.ManagementFactory</tt>. This method calls
	 * {@link #register(MBeanServer)} and writes any exceptions thrown during
	 * the attempt to the Persistit log.
	 */
	public static void setup(final Persistit persistit) {
		setup(persistit, null);
	}

	/**
	 * Attempts to construct and register an instance of this MBean on the
	 * supplied <tt>MBeanServer</tt>. This method calls
	 * {@link #register(MBeanServer)} and writes any exceptions thrown during
	 * the attempt to the Persistit log.
	 * 
	 * @param server
	 *            The MBeanServer, or <tt>null</tt> for the Java platform
	 *            MBeanServer.
	 */
	public static synchronized void setup(final Persistit persistit,
			MBeanServer server) {
		final AbstractPersistitLogger logger = persistit.getPersistitLogger();
		final LogBase base = persistit.getLogBase();
		if (server == null)
			server = getPlatformMBeanServer();

		try {
			if (logger != null && logger.isOpen()) {
				final LogTemplate lt = base
						.logTemplate(LogBase.LOG_MBEAN_REGISTRATION);
				if (logger.isLoggable(lt)) {
					logger.log(lt, server == null ? "java.management API not"
							+ " available, requires JDK 1.5" : "ok", null,
							null, null, null, null, null, null, null, null);
				}
			}
			if (server != null) {
				register(persistit, server);
			}
		} catch (Exception exception) {
			if (logger != null && logger.isOpen()) {
				final LogTemplate lt = base
						.logTemplate(LogBase.LOG_MBEAN_EXCEPTION);
				if (logger.isLoggable(lt)) {
					logger.log(lt, exception, null, null, null, null, null,
							null, null, null, null);
				}
			}
		}
	}

	void toXml(StringBuilder sb) {
		sb.append("<mbean description=\"");
		sb.append(_mBeanInfo.getDescription());
		sb.append("\">\r\n");

		for (int index = 0; index < _attrList.size(); index++) {
			AttributeReflector reflector = (AttributeReflector) _attrList
					.get(index);
			reflector.toXml(sb);
		}
		for (int index = 0; index < _methodList.size(); index++) {
			MethodReflector reflector = (MethodReflector) _methodList
					.get(index);
			reflector.toXml(sb);
		}

		sb.append("</mbean>\r\n");
	}

	static String xmlQuote(String s) {
		s = s.replace("&", "&amp;");
		s = s.replace("\"", "&quot;").replace("'", "&apos;");
		s = s.replace("<", "&lt;").replace(">", "&gt;");
		return s;
	}

	static String prettyType(String s) {
		boolean isArray = s.startsWith("[");
		if (isArray)
			s = s.substring(2, s.length() - 1) + "[]";
		if (s.startsWith("java.lang."))
			s = s.substring(10);
		return s;
	}
}
