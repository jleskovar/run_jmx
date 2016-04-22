/*
 *  Copyright 2009-2011 Felix Roethenbacher
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package ch.syabru.nagios;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.ConnectException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import javax.management.*;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.InvalidKeyException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Originally a Nagios JMX plugin.
 * Repurposed to be a generic JMX command-line executor
 *
 * @author Felix Roethenbacher
 * @author James Leskovar
 */
public class NagiosJmxPlugin {

    /**
     * Username system property.
     */
    public static final String PROP_USERNAME = "username";
    /**
     * Password system property.
     */
    public static final String PROP_PASSWORD = "password";
    /**
     * Object name system property.
     */
    public static final String PROP_OBJECT_NAME = "objectName";
    /**
     * Attribute name system property.
     */
    public static final String PROP_ATTRIBUTE_NAME = "attributeName";
    /**
     * Attribute key system property.
     */
    public static final String PROP_ATTRIBUTE_KEY = "attributeKey";
    /**
     * Service URL system property.
     */
    public static final String PROP_SERVICE_URL = "serviceUrl";
    /**
     * Threshold warning level system property.
     * The number format of this property has to correspond to the type of
     * the attribute object.
     */
    public static final String PROP_THRESHOLD_WARNING = "thresholdWarning";
    /**
     * Threshold critical level system property.
     * The number format of this property has to correspond the type of
     * the attribute object.
     */
    public static final String PROP_THRESHOLD_CRITICAL = "thresholdCritical";
    /**
     * Units system property.
     */
    public static final String PROP_UNITS = "units";
    /**
     * Operation to invoke on MBean.
     */
    public static final String PROP_OPERATION = "operation";
    /**
     * Verbose output.
     */
    public static final String PROP_VERBOSE = "verbose";
    /**
     * Help output.
     */
    public static final String PROP_HELP = "help";

    private HashMap<MBeanServerConnection, JMXConnector> connections =
            new HashMap<MBeanServerConnection, JMXConnector>();

    /**
     * Open a connection to a MBean server.
     *
     * @param serviceUrl Service URL,
     *                   e.g. service:jmx:rmi://HOST:PORT/jndi/rmi://HOST:PORT/jmxrmi
     * @param username   Username
     * @param password   Password
     * @return MBeanServerConnection if succesfull.
     * @throws IOException XX
     */
    public MBeanServerConnection openConnection(
            JMXServiceURL serviceUrl, String username, String password)
            throws IOException {
        JMXConnector connector;
        HashMap<String, Object> environment = new HashMap<String, Object>();
        // Add environment variable to check for dead connections.
        environment.put("jmx.remote.x.client.connection.check.period", 5000);
        if (username != null && password != null) {
            environment = new HashMap<String, Object>();
            environment.put(JMXConnector.CREDENTIALS,
                    new String[]{username, password});
            connector = JMXConnectorFactory.connect(serviceUrl, environment);
        } else {
            connector = JMXConnectorFactory.connect(serviceUrl, environment);
        }
        MBeanServerConnection connection = connector.getMBeanServerConnection();
        connections.put(connection, connector);
        return connection;
    }

    /**
     * Close JMX connection.
     *
     * @param connection Connection.
     * @throws IOException XX.
     */
    public void closeConnection(MBeanServerConnection connection)
            throws IOException {
        JMXConnector connector = connections.remove(connection);
        if (connector != null) {
            connector.close();
        }
    }

    /**
     * Get object name object.
     *
     * @param connection MBean server connection.
     * @param objectName Object name string.
     * @return Object name object.
     * @throws InstanceNotFoundException    If object not found.
     * @throws MalformedObjectNameException If object name is malformed.
     * @throws CheckJmxException            If object name is not unqiue.
     * @throws IOException                  In case of a communication error.
     */
    public ObjectName getObjectName(MBeanServerConnection connection,
                                    String objectName)
            throws InstanceNotFoundException, MalformedObjectNameException,
            CheckJmxException, IOException {
        ObjectName objName = new ObjectName(objectName);
        if (objName.isPropertyPattern() || objName.isDomainPattern()) {
            Set<ObjectInstance> mBeans = connection.queryMBeans(objName, null);

            if (mBeans.size() == 0) {
                throw new InstanceNotFoundException();
            } else if (mBeans.size() > 1) {
                throw new CheckJmxException(
                        "Object name not unique: objectName pattern matches "
                                + mBeans.size() + " MBeans.");
            } else {
                objName = mBeans.iterator().next().getObjectName();
            }
        }
        return objName;
    }

    /**
     * Query MBean object.
     *
     * @param connection    MBean server connection.
     * @param objectName    Object name.
     * @param attributeName Attribute name.
     * @param attributeKey  Attribute key.
     * @return Value.
     * @throws InstanceNotFoundException    XX
     * @throws IntrospectionException       XX
     * @throws ReflectionException          XX
     * @throws IOException                  XX
     * @throws AttributeNotFoundException   XX
     * @throws MBeanException               XX
     * @throws MalformedObjectNameException XX
     * @throws CheckJmxException            XX
     */
    public Object query(MBeanServerConnection connection, String objectName,
                        String attributeName, String attributeKey)
            throws InstanceNotFoundException, IntrospectionException,
            ReflectionException, IOException, AttributeNotFoundException,
            MBeanException, MalformedObjectNameException,
            CheckJmxException {
        Object value = null;
        ObjectName objName = getObjectName(connection, objectName);

        Object attribute = connection.getAttribute(objName, attributeName);
        if (attribute instanceof CompositeDataSupport) {
            CompositeDataSupport compositeAttr =
                    (CompositeDataSupport) attribute;
            value = compositeAttr.get(attributeKey);
        } else {
            value = attribute;
        }
        return value;
    }

    /**
     * Invoke an operation on MBean.
     *
     * @param connection    MBean server connection.
     * @param objectName    Object name.
     * @param operationName Operation name.
     * @throws InstanceNotFoundException    XX
     * @throws IOException                  XX
     * @throws MalformedObjectNameException XX
     * @throws MBeanException               XX
     * @throws ReflectionException          XX
     * @throws CheckJmxException            XX
     */
    public void invoke(MBeanServerConnection connection, String objectName,
                       String operationName)
            throws InstanceNotFoundException, IOException,
            MalformedObjectNameException, MBeanException, ReflectionException,
            CheckJmxException {
        ObjectName objName = getObjectName(connection, objectName);
        connection.invoke(objName, operationName, null, null);
    }

    /**
     * Get system properties and execute query.
     *
     * @param args Arguments as properties.
     * @return Nagios exit code.
     * @throws CheckJmxException XX
     */
    public int execute(Properties args) throws CheckJmxException {
        String username = args.getProperty(PROP_USERNAME);
        String password = args.getProperty(PROP_PASSWORD);
        String objectName = args.getProperty(PROP_OBJECT_NAME);
        String attributeName = args.getProperty(PROP_ATTRIBUTE_NAME);
        String attributeKey = args.getProperty(PROP_ATTRIBUTE_KEY);
        String serviceUrl = args.getProperty(PROP_SERVICE_URL);
        String operation = args.getProperty(PROP_OPERATION);
        String help = args.getProperty(PROP_HELP);

        PrintStream out = System.out;

        if (help != null) {
            showHelp();
            return 0;
        }

        if (objectName == null || attributeName == null || serviceUrl == null) {
            showUsage();
            return -1;
        }

        JMXServiceURL url = null;
        try {
            url = new JMXServiceURL(serviceUrl);
        } catch (MalformedURLException e) {
            throw new CheckJmxException("Malformed service URL ["
                    + serviceUrl + "]", e);
        }
        // Connect to MBean server.
        MBeanServerConnection connection = null;
        Object value = null;
        try {
            try {
                connection = openConnection(url, username, password);
            } catch (ConnectException ce) {
                throw new CheckJmxException(
                        "Error opening RMI connection: " + ce.getMessage(), ce);
            } catch (Exception e) {
                throw new CheckJmxException(
                        "Error opening connection: " + e.getMessage(), e);
            }
            // Query attribute.
            try {
                value = query(connection, objectName, attributeName,
                        attributeKey);
            } catch (MalformedObjectNameException e) {
                throw new CheckJmxException(
                        "Malformed objectName [" + objectName + "]", e);
            } catch (InstanceNotFoundException e) {
                throw new CheckJmxException(
                        "objectName not found [" + objectName + "]", e);
            } catch (AttributeNotFoundException e) {
                throw new CheckJmxException(
                        "attributeName not found [" + attributeName + "]", e);
            } catch (InvalidKeyException e) {
                throw new CheckJmxException(
                        "attributeKey not found [" + attributeKey + "]", e);
            } catch (Exception e) {
                throw new CheckJmxException(
                        "Error querying server: " + e.getMessage(), e);
            }
            // Invoke operation if defined.
            if (operation != null) {
                try {
                    invoke(connection, objectName, operation);
                } catch (Exception e) {
                    throw new CheckJmxException(
                            "Error invoking operation [" + operation + "]: "
                                    + e.getMessage(), e);
                }
            }
        } finally {
            if (connection != null) {
                try {
                    closeConnection(connection);
                } catch (Exception e) {
                    throw new CheckJmxException("Error closing JMX connection", e);
                }
            }
        }

        if (value != null) {
            outputStatus(out, objectName, attributeName, attributeKey, value);
            out.println();
        } else {
            // TODO: output some sort of message here?
        }

        return 0;
    }

    /**
     * Main method.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) throws CheckJmxException {
        NagiosJmxPlugin plugin = new NagiosJmxPlugin();
        Properties props = parseArguments(args);
        plugin.execute(props);
        System.exit(0);
    }

    /**
     * Show usage.
     *
     * @throws CheckJmxException XX
     */
    private void showUsage() throws CheckJmxException {
        System.out.println("Usage: check_jmx -U <service_url> -O <object_name> -A <attribute_name>\n"
                + "    [-K <compound_key>] [-o <operation_name>] [--username <username>] [--password <password>]\n"
                + "    [-v] [-h]");
    }

    /**
     * Show help.
     *
     * @throws CheckJmxException XX
     */
    private void showHelp() throws CheckJmxException {
        System.out.println("Usage: check_jmx -U <service_url> -O <object_name> -A <attribute_name>\n"
                + "    [-K <compound_key>] [-o <operation_name>] [--username <username>] [--password <password>]\n"
                + "    [-v] [-h]\n"
                + "\n"
                + "Options are:\n"
                + "\n"
                + "-h\n"
                + "    Help page, this page.\n"
                + "	\n"
                + "-U \n"
                + "    JMX URL; for example: \"service:jmx:rmi://<host>:<port>/jndi/rmi://<host>:<port>/jmxrmi\" or \"service:jmx:pid://<pid>\"\n"
                + "	\n"
                + "-O \n"
                + "    Object name to be checked, for example, \"java.lang:type=Memory\"\n"
                + "    \n"
                + "-A\n"
                + "    Attribute name\n"
                + "	\n"
                + "-K \n"
                + "    Attribute key; use when attribute is a composite\n"
                + "	\n"
                + "-v\n"
                + "    verbose\n"
                + "\n"
                + "-o\n"
                + "    Operation to invoke on MBean after querying value. Useful to\n"
                + "    reset any statistics or counter.\n"
                + "\n"
                + "--username\n"
                + "    Username, if JMX access is restricted; for example \"monitorRole\"\n"
                + "	\n"
                + "--password\n"
                + "    Password\n");
    }

    /**
     * Parse command line arguments.
     *
     * @param args Command line arguments.
     * @return Command line arguments as properties.
     */
    private static Properties parseArguments(String[] args) {
        Properties props = new Properties();
        int i = 0;
        while (i < args.length) {
            if ("-h".equals(args[i])) {
                props.put(PROP_HELP, "");
            } else if ("-U".equals(args[i])) {
                props.put(PROP_SERVICE_URL, args[++i]);
            } else if ("-O".equals(args[i])) {
                props.put(PROP_OBJECT_NAME, args[++i]);
            } else if ("-A".equals(args[i])) {
                props.put(PROP_ATTRIBUTE_NAME, args[++i]);
            } else if ("-K".equals(args[i])) {
                props.put(PROP_ATTRIBUTE_KEY, args[++i]);
            } else if ("-v".equals(args[i])) {
                props.put(PROP_VERBOSE, "true");
            } else if ("--username".equals(args[i])) {
                props.put(PROP_USERNAME, args[++i]);
            } else if ("--password".equals(args[i])) {
                props.put(PROP_PASSWORD, args[++i]);
            } else if ("-u".equals(args[i])) {
                props.put(PROP_UNITS, args[++i]);
            } else if ("-o".equals(args[i])) {
                props.put(PROP_OPERATION, args[++i]);
            }
            i++;
        }
        return props;
    }

    private MBeanServerConnection getLocalMBeanServerConnection(int pid) {
        try {
            // Use reflection, so we don't have to mess around with weird provided dependencies
            Class<?> calClass = Class.forName("sun.management.ConnectorAddressLink");
            Method importFrom = calClass.getMethod("importFrom", int.class);

            String address = (String) importFrom.invoke(pid);
            JMXServiceURL jmxUrl = new JMXServiceURL(address);
            return JMXConnectorFactory.connect(jmxUrl).getMBeanServerConnection();
        } catch (IOException e) {
            throw new RuntimeException("Of course you still have to implement a good connection handling");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("\"importFrom\" method not found");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("\"sun.management.ConnectorAddressLink\" class not found");
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Problems invoking \"importFrom\" for PID " + pid);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Output status.
     *
     * @param out           Print stream.
     * @param objectName    Object name.
     * @param attributeName Attribute name.
     * @param attributeKey  Attribute key, or null
     * @param value         Value
     */
    private void outputStatus(PrintStream out, String objectName, String attributeName, String attributeKey, Object value) {
        StringBuilder output = new StringBuilder();
        output.append(attributeName);
        if (attributeKey != null) {
            output.append(".").append(attributeKey);
        }
        output.append(" = ").append(value);
        out.print(output.toString());
    }

}
