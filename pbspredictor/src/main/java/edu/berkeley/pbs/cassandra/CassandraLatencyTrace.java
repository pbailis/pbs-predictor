package edu.berkeley.pbs.cassandra;

import edu.berkeley.pbs.trace.PBSLatencyTrace;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import com.yammer.metrics.reporting.JmxReporter;
import java.io.IOException;
import java.util.Random;

public class CassandraLatencyTrace implements PBSLatencyTrace {

    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final String fmtObject = "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=%s,scope=%s,name=%s";
    private static final String readName = "ReadLatency";
    private static final String writeName = "WriteLatency";

    private Random random = new Random();

    double[] readValues, writeValues;

    private static JmxReporter.HistogramMBean readReporter;
    private static JmxReporter.HistogramMBean writeReporter;

    private JMXConnector jmxc = null;
    private MBeanServerConnection mBeanServerConnection = null;

    public CassandraLatencyTrace(String cassHost,
                                 int cassPort,
                                 String cassKeyspace,
                                 String cassColumnFamily) throws IOException,
                                                                 MalformedObjectNameException {
        connect(cassHost, cassPort, cassKeyspace, cassColumnFamily);
        refresh();
    }

    private void connect(String cassHost,
                         int cassPort,
                         String cassKeyspace,
                         String cassColumnFamily) throws IOException, MalformedObjectNameException {
        if(jmxc == null) {
            jmxc = JMXConnectorFactory.connect(new JMXServiceURL(String.format(fmtUrl,
                                                                               cassHost,
                                                                               cassPort)));
            mBeanServerConnection = jmxc.getMBeanServerConnection();
        }

        readReporter = JMX.newMBeanProxy(mBeanServerConnection,
                                         new ObjectName(String.format(fmtObject,
                                                                      cassKeyspace,
                                                                      cassColumnFamily,
                                                                      readName)),
                                         JmxReporter.HistogramMBean.class);

        writeReporter = JMX.newMBeanProxy(mBeanServerConnection,
                                         new ObjectName(String.format(fmtObject,
                                                                      cassKeyspace,
                                                                      cassColumnFamily,
                                                                      writeName)),
                                         JmxReporter.HistogramMBean.class);

    }

    public double getNextWValue() {
        return writeValues[random.nextInt(writeValues.length)]/2;
    }

    public double getNextAValue() {
        return writeValues[random.nextInt(writeValues.length)]/2;
    }

    public double getNextRValue() {
        return readValues[random.nextInt(writeValues.length)]/2;
    }

    public double getNextSValue() {
        return readValues[random.nextInt(writeValues.length)]/2;
    }

    public void refresh() throws IOException {
        readValues = readReporter.values();
        writeValues = writeReporter.values();

        if(readValues.length == 0) {
            throw new IOException("no read latencies recorded!");
        }

        if(writeValues.length == 0) {
            throw new IOException("no write latencies recorded!");
        }
    }

    public void close() throws IOException {
        if(jmxc != null) {
            jmxc.close();
            jmxc = null;
        }
    }
}