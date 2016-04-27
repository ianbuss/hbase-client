import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Random;

public class ExampleProxyJavaClient {

  private static Connection connectionA;
  private static Connection connectionB;

  private String clientConfig;
  private String keytabLocation;
  private String user;
  private String table;

  private UserGroupInformation proxyUserA;
  private UserGroupInformation proxyUserB;

  boolean verbose = false;

  public ExampleProxyJavaClient(String clientConfig, String keytabLocation, String user, String table) {
    this.clientConfig = clientConfig;
    this.keytabLocation = keytabLocation;
    this.user = user;
    this.table = table;
  }

  public void initialise() throws IOException, InterruptedException {
    final Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path(clientConfig));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

    // Login as a user in the supplied keytab - this user must be authorised to proxy as
    // other users via the hadoop.proxyuser.theuser.hosts and hadoop.proxyuser.theuser.groups
    // properties in core-site.xml
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keytabLocation);

    // Create the proxy users
    proxyUserA = UserGroupInformation.createProxyUser("usera", UserGroupInformation.getLoginUser());
    proxyUserB = UserGroupInformation.createProxyUser("userb", UserGroupInformation.getLoginUser());

    System.out.println(conf.toString());

    // Open HBase connection objects as the proxy users - do this once per proxy user per JVM
    connectionA = proxyUserA.doAs(new PrivilegedExceptionAction<Connection>() {
      @Override
      public Connection run() throws Exception {
        return ConnectionFactory.createConnection(conf);
      }
    });
    connectionB = proxyUserB.doAs(new PrivilegedExceptionAction<Connection>() {
      @Override
      public Connection run() throws Exception {
        return ConnectionFactory.createConnection(conf);
      }
    });
  }

  public ExampleProxyJavaClient verbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  public void doScanAs(String as) throws IOException, InterruptedException {
    UserGroupInformation thisProxyUser;
    final Connection thisConnection;
    switch(as) {
      case "usera": thisProxyUser = proxyUserA; thisConnection = connectionA; break;
      case "userb": thisProxyUser = proxyUserB; thisConnection = connectionB; break;
      default: throw new IllegalArgumentException("Invalid proxy user: " + as);
    }

    // Run the scan as the appropriate user
    thisProxyUser.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        Table tableRef = thisConnection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner scanner = tableRef.getScanner(scan);
        long now = System.currentTimeMillis();
        if (verbose) System.out.println("Starting scan");
        for (Result res : scanner) {
          if (verbose) System.out.println(res.toString());
        }
        if (verbose) System.out.printf("Scan finished: %d ms\n\n", System.currentTimeMillis() - now);
        tableRef.close();
        return null;
      }
    });
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      System.err.printf("Usage: %s <conf> <user> <keytab> <table> <reps> [-v]\n", ExampleProxyJavaClient.class);
      System.exit(-1);
    }

    ExampleProxyJavaClient exampleJavaClient = new ExampleProxyJavaClient(args [0], args[1], args[2], args[3]);
    exampleJavaClient.initialise();
    if (args.length == 6 && args[5].equals("-v")) {
      exampleJavaClient.verbose(true);
    }

    int reps = Integer.parseInt(args[4]);

    long start = System.currentTimeMillis();
    System.out.println("Beginning only usera for " + reps + " scans");
    for (int i=0; i<reps; ++i) {
      exampleJavaClient.doScanAs("usera");
    }
    System.out.println("Runtime: " + (System.currentTimeMillis() - start) + "ms");
    System.out.println("Ended scans for usera");

    start = System.currentTimeMillis();
    System.out.println("Beginning only userb for " + reps + " scans");
    for (int i=0; i<reps; ++i) {
      exampleJavaClient.doScanAs("userb");
    }
    System.out.println("Runtime: " + (System.currentTimeMillis() - start) + "ms");

    start = System.currentTimeMillis();
    System.out.println("Beginning mixed scan workload for " + reps + " scans");
    Random random = new Random();
    int useraCount = 0;
    int userbCount = 0;
    for (int i=0; i<reps; ++i) {
      double d = random.nextGaussian();
      if (d < 0) {
        ++useraCount;
        exampleJavaClient.doScanAs("usera");
      } else {
        ++userbCount;
        exampleJavaClient.doScanAs("userb");
      }
    }
    System.out.println("Runtime: " + (System.currentTimeMillis() - start) + "ms");
    System.out.println("A: " + useraCount + ", B: " + userbCount);
  }

}
