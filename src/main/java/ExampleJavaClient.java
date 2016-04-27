import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class ExampleJavaClient {

  private static Connection connection;

  private String clientConfig;
  private String keytabLocation;
  private String user;
  private String table;

  private boolean verbose;

  public ExampleJavaClient(String clientConfig, String keytabLocation, String user, String table) {
    this.clientConfig = clientConfig;
    this.keytabLocation = keytabLocation;
    this.user = user;
    this.table = table;
  }

  public ExampleJavaClient verbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  public void initialise() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path(clientConfig));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keytabLocation);

    System.out.println(conf.toString());

    connection = ConnectionFactory.createConnection(conf);
  }

  public void doScan() throws IOException {
    Table tableRef = connection.getTable(TableName.valueOf(table));
    Scan scan = new Scan();
    ResultScanner scanner = tableRef.getScanner(scan);
    long now = System.currentTimeMillis();
    if (verbose) System.out.println("Starting scan");
    for (Result res : scanner) {
      if (verbose) System.out.println(res);
    }
    if (verbose) System.out.printf("Scan finished: %d ms\n\n", System.currentTimeMillis() - now);
    tableRef.close();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      System.err.printf("Usage: %s <conf> <keytab> <user> <table> <reps> [-v]\n", ExampleJavaClient.class);
      System.exit(-1);
    }

    ExampleJavaClient exampleJavaClient = new ExampleJavaClient(args [0], args[1], args[2], args[3]);
    exampleJavaClient.initialise();
    if (args.length == 6 && args[5].equals("-v")) {
      exampleJavaClient.verbose(true);
    }

    int reps = Integer.parseInt(args[4]);

    long start = System.currentTimeMillis();
    System.out.println("Beginning " + reps + " scans");
    for (int i=0; i<reps; ++i) {
      exampleJavaClient.doScan();
    }
    System.out.println("Runtime: " + (System.currentTimeMillis() - start) + "ms");
    System.out.println("Ended scans for " + args[2]);
  }

}
