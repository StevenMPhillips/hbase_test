import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RunHBase implements Runnable {
  static Configuration conf = new Configuration();

  static Options o;

  private String start;
  private String end;


  private static AtomicInteger totalCount = new AtomicInteger();
  private static AtomicInteger runningThreads;

  public RunHBase(String start, String end) {
    this.start = start;
    this.end = end;
  }

  public void run() {
    try {
      HTable table = new HTable(conf, o.table);
      Scan s = new Scan();
      if (start != null) {
        s.setStartRow(Bytes.toBytesBinary(start));
      }
      if (end != null) {
        s.setStopRow(Bytes.toBytesBinary(end));
      }
      ResultScanner rs = table.getScanner(s);
      Result result;
      int count = 0;
      while ((result = rs.next()) != null) {
        byte[] l_orderkey = result.getRow();
        if (Arrays.equals(l_orderkey, o.value.getBytes())) {
          System.out.println(result);
        }
        count++;
        if (count == 100) {
          totalCount.addAndGet(100);
          count = 0;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      runningThreads.decrementAndGet();
    }
  }

  public static void main(String[] args) throws Exception {
    o = new Options();
    JCommander jc = null;
    try {
      jc = new JCommander(o, args);
      jc.setProgramName("./runHBase");
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      String[] valid = {"-t", "table", "-f", "family", "q", "qualifier", "-v", "value"};
      new JCommander(o, valid).usage();
      System.exit(-1);
    }
    if (o.help) {
      jc.usage();
      System.exit(0);
    }

    List<String> splits = Lists.newArrayList();
    List<Thread> threads = Lists.newArrayList();
    if (o.splitsFile != null) {
      BufferedReader reader = new BufferedReader(new FileReader(o.splitsFile));
      String line;
      while ((line = reader.readLine()) != null) {
        splits.add(line);
      }
    }

    if (splits.size() > 0) {
      threads.add(new Thread(new RunHBase(null, splits.get(0))));

      for (int i = 0; i < splits.size() - 1; i++) {
        threads.add(new Thread(new RunHBase(splits.get(i), splits.get(i + 1))));
      }

      threads.add(new Thread(new RunHBase(splits.get(splits.size() - 1), null)));
    } else {
      threads.add(new Thread(new RunHBase(null, null)));
    }

    runningThreads = new AtomicInteger(threads.size());

    for (Thread t : threads) {
      t.start();
    }

    int prev = 0;
    int current;
    long t1 = System.nanoTime();
    long t2;
    while (runningThreads.get() > 0) {
      Thread.sleep(5000);
      current = totalCount.get();
      t2 = System.nanoTime();
      System.out.println(String.format("%f records/sec", (current - prev) * 1e9 / (t2 - t1)));
      t1 = t2;
      prev = current;
    }

    for (Thread t: threads) {
      t.join();
    }
  }

  static class Options {
    @Parameter(names = {"-t"}, description = "table", required=true)
    public String table = null;

    @Parameter(names = {"-f"}, description = "family", required=true)
    public String family = null;

    @Parameter(names = {"-q"}, description = "qualifier", required=true)
    public String qualifier = null;

    @Parameter(names = {"-v"}, description = "value", required=true)
    public String value = null;

    @Parameter(names = {"-s"}, description = "splits file", required=false)
    public String splitsFile = null;

    @Parameter(names = {"-h", "--help"}, description = "show usage", help=true)
    public boolean help = false;
  }
}
