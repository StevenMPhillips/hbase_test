/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RunText implements Runnable {

  static Path path;
  static byte delim;
  static String toFind;
  static int index;
  static int numThreads;
  static Options o;

  private static AtomicInteger totalCount = new AtomicInteger();
  private static AtomicInteger runningThreads;

  public static void main(String[] args) throws Exception {
    o = new Options();
    JCommander jc = null;
    try {
      jc = new JCommander(o, args);
      jc.setProgramName("./runText");
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      String[] valid = {"-p", "path", "-d", "delimiter", "v", "value", "-i", "index"};
      new JCommander(o, valid).usage();
      System.exit(-1);
    }
    if (o.help) {
      jc.usage();
      System.exit(0);
    }
    path = new Path(o.path);
    delim = o.delimiter.getBytes()[0];
    toFind = o.value;
    index = o.index;
    numThreads = o.threads;
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    TextInputFormat format = new TextInputFormat();
    long len = fs.getFileStatus(path).getLen() / numThreads;

    List<Thread> threads = Lists.newArrayList();

    for (int i = 0; i < numThreads; i++) {
      FileSplit split = new FileSplit(path, i * len, len, new String[]{""});
      threads.add(new Thread(new RunText(split, format)));
    }

    runningThreads = new AtomicInteger(numThreads);

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

    fs.close();
  }

  private FileSplit split;
  private TextInputFormat format;

  public RunText(FileSplit split, TextInputFormat format) {
    this.split = split;
    this.format = format;
  }

  @Override
  public void run() {
    try {
      JobConf job = new JobConf();
      job.setInputFormat(format.getClass());
      RecordReader<LongWritable, Text> reader = format.getRecordReader(split, job, Reporter.NULL);
      Text value = reader.createValue();
      LongWritable key = reader.createKey();
      int count = 0;
      long t1 = System.nanoTime();
      while (reader.next(key, value)) {
        List<String> values = parse(value);
        if (values.get(index).equals(toFind)) {
          System.out.println(value);
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

  private static List<String> parse(Text value) {
    int p = 0;
    List<String> strings = Lists.newArrayList();
    while (p < value.getLength()) {
      int next = find(value, delim, p);
      if (next == -1) {
        break;
      }
      String s = new String(value.getBytes(), p, next - p);
      strings.add(s);
      p = next + 1;
    }
    return strings;
  }

  private static int find(Text text, byte what, int start) {
    int len = text.getLength();
    int p = start;
    byte[] bytes = text.getBytes();
    boolean inQuotes = false;
    while (p < len) {
      if ('\"' == bytes[p]) {
        inQuotes = !inQuotes;
      }
      if (!inQuotes && bytes[p] == what) {
        return p;
      }
      p++;
    }
    return -1;
  }


  static class Options {
    @Parameter(names = {"-p"}, description = "path", required=true)
    public String path = null;

    @Parameter(names = {"-d"}, description = "delimiter", required=true)
    public String delimiter = null;

    @Parameter(names = {"-v"}, description = "value", required=true)
    public String value = null;

    @Parameter(names = {"-i"}, description = "index", required=true)
    public int index = 0;

    @Parameter(names = {"-t"}, description = "threads", required=true)
    public int threads = 0;

    @Parameter(names = {"-h", "--help"}, description = "show usage", help=true)
    public boolean help = false;
  }
}
