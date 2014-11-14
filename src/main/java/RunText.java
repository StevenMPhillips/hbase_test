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

public class RunText {

  static Path path;
  static byte delim;
  static String toFind;
  static int index;
  static Options o;

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
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    TextInputFormat format = new TextInputFormat();
    FileSplit split = new FileSplit(path, 0L, fs.getFileStatus(path).getLen(), new String[] {""});
    JobConf job = new JobConf();
    job.setInputFormat(format.getClass());
    RecordReader<LongWritable,Text> reader = format.getRecordReader(split, job, Reporter.NULL);
    Text value = reader.createValue();
    LongWritable key = reader.createKey();
    int count = 0;
    long t1 = System.nanoTime();
    while(reader.next(key, value)) {
      List<String> values = parse(value);
      if (values.get(index).equals(toFind)) {
        System.out.println(value);
      }
      count++;
    }
    long t2 = System.nanoTime();
    System.out.printf("Read %d records in %f seconds. %f records per second", count, (t2 - t1) / 1E9, count * 1E9 / (t2 - t1));
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

    @Parameter(names = {"-h", "--help"}, description = "show usage", help=true)
    public boolean help = false;
  }
}
