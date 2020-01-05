/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import tl.lin.data.pair.PairOfStringLong;
import tl.lin.data.pair.PairOfLongInt;
import org.apache.hadoop.io.WritableUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.List;
import java.util.ArrayList;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class BooleanRetrievalCompressed extends Configured implements Tool {
  private List<MapFile.Reader> indexArray = new ArrayList<>();
  private FSDataInputStream collection;
  private Stack<Set<Long>> stack;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
    //Read multiple MapFile indices
    FileStatus [] fstatus = fs.listStatus(new Path(indexPath));
    for(int i=0;i< fstatus.length; i++){
      if(!fstatus[i].getPath().toString().contains("_SUCCESS")){
        indexArray.add(new MapFile.Reader(fstatus[i].getPath(), fs.getConf()));
      }
    }
    collection = fs.open(new Path(collectionPath));
    stack = new Stack<>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Long> set = stack.pop();

    for (Long i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    //bug: may throw empty stack exception
    Set<Long> s1 = stack.pop();
    Set<Long> s2 = stack.pop();

    Set<Long> sn = new TreeSet<>();

    for (long n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Long> s1 = stack.pop();
    Set<Long> s2 = stack.pop();

    Set<Long> sn = new TreeSet<>();

    for (long n : s1) {
      sn.add(n);
    }

    for (long n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Long> fetchDocumentSet(String term) throws IOException {
    Set<Long> set = new TreeSet<>();
    for (PairOfLongInt pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfLongInt> fetchPostings(String term) throws IOException {
    Text key = new Text();
    BytesWritable value = new BytesWritable();
    //PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
    //    new PairOfWritables<>();

    //Find out which partition does the key hash to
    int partition = (term.hashCode() & Integer.MAX_VALUE) % indexArray.size();
    key.set(term);
    indexArray.get(partition).get(key, value);

    ArrayListWritable<PairOfLongInt> postings = new ArrayListWritable<PairOfLongInt>();
    byte [] byteArray = value.copyBytes();
    DataInputStream dis = new DataInputStream( new ByteArrayInputStream(byteArray));
    int df = 0;
    try{
      df = WritableUtils.readVInt(dis); //document frequency = number of postings byte array
    }
    catch(EOFException e){
      System.out.println("WARNING: Empty postings byte array");
      return postings;
    }
    int EOS = 0; //end of stream indicator
    int count_postings = 0;
    long prev_doc_id = 0;
    while(EOS==0){
      //bug: may read from empty stream
      try{
        long doc_id = WritableUtils.readVLong(dis) + prev_doc_id; //document id
        int tf = WritableUtils.readVInt(dis); //term frequency
        PairOfLongInt element = new PairOfLongInt(doc_id, tf);
        postings.add(element);
        prev_doc_id = doc_id;
        count_postings++;
      }
      catch(EOFException e){
        EOS = 1;
        dis.close();
        if(count_postings!=df){
          System.out.println("Error: Incorrect parsing of byte stream of postings");
          System.exit(1);
        }
      }
    }
    
    //return value.getRightElement();
    return postings;
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
