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

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import tl.lin.data.pair.PairOfStringLong;
import tl.lin.data.pair.PairOfLongInt;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapperIndex extends Mapper<LongWritable, Text, PairOfStringLong , VIntWritable> {
    private static final Text WORD = new Text();
    private static final VIntWritable TF = new VIntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        WORD.set(e.getLeftElement());
        TF.set(e.getRightElement());
        context.write(new PairOfStringLong(WORD.toString(),docno.get()),TF);
        //context.write(WORD, new PairOfInts((int) docno.get(), e.getRightElement()));
      }
    }
  }

  private static final class MyReducerIndex extends
      Reducer<PairOfStringLong, VIntWritable, Text, BytesWritable>{ //PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {
    //Member initialization
    private static final Text PREV_TERM = new Text("");
    private static final VIntWritable DF = new VIntWritable(0);
    private static final VIntWritable TF = new VIntWritable();
    private static final VLongWritable DOC_ID = new VLongWritable();
    private static final LongWritable PREV_DOC_ID = new LongWritable(0);
    private static List<PairOfLongInt> postings = new ArrayListWritable<>();
    private static BytesWritable bosWritable = new BytesWritable();
    private static ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private static DataOutputStream dos = new DataOutputStream(bos);

    @Override
    public void reduce(PairOfStringLong key, Iterable<VIntWritable> values, Context context)
        throws IOException, InterruptedException {
      //check if term has changed
      String term = key.getLeftElement();
      if(!( term.equals( PREV_TERM.toString() ) || term.equals("") )){
        //write previous term posting to disk and update variables
        DF.write(dos); //write df to bytestream
        for(PairOfLongInt posting: postings){
          DOC_ID.set(posting.getLeftElement());
          TF.set(posting.getRightElement());
          DOC_ID.write(dos);
          TF.write(dos);
        }
        bosWritable.set(bos.toByteArray(),0,bos.size());
        //write to disk
        context.write(PREV_TERM,bosWritable);
        //reset variables
        DF.set(0);
        postings.clear();
        PREV_TERM.set("");
        PREV_DOC_ID.set(0);
        dos.flush();
        bos.reset();
        bosWritable.setSize(0);
      }

      Long docID = key.getRightElement();
      Iterator<VIntWritable> iter = values.iterator();
      int tf = 0; //term frequency
      while (iter.hasNext()) {
        tf += iter.next().get();
      }
      int df = DF.get(); //increment document frequency
      df++;
      DF.set(df);
      //append to postings
      postings.add(new PairOfLongInt( docID-PREV_DOC_ID.get(), tf )); //delta compresssion
      PREV_DOC_ID.set(docID);
      PREV_TERM.set(term);
    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException{
      //write previous term posting to disk and update variables
      if(DF.get()>0){
        DF.write(dos); //write df to bytestream
        for(PairOfLongInt posting: postings){
          DOC_ID.set(posting.getLeftElement());
          TF.set(posting.getRightElement());
          DOC_ID.write(dos);
          TF.write(dos);
        }
        bosWritable.set(bos.toByteArray(),0,bos.size());
        //write to disk
        context.write(PREV_TERM,bosWritable);
      }
      //reset variables
      DF.set(0);
      postings.clear();
      PREV_TERM.set("");
      PREV_DOC_ID.set(0);
      dos.flush();
      bos.reset();
      bosWritable.setSize(0);
    }
  }

  public static final class MyPartitionerIndex extends Partitioner<PairOfStringLong,VIntWritable>{
    @Override
    public int getPartition(PairOfStringLong key, VIntWritable value, int numReduceTasks){
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;
    
    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
	  int numReducers = 1;
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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringLong.class);
    job.setMapOutputValueClass(VIntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapperIndex.class);
    job.setReducerClass(MyReducerIndex.class);
    job.setPartitionerClass(MyPartitionerIndex.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
