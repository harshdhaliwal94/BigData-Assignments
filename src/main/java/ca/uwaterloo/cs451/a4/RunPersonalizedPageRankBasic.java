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

package ca.uwaterloo.cs451.a4;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 *
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateMass = new PageRankNode();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();

    private int numSources=1;
    private int nodeCnt = 0;
    private String srcListCsv = "";
    private List srcList;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      nodeCnt = conf.getInt("NodeCount", 0);
      srcListCsv = conf.get("SourceList", "");
      if (nodeCnt == 0||srcListCsv.equals("")) {
        throw new RuntimeException("num nodes cannot be 0 and source node list or missing mass can't be empty!");
      }
      //Parse source list
      String[] srcListStringArr = srcListCsv.trim().replaceAll("\\s+","").split(",");
      srcList = new ArrayList<Integer>();
      for(int i=0;i<srcListStringArr.length;i++)
        srcList.add(Integer.parseInt(srcListStringArr[i]));
      numSources = srcList.size();
    }


    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacencyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;
      float mass = Float.NEGATIVE_INFINITY;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacencyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacencyList();
        
        //if(node.getPageRank()!=Float.NEGATIVE_INFINITY)
        //  mass = node.getPageRank() - (float) StrictMath.log(list.size());
        //else
        //  mass = Float.NEGATIVE_INFINITY;
        ArrayListOfFloatsWritable massList = node.getPageRank();
        //if(massList.size()!=numSources){
        //  System.out.println("*******************************************");
        //  System.out.println("********Error: incorrect list length*******");
        //  System.out.println("*******************************************");
        //}
        ArrayList<Float> massListOut = new ArrayList<Float>();
        for(int k=0; k<numSources;k++)
          massListOut.add(Float.NEGATIVE_INFINITY);

        for(int k=0; k< massList.size(); k++){
          if(massList.get(k)!=Float.NEGATIVE_INFINITY)
            massListOut.set(k,massList.get(k) - (float) StrictMath.log(list.size()));
          else
            massListOut.set(k,Float.NEGATIVE_INFINITY);
        }
        float[] massListOutArr = new float[massListOut.size()];
        int k =0;
        for(Float f : massListOut)
          massListOutArr[k++] = (f!=null? f:Float.NaN);

        context.getCounter(PageRank.edges).increment(list.size());

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(PageRankNode.Type.Mass);
          //intermediateMass.setPageRank(mass);
          intermediateMass.setPageRank(new ArrayListOfFloatsWritable(massListOutArr));
          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static final PageRankNode intermediateMass = new PageRankNode();

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;

      // Remember, PageRank mass is stored as a log prob.
      //float mass = Float.NEGATIVE_INFINITY;
      ArrayList<Float> massListOut = new ArrayList<Float>();
      for (PageRankNode n : values) {
        ArrayListOfFloatsWritable massList = n.getPageRank();
        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
            if(massListOut.isEmpty()){
              for(int i=0;i<massList.size();i++)
                massListOut.add(massList.get(i));
            }
            else{
              // Accumulate PageRank mass contributions.
              for(int i=0;i<massList.size();i++){
                float temp_prev = (float)massListOut.get(i);
                massListOut.set(i, sumLogProbs(temp_prev, (float)massList.get(i)));
              }
            }
            massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0 && !massListOut.isEmpty()) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        //intermediateMass.setPageRank(mass);
        float[] massListOutArr = new float[massListOut.size()];
        int k=0;
        for(Float f: massListOut) 
          massListOutArr[k++] = (f!= null ? f : Float.NaN);
        intermediateMass.setPageRank(new ArrayListOfFloatsWritable(massListOutArr));

        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    //private float totalMass = Float.NEGATIVE_INFINITY;
    private ArrayList<Float> totalMass = new ArrayList<Float>();
    private int numSources=1;
    private int nodeCnt = 0;
    private String srcListCsv = "";
    private List srcList;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      nodeCnt = conf.getInt("NodeCount", 0);
      srcListCsv = conf.get("SourceList", "");
      if (nodeCnt == 0||srcListCsv.equals("")) {
        throw new RuntimeException("num nodes cannot be 0 and source node list or missing mass can't be empty!");
      }
      //Parse source list
      String[] srcListStringArr = srcListCsv.trim().replaceAll("\\s+","").split(",");
      srcList = new ArrayList<Integer>();
      for(int i=0;i<srcListStringArr.length;i++)
        srcList.add(Integer.parseInt(srcListStringArr[i]));
      numSources = srcList.size();
      for(int k=0; k<numSources;k++)
          totalMass.add(Float.NEGATIVE_INFINITY);
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<PageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      PageRankNode node = new PageRankNode();

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      //float mass = Float.NEGATIVE_INFINITY;
      ArrayList<Float> massListOut = new ArrayList<Float>();
      for(int k=0; k<numSources;k++)
          massListOut.add(Float.NEGATIVE_INFINITY);

      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacencyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.
          //mass = sumLogProbs(mass, n.getPageRank());
          //massMessagesReceived++;
            ArrayListOfFloatsWritable massList = n.getPageRank();
            //if(massList.size()!=numSources){
            //  System.out.println("*******************************************");
            //  System.out.println("********Error: incorrect list length*******");
            //  System.out.println("*******************************************");
            //}
            if(massListOut.isEmpty()){
              for(int i=0;i<massList.size();i++)
                massListOut.add(massList.get(i));
            }
            else{
              // Accumulate PageRank mass contributions.
              for(int i=0;i<massList.size();i++){
                float temp_prev = (float)massListOut.get(i);
                massListOut.set(i, (float)sumLogProbs(temp_prev, (float)massList.get(i)));
              }
            }
            massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      //node.setPageRank(mass);
      //if(massListOut.isEmpty()){
      //  for(int k=0; k<numSources;k++)
      //    massListOut.add(Float.NEGATIVE_INFINITY);
      //}
      float[] massListOutArr = new float[massListOut.size()];
      int k=0;
      for(Float f: massListOut) 
        massListOutArr[k++] = (f!= null ? f : Float.NaN);
      node.setPageRank(new ArrayListOfFloatsWritable(massListOutArr));
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

        // Keep track of total PageRank mass.
        //totalMass = sumLogProbs(totalMass, mass);
        if(totalMass.isEmpty()){
          for(int i=0;i<massListOut.size();i++)
            totalMass.add(massListOut.get(i));
        }
        else{
        // Accumulate PageRank mass contributions.
          for(int i=0;i<massListOut.size();i++){
            float temp_prev = (float)totalMass.get(i);
            totalMass.set(i, sumLogProbs(temp_prev, (float)massListOut.get(i)));
          }
        }
        
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      DataOutputStream dout = new DataOutputStream(out);
      float[] totalMassArr = new float[totalMass.size()];
      int k=0;
      for(Float f: totalMass) 
        totalMassArr[k++] = (f!= null ? f : Float.NaN);
      ArrayListOfFloatsWritable totalMassWritable = new ArrayListOfFloatsWritable(totalMassArr);
      //System.out.println("==========================================================");
      //System.out.println("Phase1 total mass written: " + Arrays.toString(totalMassArr));
      //System.out.println("==========================================================");
      totalMassWritable.write(dout);
      dout.close();
      //totalMass.clear();
      //out.writeFloat(totalMass);
      //out.close();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    //private float missingMass = 0.0f;
    private ArrayList<Float> missingMass = new ArrayList<Float>();
    private int nodeCnt = 0;
    private String srcListCsv = "";
    //int[] srcList;
    private List srcList;
    private int numSources=1;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      String missing = conf.get("MissingMass", "");
      nodeCnt = conf.getInt("NodeCount", 0);
      srcListCsv = conf.get("SourceList", "");
      if (nodeCnt == 0||srcListCsv.equals("")||missing.equals("")) {
        throw new RuntimeException("num nodes cannot be 0 and source node list or missing mass can't be empty!");
      }
      //Parse source list
      String[] srcListStringArr = srcListCsv.trim().replaceAll("\\s+","").split(",");
      String[] missingArr = missing.split(",");
      //srcList = new int[srcListStringArr.length];
      srcList = new ArrayList<Integer>();
      for(int i=0;i<srcListStringArr.length;i++){
        //srcList[i]=Integer.parseInt(srcListStringArr[i]);
        srcList.add(Integer.parseInt(srcListStringArr[i]));
        missingMass.add(Float.parseFloat(missingArr[i]));
      }
      numSources = srcList.size();
      //System.out.println("==========================================================");
      //System.out.println("Phase2 missing mass read: "+missingMass.toString());
      //System.out.println("==========================================================");
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      //float p = node.getPageRank();
      // if(nid.get()==srcList[0]){
      //   float jump = (float) (Math.log(ALPHA));
      //   float link = (float) Math.log(1.0f - ALPHA)
      //     + sumLogProbs(p, (float) (Math.log(missingMass)));
      //   p = sumLogProbs(jump, link);
      //   node.setPageRank(p);
      // }
      // else{
      //   float jump = (float) Math.log(1.0f - ALPHA) + p;
      //   node.setPageRank(jump);
      // }
      ArrayListOfFloatsWritable p = node.getPageRank();
      //if(p.size()!=numSources){
      //    System.out.println("*******************************************");
      //    System.out.println("********Error: incorrect list length*******");
      //    System.out.println("*******************************************");
      //  }
      ArrayList<Float> newMass = new ArrayList<Float>();
      for(int k =0; k<missingMass.size();k++){
          float jump = (float) Math.log(1.0f - ALPHA) + p.get(k);
          newMass.add(jump);
        }
      
      if(srcList.contains(nid.get())){
        int k = srcList.indexOf(nid.get());
        float jump = (float) (Math.log(ALPHA));
        float link = (float) Math.log(1.0 - ALPHA)
          + sumLogProbs(p.get(k),(float)(missingMass.get(k) == 0 ? Float.NEGATIVE_INFINITY: Math.log((float)missingMass.get(k))));
        float new_rank = sumLogProbs(jump,link);
        newMass.set(k,new_rank);
      }  
      float[] newMassArr = new float[newMass.size()];
      int k=0;
      for(Float f: newMass) 
        newMassArr[k++] = (f!= null ? f : Float.NaN);
      node.setPageRank(new ArrayListOfFloatsWritable(newMassArr));
      context.write(nid, node);
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }

  public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("list").hasArg()
        .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES)|| !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    String srcNodes = cmdline.getOptionValue(SOURCES);
  

    LOG.info("Tool name: RunPersonalizedPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - source nodes: " + srcNodes);

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, srcNodes);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes, String srcNodes) 
    throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    //float mass = phase1(i, j, basePath, numNodes, srcNodes);
    ArrayList<Float> mass;
    mass = phase1(i, j, basePath, numNodes, srcNodes);
    //System.out.println("==========================================================");
    //System.out.println("Iteration missing mass read: "+mass.toString());
    //System.out.println("==========================================================");
    // Find out how much PageRank mass got lost at the dangling nodes.
    ArrayList<Float> missing = new ArrayList<Float>();
    for(int k=0;k<mass.size();k++){
      missing.add(1.0f - (float) StrictMath.exp(mass.get(k)));
    }
    //System.out.println("==========================================================");
    //System.out.println("Iteration missing mass calculated: "+missing.toString());
    //System.out.println("==========================================================");
    //float missing = 1.0f - (float) StrictMath.exp(mass);

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(i, j, missing, basePath, numNodes, srcNodes);
  }

  private ArrayList<Float> phase1(int i, int j, String basePath, int numNodes,
      String srcNodes) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    LOG.info(" - sourceNodes: " + srcNodes);
    LOG.info("computed number of partitions: " + numPartitions);

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().set("SourceList", srcNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapClass.class);

    job.setCombinerClass(CombineClass.class);

    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    //float mass = Float.NEGATIVE_INFINITY;
    ArrayList<Float> massList = new ArrayList<Float>();
    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      DataInputStream din = new DataInputStream(fin);
      //mass = sumLogProbs(mass, fin.readFloat());
      //fin.close();
      ArrayListOfFloatsWritable massListIn = new ArrayListOfFloatsWritable();
      massListIn.readFields(din);
      if(massList.isEmpty()){
        for(int k=0;k<massListIn.size();k++)
          massList.add(massListIn.get(k));
      }
      else{
      // Accumulate PageRank mass contributions.
        for(int k=0;k<massListIn.size();k++)
          massList.set(k, sumLogProbs(massList.get(k), massListIn.get(k)));
      }
      din.close();
    }
    //System.out.println("=============================================================");
    //System.out.println("Phase 1 : total mass read: " + Arrays.toString(massList.toArray()));
    //System.out.println("=============================================================");
    return massList;
  }

  private void phase2(int i, int j, ArrayList<Float> missing, String basePath, int numNodes, String srcNodes) 
    throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    LOG.info("missing PageRank mass: " + missing.toString());
    LOG.info("number of nodes: " + numNodes);
    LOG.info("source nodes: " + srcNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);
    String s1 = Arrays.toString(missing.toArray());
    s1 = s1.replaceAll("\\[","");
    s1 = s1.replaceAll("\\]","");
    s1 = s1.replaceAll("\\s+","");

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().setFloat("MissingMass", (float) missing);
    job.getConfiguration().set("MissingMass",s1);
    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().set("SourceList", srcNodes);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
