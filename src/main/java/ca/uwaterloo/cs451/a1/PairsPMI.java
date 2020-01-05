package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.MapKF;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


public class PairsPMI extends Configured implements Tool{
	private static final Logger LOG = Logger.getLogger(PairsPMI.class);
	private static enum LineCount{CNT}
	
	//Stage 1 job: Counting word frequencies
	//Stage 1: Mapper
	private static final class MyMapperCount extends Mapper<LongWritable, Text, Text, IntWritable>{
		private static final IntWritable ONE = new IntWritable(1);
		private static final Text WORD = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
			Set<String> word_set = new HashSet<String>();
			List<String>word_list = Tokenizer.tokenize(value.toString());
			//emit pairs of words for upto 40 words in the line
			int loop_count = Math.min(word_list.size(),40);
			for(int i=0;i<loop_count;i++){
				String word = word_list.get(i);
				if(word_set.add(word)){
					WORD.set(word);
					context.write(WORD,ONE);
				}
			}
			//for(String word: Tokenizer.tokenize(value.toString())){
			//	if(word_set.add(word)){
			//		WORD.set(word);
			//		context.write(WORD,ONE);
			//	}
			//}
			//for counting total number of lines
			WORD.set("*");
			context.write(WORD,ONE);
		}
	}

	//Stage 1: Reducer
	private static final class MyReducerCount extends Reducer<Text, IntWritable, Text, IntWritable>{
		// Reuse objects.
	    private static final IntWritable SUM = new IntWritable();
	    private static final Text NUMLINES = new Text("*");

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException{
			long sum=0;
			Iterator<IntWritable> itr = values.iterator();
			while(itr.hasNext()){
				sum += itr.next().get();
			}
			//Writing total number of lines to a counter
			if(key.equals(NUMLINES)){
				context.getCounter(LineCount.CNT).increment(sum);
			}
			else{
				SUM.set((int)sum);
				context.write(key, SUM);
			}
		}
	}

	//Stage 2: Pairs PMI
	//Stage 2: Mapper
	private static final class MyMapperPMI extends Mapper<LongWritable, Text, PairOfStrings, IntWritable>{//PairOfFloatInt>{
		private static final IntWritable ONE = new IntWritable(1);
		private static final PairOfStrings PAIR = new PairOfStrings();
		//private static final PairOfFloatInt PMICNT = new PairOfFloatInt();
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
			Set<String> word_set = new HashSet<String>();
			List<String>word_list1 = Tokenizer.tokenize(value.toString());
			int loop_count = Math.min(word_list1.size(), 40);
			for(int i=0; i<loop_count;i++){
				String word = word_list1.get(i);
				if(word_set.add(word))
					continue;
			}
			//for(String word: Tokenizer.tokenize(value.toString())){
			//	if(word_set.add(word))
			//		continue;
			//}
			//get unique word list
			List<String>word_list = new ArrayList<String>(word_set);
			//emit pairs of words
			//int loop_count = Math.min(word_list.size(),40);
			for(int i=0;i<word_list.size();i++){
				for(int j=0;j<word_list.size();j++){
					if(i==j)
						continue;
					PAIR.set(word_list.get(i),word_list.get(j));
					//PMICNT.set(0,1);
					//context.write(PAIR,PMICNT);
					context.write(PAIR,ONE);
				}
			}

		}
	}

	//Stage 2: Combiner
	private static final class MyCombinerPMI extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable>{//PairOfFloatInt, PairOfStrings, PairOfFloatInt> {
		//private static final PairOfFloatInt PMICNT = new PairOfFloatInt();
		private static final IntWritable SUM = new IntWritable();
		@Override
		public void reduce(PairOfStrings key,Iterable<IntWritable> values, Context context) //Iterable<PairOfFloatInt> values, Context context)
			throws IOException, InterruptedException {
			int sum = 0;
			//Iterator<PairOfFloatInt> iter = values.iterator();
			Iterator<IntWritable> iter = values.iterator();
			while (iter.hasNext()) {
				//sum += iter.next().getRightElement();
				sum += iter.next().get();
			}
			//PMICNT.set(0,sum);
			SUM.set(sum);
			//context.write(key, PMICNT);
			context.write(key, SUM);
		}
	}

	//Stage 2: Reducer

	private static final class MyReducerPMI extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt>{//PairOfFloatInt, PairOfStrings, PairOfFloatInt>{
		private static Map<String, Integer>wordCount = new HashMap<String, Integer>();
		private static PairOfFloatInt PMICNT = new PairOfFloatInt();
		private float totalLines;
		private int threshhold;

		@Override
		public void setup(Context context) throws IOException{
			totalLines = context.getConfiguration().getLong("DOCLINES",1);
			threshhold = context.getConfiguration().getInt("threshhold",10);
			try{
				Configuration conf = context.getConfiguration();
				FileSystem fs = FileSystem.get(conf);
				//Path countsFile = new Path("./countsFile/part-r-00000");
				FileStatus[] fstat = fs.listStatus(new Path("./countsFile/part-r-00000"));
				for(int i=0;i<fstat.length;i++){
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fstat[i].getPath())));
					String line;
					line = reader.readLine();
					while(line!=null){
						String[] keyVal = line.split("\\s+");
        				if(keyVal.length != 2){
          					LOG.info("Input line did not have exactly 2 tokens: '" + line + "'");
        				} 
        				else {
          						wordCount.put(keyVal[0], Integer.parseInt(keyVal[1]));
        				}
        				line = reader.readLine();
					}
					reader.close();
				}
			}catch(Exception e){
				System.out.println("Error: Intermediate File countsFile not found");
			} 
		}

		@Override
		public void reduce(PairOfStrings pair, Iterable<IntWritable> values, Context context)//Iterable<PairOfFloatInt> values, Context context)
		throws IOException, InterruptedException{
			int sumPair=0;
			//Iterator<PairOfFloatInt> itr = values.iterator();
			Iterator<IntWritable> itr = values.iterator();
			while(itr.hasNext()){
				//sumPair+=itr.next().getRightElement();
				sumPair+=itr.next().get();
			}
			if(sumPair>=threshhold){
				String first = pair.getLeftElement();
				String second = pair.getRightElement();

				float pair_prob = ((float)sumPair)/totalLines;
				float first_prob = wordCount.get(first)/totalLines;
				float second_prob = wordCount.get(second)/totalLines;
				float pmi = (float)Math.log10(pair_prob/(first_prob*second_prob));

				PMICNT.set(pmi,sumPair);
				context.write(pair,PMICNT);
			}
		}
	}

	private static final class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {//PairOfFloatInt> {
		@Override
    		public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {//PairOfFloatInt value, int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	  /**
   * Creates an instance of this tool.
   */
  	private PairsPMI() {}

  	private static final class Args {
	    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
	    String input;

	    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
	    String output;

	    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
	    int numReducers = 1;

	    @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
	    boolean textOutput = false;
	    
	    @Option(name = "-threshold", metaVar = "[num]", usage = "threshhold for PMI")
	    int threshold = 10;
	   
	    @Option(name = "-cmboff", usage = "turn off combiner (otherwise combiner is on)")
    	    boolean cmboff = false;
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

	    String countsFile = "./countsFile";

	    LOG.info("Tool name: " + PairsPMI.class.getSimpleName()+" Stage-1");
	    LOG.info(" - input path: " + args.input);
	    LOG.info(" - output path: " + countsFile);
	    LOG.info(" - num reducers: " + 1);
	    LOG.info(" - text output: " + args.textOutput);
	    LOG.info(" - threshold: " + args.threshold);
	    //LOG.info(" - combiner: " + args.cmboff);

	    Job job1 = Job.getInstance(getConf());
	    job1.setJobName(PairsPMI.class.getSimpleName()+" Stage-1");
	    job1.setJarByClass(PairsPMI.class);

	    job1.setNumReduceTasks(1);

	    FileInputFormat.setInputPaths(job1, new Path(args.input));
	    FileOutputFormat.setOutputPath(job1, new Path(countsFile));

	    //Fix this later
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    //if (args.textOutput) {
	      job1.setOutputFormatClass(TextOutputFormat.class);
	    //} else {
	    //  job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	    //}

	    job1.setMapperClass(MyMapperCount.class);
	    //if(args.cmb) job1.setCombinerClass(MyReducerCount.class);
	    job1.setCombinerClass(MyReducerCount.class);
	    job1.setReducerClass(MyReducerCount.class);
	    //job.setPartitionerClass(MyPartitioner.class);

	    //job configurations on cluster
	    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
	    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
	    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
	    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
	    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

	    // Delete the output directory if it exists already.
	    Path countsDir = new Path(countsFile);
	    FileSystem.get(getConf()).delete(countsDir, true);

	    long startTime = System.currentTimeMillis();
	    job1.waitForCompletion(true);
	    long totalLines = job1.getCounters().findCounter(LineCount.CNT).getValue();
	    System.out.println("Stage 1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	    //Start stage 2
	    //long totalLines = job1.getCounters().findCounter("NUMLINES","NUMLINES").getValue();
	    LOG.info("Tool name: " + PairsPMI.class.getSimpleName()+" Stage-2");
	    LOG.info(" - input path: " + args.input);
	    LOG.info(" - output path: " + args.output);
	    LOG.info(" - num reducers: " + args.numReducers);
	    LOG.info(" - text output: " + args.textOutput);
	    LOG.info(" - threshold: " + args.threshold);
	    LOG.info(" - combiner off: " + args.cmboff);

	    Job job2 = Job.getInstance(getConf());
	    job2.setJobName(PairsPMI.class.getSimpleName()+" Stage-2");
	    job2.setJarByClass(PairsPMI.class);

	    job2.setNumReduceTasks(args.numReducers);

	    FileInputFormat.setInputPaths(job2, new Path(args.input));
	    FileOutputFormat.setOutputPath(job2, new Path(args.output));

	    //Fix this later
	    job2.setMapOutputKeyClass(PairOfStrings.class);
	    //job2.setMapOutputValueClass(PairOfFloatInt.class);
	    job2.setMapOutputValueClass(IntWritable.class);
	    job2.setOutputKeyClass(PairOfStrings.class);
	    job2.setOutputValueClass(PairOfFloatInt.class);
	    //if (args.textOutput) {
	      job2.setOutputFormatClass(TextOutputFormat.class);
	    //} else {
	    //  job2.setOutputFormatClass(SequenceFileOutputFormat.class);
	    //}

	    job2.setMapperClass(MyMapperPMI.class);
	    if(!args.cmboff) job2.setCombinerClass(MyCombinerPMI.class);
	    job2.setReducerClass(MyReducerPMI.class);
	    job2.setPartitionerClass(MyPartitioner.class);

	    //job configurations on cluster
	    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
	    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
	    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
	    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
	    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

	    job2.getConfiguration().setLong("DOCLINES",totalLines);
	    job2.getConfiguration().setInt("threshhold",args.threshold);

	    // Delete the output directory if it exists already.
	    Path outputDir = new Path(args.output);
	    FileSystem.get(getConf()).delete(outputDir, true);

	    startTime = System.currentTimeMillis();
	    job2.waitForCompletion(true);
	    System.out.println("Stage 2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

	    return 0;
  	}

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  	public static void main(String[] args) throws Exception {
    	ToolRunner.run(new PairsPMI(), args);
  	}
}
