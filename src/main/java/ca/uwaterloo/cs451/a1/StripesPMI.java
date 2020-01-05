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
import tl.lin.data.map.MapKI;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;

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


public class StripesPMI extends Configured implements Tool{
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
			// for(String word: Tokenizer.tokenize(value.toString())){
			// 	if(word_set.add(word)){
			// 		WORD.set(word);
			// 		context.write(WORD,ONE);
			// 	}
			// }

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

	private static final class MyMapperStripes extends Mapper<LongWritable, Text, Text, HMapStIW>{
		private static final Text WORD = new Text();
		private static final HMapStIW MAP = new HMapStIW();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
			//Map<String, HMapStFW> stripes =  new HashMap<>();
			Set<String> word_set = new HashSet<String>();
			List<String>word_list1 = Tokenizer.tokenize(value.toString());
			int loop_count = Math.min(word_list1.size(), 40);
			for(int i=0; i<loop_count;i++){
				String word = word_list1.get(i);
				if(word_set.add(word))
					continue;
			}
			//get unique word list
			List<String>word_list2 = new ArrayList<String>(word_set);
			for(int i=0; i<word_list2.size();i++){
				MAP.clear();
				for(int j=0;j<word_list2.size();j++){
					if(i==j)
						continue;
					MAP.increment(word_list2.get(j));
				}
				WORD.set(word_list2.get(i));
				context.write(WORD, MAP);
			}
		}
	}

	//Stage 2: Combiner
	private static final class MyCombinerStripes extends Reducer<Text, HMapStIW, Text, HMapStIW>{
		@Override
		public void reduce(Text key, Iterable<HMapStIW> values, Context context)
		throws IOException, InterruptedException{
			HMapStIW map = new HMapStIW();
			Iterator<HMapStIW> itr = values.iterator();
			while(itr.hasNext()){
				map.plus(itr.next());
			}
			context.write(key, map);
		}
	}

	//Stage 2: Reducer
	private static final class MyReducerStripes extends Reducer<Text, HMapStIW, Text, HashMapWritable<Text,PairOfFloatInt> >{
		private static Map<String, Integer>wordCount = new HashMap<String, Integer>();
		//private static PairOfFloatInt PMICNT = new PairOfFloatInt();
		//private static Text WORD = new Text();
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
		public void reduce(Text key, Iterable<HMapStIW> values, Context context) 
		throws IOException, InterruptedException{
			Text WORD = new Text();
			PairOfFloatInt PMICNT = new PairOfFloatInt();
			HashMapWritable<Text,PairOfFloatInt> MAP = new HashMapWritable<Text,PairOfFloatInt>();
			HMapStIW map = new HMapStIW();
			int num_entries = 0;
			//HMapStIW mapout = new HMapStIW();
			Iterator<HMapStIW> itr = values.iterator();
			while(itr.hasNext()){
				map.plus(itr.next());
			}
			for(MapKI.Entry<String> entry : map.entrySet()){
				if(entry.getValue()>=threshhold){
					num_entries++;
					float pair_prob = entry.getValue()/totalLines;
					float key_prob = wordCount.get(key.toString())/totalLines;
					float cooccur_prob = wordCount.get(entry.getKey())/totalLines;
					float pmi = (float)Math.log10(pair_prob/(key_prob*cooccur_prob));
					//WORD.set(entry.getKey());
					//PMICNT.set(pmi,entry.getValue());
					//if(key.toString().equals("aaron")){
					//	System.out.println("**************************WORD="+WORD.toString()+"*******************");
					//	System.out.println("*********************************************************************");
					//}
					MAP.put(new Text(entry.getKey()), new PairOfFloatInt(pmi,entry.getValue()));//WORD,PMICNT);
					//mapout.put(entry.getKey(),entry.getValue());
				}
			}
			if(num_entries>0) context.write(key,MAP);
			//context.write(key,mapout);
		}
	}
	/**
   * Creates an instance of this tool.
   */
	private StripesPMI(){}

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

	    LOG.info("Tool name: " + StripesPMI.class.getSimpleName()+" Stage-1");
	    LOG.info(" - input path: " + args.input);
	    LOG.info(" - output path: " + countsFile);
	    LOG.info(" - num reducers: " + 1);
	    LOG.info(" - text output: " + args.textOutput);
	    LOG.info(" - threshold: " + args.threshold);

	    Job job1 = Job.getInstance(getConf());
	    job1.setJobName(StripesPMI.class.getSimpleName()+" Stage-1");
	    job1.setJarByClass(StripesPMI.class);

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
	    LOG.info("Tool name: " + StripesPMI.class.getSimpleName()+" Stage-2");
	    LOG.info(" - input path: " + args.input);
	    LOG.info(" - output path: " + args.output);
	    LOG.info(" - num reducers: " + args.numReducers);
	    LOG.info(" - text output: " + args.textOutput);
	    LOG.info(" - threshold: " + args.threshold);
	    LOG.info(" - combiner off: " + args.cmboff);

	    Job job2 = Job.getInstance(getConf());
	    job2.setJobName(StripesPMI.class.getSimpleName()+" Stage-2");
	    job2.setJarByClass(StripesPMI.class);

	    job2.setNumReduceTasks(args.numReducers);

	    FileInputFormat.setInputPaths(job2, new Path(args.input));
	    FileOutputFormat.setOutputPath(job2, new Path(args.output));

	    //Fix this later
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(HMapStIW.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(HashMapWritable.class);
	    //job2.setOutputValueClass(HMapStIW.class);
	    //if (args.textOutput) {
	      job2.setOutputFormatClass(TextOutputFormat.class);
	    //} else {
	    //  job2.setOutputFormatClass(SequenceFileOutputFormat.class);
	    //}

	    job2.setMapperClass(MyMapperStripes.class);
	    if(!args.cmboff) job2.setCombinerClass(MyCombinerStripes.class);
	    job2.setReducerClass(MyReducerStripes.class);
	    //job.setPartitionerClass(MyPartitioner.class);

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
    	ToolRunner.run(new StripesPMI(), args);
  	}


}
