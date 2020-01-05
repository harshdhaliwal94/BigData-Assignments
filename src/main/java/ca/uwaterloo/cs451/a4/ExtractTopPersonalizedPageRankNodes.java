package ca.uwaterloo.cs451.a4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.lang.Object;
import java.util.ArrayList;
import java.util.List;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool{
    //static top level class variables
    public static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);
    public static final String INDEX = "source_index";
    public static final String SOURCE_NODE = "source_node";

    private static class MyMapperExtract extends Mapper<IntWritable, PageRankNode, FloatWritable, IntWritable>{
        //static class variables
        private static FloatWritable page_rank = new FloatWritable();
        private static int index = 0;
        private static int srcNode;
        @Override
        public void setup(Context context){
            index = context.getConfiguration().getInt(INDEX,0);
            srcNode = context.getConfiguration().getInt(SOURCE_NODE,1);
        }
        @Override
        public void map(IntWritable nid, PageRankNode node, Context context) throws IOException, InterruptedException{
            float mass = node.getPageRankByIndex(index);
            if(mass==Float.NEGATIVE_INFINITY)
                page_rank.set((float) 0);
            else
                page_rank.set((float) (1*StrictMath.exp(mass)));
            context.write(page_rank, nid);
        }
    }

    private static class MyReducerExtract extends 
        Reducer<FloatWritable, IntWritable, FloatWritable, IntWritable>{
        //private static FloatWritable page_rank = new FloatWritable();
        @Override
        public void reduce(FloatWritable pagerank, Iterable<IntWritable> nodes, Context context)
            throws IOException, InterruptedException{
            Iterator<IntWritable> nids = nodes.iterator();
            //page_rank.set((float)(1*pagerank.get()));
            while(nids.hasNext()){
                context.write(pagerank,nids.next());
            }
        }
    }

    private static class MyKeyComparator extends WritableComparator {
         private MyKeyComparator() {
             super(FloatWritable.class, true);
         }

         @SuppressWarnings("rawtypes")
         @Override
         public int compare(WritableComparable w1, WritableComparable w2) {
             FloatWritable key1 = (FloatWritable) w1;
             FloatWritable key2 = (FloatWritable) w2;
             return -1 * key1.compareTo(key2);
         }
     }

    public ExtractTopPersonalizedPageRankNodes(){}
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TOP = "top";
    private static final String SOURCES = "sources";

    //run the tool
    @SuppressWarnings({"static-access"})
    public int run(String[] args) throws Exception{
        //parse options
        Options options =  new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
            .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
            .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
            .withDescription("top pangerank nodes").create(TOP));
        options.addOption(OptionBuilder.withArgName("list").hasArg()
            .withDescription("source node list").create(SOURCES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
        cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
        System.err.println("Error parsing command line: " + exp.getMessage());
        return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)|| !cmdline.hasOption(SOURCES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int top = Integer.parseInt(cmdline.getOptionValue(TOP));
        String srcNodes = cmdline.getOptionValue(SOURCES);

        LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
        LOG.info(" - inputDir: " + inputPath);
        LOG.info(" - outputDir: " + outputPath);
        LOG.info(" - top: " + top);
        LOG.info(" - sourceNodes: " + srcNodes);
        Configuration conf = getConf();
        FileSystem.get(conf).delete(new Path(outputPath), true);

        String[] srcListStringArr = srcNodes.trim().replaceAll("\\s+","").split(",");
        List srcList = new ArrayList<Integer>();
        List massList = new ArrayList<Float>();
        for(int i=0;i<srcListStringArr.length;i++){
            srcList.add(Integer.parseInt(srcListStringArr[i]));
            sortPageRank(i,Integer.parseInt(srcListStringArr[i]), inputPath, outputPath, conf);
        }
        for(int i=0;i<srcListStringArr.length;i++){
            //print top k
            try {
                Path inFile = new Path(outputPath+"/source-"+srcListStringArr[i]+"/part-r-00000");
                SequenceFile.Reader reader = null;
                try {
                    FloatWritable pagerank_key = new FloatWritable();
                    IntWritable nodeid_value = new IntWritable();
                    reader = new SequenceFile.Reader(conf, Reader.file(inFile), Reader.bufferSize(4096));
                    int topx = 0;
                    System.out.println("");
                    System.out.println("Source: "+srcListStringArr[i]);
                    while(reader.next(pagerank_key, nodeid_value)) {
                        String result = String.format("%.5f %d", pagerank_key.get(), nodeid_value.get());
                        System.out.println(result);
                        topx++;
                        if(topx>=top)
                            break;
                    }
 
                } finally {
                    if(reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return 0;
    }

    private void sortPageRank(int index, int srcNode, String inputPath, 
            String baseOutputPath,Configuration conf) throws Exception{
        String outputPath = baseOutputPath + "/source-"+srcNode;
        conf.setInt(INDEX,index);
        conf.setInt(SOURCE_NODE, srcNode);
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

        Job job = Job.getInstance(conf);
        job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getSimpleName()+ "source node: " + srcNode);
        job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(1);
        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MyMapperExtract.class);
        job.setReducerClass(MyReducerExtract.class);
        job.setSortComparatorClass(MyKeyComparator.class);
        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(),args);
    }
}
