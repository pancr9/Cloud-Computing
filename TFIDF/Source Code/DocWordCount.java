package org.myorg;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * @author Rekhansh Panchal (rpanchal@uncc.edu)
 * Calculates word count in each file.
 * File to obtain the wordcount for each file and provide output in different format.
 */
public class DocWordCount extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(DocWordCount.class);
    private static final String hash = "#####";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DocWordCount(), args);
        System.exit(res);
    }

    /**
     * @param args is command line arguments for input files to be processed.
     * @return 0 on completion of job, to exit.
     */
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Doc Word Count");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Map class to get words existing and individual count.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        //Pattern to detect and eliminate non-words.
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
        private static final Pattern WORD_CHECK = Pattern.compile("[a-z]+");
        String fileName = new String();


        /**
         * Setup function to setup filename to be used during map.
         *
         * @param context helps to get the filename which is being processed with help of FileSplit.
         */
        protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException {
            fileName = ((FileSplit) context.getInputSplit()).getPath().toString();

            fileName = fileName.substring(fileName.lastIndexOf('/') + 1).trim();
        }


        /**
         * Map function finds the words occuring with patterns and writes it to context.
         *
         * @param lineText is the actual line read from input file.
         */
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            Text currentWord = new Text();

            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty())
                    continue;

                //Formatting as per requirement. "word#####fileName" for key.
                currentWord = new Text(word.trim().toLowerCase() + hash + fileName);

                //Match pattern to map words only. word is skipped if is not a "Word"
                if (WORD_CHECK.matcher(word.trim().toLowerCase()).matches())
                    context.write(currentWord, one);
            }
        }
    }


    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param word   gives the word which is being considered for counting.
         * @param counts provides the number of the times word occured.
         */
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            //Addition of all counts.
            for (IntWritable count : counts) {
                sum = sum + count.get();
            }

            //Sending word and sum.
            context.write(word, new IntWritable(sum));
        }
    }
}
