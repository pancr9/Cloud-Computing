package org.myorg;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TermFrequency extends Configured implements Tool {

    /**
     * @author Rekhansh Panchal (rpanchal@uncc.edu)
     * ITCS 6190 : Clouding Computing Assignment 2
     * Calculates Term Frequency of words.
     */

    private static final Logger LOG = Logger.getLogger(TermFrequency.class);
    private static final String hash = "#####";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TermFrequency(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Term Frequency");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MapTF.class);
        job.setReducerClass(ReduceTF.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapTF extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
        private static final Pattern WORD_CHECK = Pattern.compile("[a-z]+");
        String fileName = new String();

        /**
         * Method setUp helps to get fileName, required for mapping.
         *
         * @param context
         */

        protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException {
            fileName = ((FileSplit) context.getInputSplit()).getPath().toString();

            //Splitting by last index of / to get file name.
            fileName = fileName.substring(fileName.lastIndexOf('/') + 1).trim();
        }

        /**
         * Pattern detection with REGEX helps to eliminate non words.
         */
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            Text currentWord = new Text();

            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty())
                    continue;

                word = word.trim();
                word = word.toLowerCase();

                currentWord = new Text(word + hash + fileName);

                //Match pattern to map words only. word is skipped if is not a "Word"
                Matcher matcher = WORD_CHECK.matcher(word);
                if (matcher.matches())
                    context.write(currentWord, one);
            }
        }
    }

    public static class ReduceTF extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        /**
         * @param word   input word
         * @param counts gives an iterable containing word counts.
         */
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts)
                sum = sum + count.get();

            //Calculating TF
            context.write(word, new DoubleWritable(Math.log10(sum) + 1));
        }
    }
}
