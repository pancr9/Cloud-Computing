package org.myorg;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;

/**
 * @author Rekhansh Panchal (rpanchal@uncc.edu)
 * <p>
 * Search.java accepts user Query as input and outputs list of documents
 * with scores that best match the search hits.
 */

public class Search extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Search.class);
    private static final String hash = "#####";
    private static final String tab = "\t";

    /**
     * @param args : Command line arguments for TFIDF input file, Search output file, and userQuery.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }


    /**
     * @return 0 on completion of job, to exit.
     * @throws Exception Logic to remove input and output file path from command line arguments and send search keys.
     */
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Search");
        String[] searchKeys = new String[args.length - 2];

        if (args != null)
            for (int i = 2; i < args.length; i++)
                searchKeys[i - 2] = args[i];

        job.getConfiguration().setStrings("searchKeys", searchKeys);
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MapSearch.class);
        job.setReducerClass(ReduceSearch.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    /**
     * Includes methods for setup and mapping.
     */
    public static class MapSearch extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        HashSet<String> searchKeySet = new HashSet<>();

        /**
         * @param context gives the search keys to find the documents.
         * @throws IOException
         * @throws InterruptedException Setup function helps to create a HashSet of search keys.
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            String[] searchKeys = context.getConfiguration().getStrings("searchKeys");

            for (String key : searchKeys)
                searchKeySet.add(key.toLowerCase());
        }


        /**
         * Function maps value of each line with TFIDF score if it contains search key.
         *
         * @param lineText provides the line from text file which needs to be processed by splitting.
         */
        @Override
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

            String words[] = lineText.toString().split(hash);

            if (searchKeySet.contains(words[0])) {
                String list[] = words[1].split(tab);
                context.write(new Text(list[0]), new DoubleWritable(Double.valueOf(list[1])));
            }
        }
    }


    public static class ReduceSearch extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        /**
         * ReduceSearch helps to combine all mapped <key,value> pairs with same keys.
         *
         * @param filename is the filename in which word is occured.
         * @param counts   are the values of TDIDF scores for a each file.
         */
        @Override
        public void reduce(Text filename, Iterable<DoubleWritable> counts, Context context) throws IOException, InterruptedException {

            double sum = 0;

            //Calculating total TFIDF score.
            for (DoubleWritable doubleWritable : counts)
                sum = sum + doubleWritable.get();

            context.write(filename, new DoubleWritable(sum));
        }
    }
}
