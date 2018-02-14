package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
import org.myorg.TermFrequency;

import java.io.IOException;
import java.util.HashMap;

public class TFIDF extends Configured implements Tool {

    /**
     * @author Rekhansh Panchal (rpanchal@uncc.edu)
     * ITCS 6190 : Clouding Computing Assignment 2
     * Calculates TFIDF values using chaining of TermFrequency.java
     */


    private static final Logger LOG = Logger.getLogger(TFIDF.class);

    //Making variables static final to access them faster.
    private static final String hash = "#####";
    private static final String tab = "\t";
    private static final String equals = "=";


    public static void main(String[] args) throws Exception {

        int resTF = ToolRunner.run(new TermFrequency(), args);
        int resTFIDF = ToolRunner.run(new TFIDF(), args);

        //Exit when TFIDF is computed.
        System.exit(resTFIDF);
    }

    /**
     * Includes logic to fetch number of files for which TFIDF is being calculated.
     * Function to set job properties.
     */

    public int run(String[] args) throws Exception {

        // Count input files.
        int count = 0;
        FileSystem fs = FileSystem.get(getConf());
        boolean recursive = false;
        RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(args[0]), recursive);

        while (ri.hasNext()) {
            count++;
            ri.next();
        }

        Configuration conf = new Configuration();
        conf.set("fileCount", String.valueOf(count));
        Job job = Job.getInstance(conf, " TFIDF ");
        job.setJarByClass(this.getClass());

        //Input path is set as args[1] since it is output of TermFrequency.
        FileInputFormat.addInputPaths(job, args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(MapIDF.class);
        job.setReducerClass(ReduceIDF.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class MapIDF extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * MapIDF helps to provide key value pairs in the format of <"word","filename=count"> by spliting input.
         *
         * @param lineText
         */
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

            //Splitting input from TF.
            String line = lineText.toString();
            String[] parts = line.split(hash);
            String[] value = parts[1].split(tab);

            //Write map function to provide value "filename=count"
            context.write(new Text(parts[0]), new Text(value[0] + equals + value[1]));
        }

    }

    public static class ReduceIDF extends Reducer<Text, Text, Text, DoubleWritable> {

        /**
         * @param word   gets the key from map
         * @param counts helps to find IDF value and TFIDF.
         */
        public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            //Get file count.
            double fileCount = Double.valueOf(conf.get("fileCount"));

            //Creating a HashMap to store key value pairs.
            HashMap<String, Double> hashMap = new HashMap<>();

            int occurredIn = 0;
            for (Text count : counts) {
                String value = count.toString();
                hashMap.put(value.split(equals)[0], Double.valueOf(value.split(equals)[1]));
                occurredIn++;
            }

            // Calculating the IDF value
            double IDFValue = Math.log10(1 + (fileCount / occurredIn));
            double TFIDFValue = 0;

            for (String key : hashMap.keySet()) {
                //calculating TFIDF score
                TFIDFValue = IDFValue * hashMap.get(key);
                context.write(new Text(word + hash + key), new DoubleWritable(TFIDFValue));
            }
        }
    }
}
