/**
 * Assignment 3: Cloud Computing for Data Analysis.
 *
 * @author Rekhansh Panchal (rekhanshpanchal@gmail.com)
 * Implementation of Page Rank Algorithm.
 * University of North Carolina, at Charlotte.
 */

package org.myorg;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PageRank extends Configured implements Tool {

    //Strings.
    private static final Logger LOG = Logger.getLogger(PageRank.class);
    private static final String HASH = "###";
    private static final String SEPARATOR = "&&&";
    private static final double d = 0.85d;
    private static final String identifier = "!@!";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PageRank(), args);
        System.exit(res);
    }

    /**
     * @param args is command line arguments for input files to be processed.
     * @return 0 on completion of job, to exit.
     */
    public int run(String[] args) throws Exception {

        //File System Setup.
        FileSystem fileSystem = FileSystem.get(getConf());
        final String intermediatePath = "intermediate";
        final String iteration = "Iteration";

        /**
         * Job for counting number of files.
         */
        LOG.info("Counting number of links....");
        Job jobFileCounter = Job.getInstance(getConf(), "File Count");
        jobFileCounter.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(jobFileCounter, args[0]);
        FileOutputFormat.setOutputPath(jobFileCounter, new Path(intermediatePath));
        jobFileCounter.setMapperClass(MapCounter.class);
        jobFileCounter.setReducerClass(ReduceCounter.class);
        jobFileCounter.setOutputKeyClass(Text.class);
        jobFileCounter.setOutputValueClass(IntWritable.class);
        jobFileCounter.waitForCompletion(true);
        fileSystem.delete(new Path(intermediatePath), true);


        //Get file count through counters.
        long fileCount = jobFileCounter.getCounters().findCounter("pageRank", "fileCount").getValue();
        LOG.info("Links counted. Weblinks directly mentioned are: " + fileCount);

        /**
         * Job for initializing matrix and values to 1/N
         * Fetch titles and outlinks.
         */
        LOG.info("Starting matrix initialization...");
        Job jobInitializeMatrix = Job.getInstance(getConf(), "Initialize Matrix");
        jobInitializeMatrix.getConfiguration().setLong("N", fileCount);
        jobInitializeMatrix.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(jobInitializeMatrix, args[0]);
        FileOutputFormat.setOutputPath(jobInitializeMatrix, new Path(intermediatePath + iteration + "0"));
        jobInitializeMatrix.setMapperClass(MapByN.class);
        jobInitializeMatrix.setReducerClass(ReduceByN.class);
        jobInitializeMatrix.setOutputKeyClass(Text.class);
        jobInitializeMatrix.setOutputValueClass(Text.class);
        jobInitializeMatrix.waitForCompletion(true);
        LOG.info("Matrix initialization complete. Proceeding to start iterating...");

        /**
         * Diasy chained jobs for iterations over matrix untill convergence.
         * Assumption: Matrix values converge by end of n iterations.
         */
        int n = 10;
        for (int i = 0; i < n; i++) {

            LOG.info("STARTING : Iteration number: " + (i + 1));
            Job jobPageRank = Job.getInstance(getConf(), "PageRank");
            jobPageRank.setJarByClass(this.getClass());
            FileInputFormat.addInputPaths(jobPageRank, intermediatePath + iteration + (i));
            FileOutputFormat.setOutputPath(jobPageRank, new Path(intermediatePath + iteration + (i + 1)));
            jobPageRank.setMapperClass(MapPageRank.class);
            jobPageRank.setReducerClass(ReducePageRank.class);
            jobPageRank.setOutputKeyClass(Text.class);
            jobPageRank.setOutputValueClass(Text.class);
            jobPageRank.waitForCompletion(true);
            fileSystem.delete(new Path(intermediatePath + iteration + (i)), true);
            LOG.info("ENDING : Iteration number: " + (i + 1));
        }

        LOG.info("Page Rank calculations complete. Proceeding to start sorting...");

        /**
         * Job for sorting the output. Has single reducer.
         */
        Job jobSort = Job.getInstance(getConf(), "Sort Page Ranks");
        jobSort.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(jobSort, intermediatePath + iteration + n);
        FileOutputFormat.setOutputPath(jobSort, new Path(args[1]));
        jobSort.setMapperClass(MapSort.class);
        jobSort.setReducerClass(ReduceSort.class);

        //Setting single reducer task to get global sorting.
        jobSort.setNumReduceTasks(1);

        jobSort.setOutputKeyClass(DoubleWritable.class);
        jobSort.setOutputValueClass(Text.class);
        jobSort.waitForCompletion(true);
        fileSystem.delete(new Path(intermediatePath + iteration + n), true);
        LOG.info("Page Ranks are sorted now. Please get the output file.");

        //Return 1, when all jobs are completed.
        return 1;
    }

    /**
     * Map class to get title counts.
     */
    public static class MapCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
        //Pattern to detect title and eliminate non-title tags.
        private static final Pattern TITLE_BOUNDARY = Pattern.compile("<title>(.+?)</title>");

        /**
         * Map function finds the titles occurring with patterns and writes it to context.
         *
         * @param lineText is the actual line read from input file.
         */
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            Matcher matcher = TITLE_BOUNDARY.matcher(line);

            // Find all matches and send the group count.
            context.write(new Text("File Count"), new IntWritable(matcher.groupCount()));
        }
    }

    /**
     * Reducer class to sum total file counts.
     */
    public static class ReduceCounter extends Reducer<Text, IntWritable, Text, IntWritable> {

        int sum = 0;

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {

            //Addition of all counts.
            for (IntWritable count : counts)
                this.sum += count.get();
        }

        /**
         * Function to send filecount at end of task.
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter("pageRank", "fileCount").increment(sum);
        }

    }

    /**
     * Mapper class to initialize pageRank matrix.
     */
    public static class MapByN extends Mapper<LongWritable, Text, Text, Text> {

        //Patterns to detect tags and eliminate rest.
        private static final Pattern TITLE_BOUNDARY = Pattern.compile("<title>(.+?)</title>");
        private static final Pattern TEXT_BOUNDARY = Pattern.compile("<text(.*?)>(.*?)</text>");
        private static final Pattern OUTLINK_BOUNDARY = Pattern.compile("\\[\\[.*?]\\]");
        private double initialRank;

        //Fetch the value of N from configuration.
        public void setup(Context context) throws IOException, InterruptedException {

            long n = context.getConfiguration().getLong("N", (long) 1);
            initialRank = 1.0 / n;
        }

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

            Matcher matcherTitle, matcherURL, matcherText;
            String line = lineText.toString();
            Text title = new Text("null");
            matcherTitle = TITLE_BOUNDARY.matcher(line);
            matcherText = TEXT_BOUNDARY.matcher(line);

            if (line != null && !line.isEmpty()) {

                //Find the title.
                if (matcherTitle.find())
                    title = new Text(matcherTitle.group(1).trim());

                BigDecimal bigDecimal = new BigDecimal(initialRank);

                StringBuilder initialGraph = new StringBuilder(HASH + bigDecimal.toPlainString());

                /** Find the text and set matcher for URLs.
                 *  Find the URLs, if there is a match and store it in a list.
                 */
                if (matcherText.find()) {

                    matcherURL = OUTLINK_BOUNDARY.matcher(matcherText.group());

                    while (matcherURL != null && matcherURL.find()) {

                        String url = matcherURL.group().replace("[[", "").replace("]]", "");
                        if (!url.isEmpty())
                            initialGraph.append(SEPARATOR + url.trim());
                    }
                }
                context.write(title, new Text(initialGraph.toString()));
            }
        }
    }

    /**
     * Reducer class to initialize pageRank matrix.
     */
    public static class ReduceByN extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text word, Text counts, Context context) throws IOException, InterruptedException {

            //Sending link and initial value.
            context.write(word, counts);
        }
    }

    /**
     * Mapper class to calculate new PageRank.
     */
    public static class MapPageRank extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            Text title, rank;

            //Get actual string by replacig tab by empty string.
            String line = lineText.toString();
            line = line.replace("\t", "");

            //Seperate title, rank and outlinks (if any).
            String[] nameValues = (line).split(HASH);
            title = new Text(nameValues[0]);

            //Seperate currentPageRank from outlinks.
            String[] valueLinks = nameValues[1].split(SEPARATOR);

            //Get the current page rank.
            String currentPageRankString = valueLinks[0];
            double currentPageRank = Double.parseDouble(currentPageRankString);

            /**
             * Pass to reducer phase depending on outgoing links.
             *
             */
            switch (valueLinks.length) {

                //The page is independent of others and has no outgoing links.
                case 1:
                    rank = new Text(HASH + currentPageRankString);
                    //context.write(title, rank);
                    context.write(title, new Text(identifier));
                    break;

                /**
                 * The link has outlinks and will share it's pageRank.
                 */
                default:

                    //PageRank shared by each outlink.
                    double pageRankToAdd = (currentPageRank / (double) (valueLinks.length - 1));
                    BigDecimal bigDecimal = new BigDecimal(pageRankToAdd);

                    //Add pages rank to all outlinks. 0th location is the rank of current page being checked.
                    for (int i = 1; i < valueLinks.length; i++)
                        context.write(new Text(valueLinks[i].trim()), new Text(HASH + bigDecimal.toPlainString()));

                    //Send original response with the weblink to maintain the track of outlink data.
                    context.write(title, new Text(HASH + nameValues[1].trim()));

                    //Identifier to check if it is an direct link in title or not.
                    context.write(title, new Text(identifier));
                    break;
            }
        }
    }

    /**
     * Reducer class for pageRank matrix.
     */
    public static class ReducePageRank extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double rank = 0;
            boolean isValid = false;
            String outLinks = "";

            for (Text value : values) {

                String input = value.toString();
                input = input.trim();

                if (input.equals(identifier)) {
                    isValid = true;
                    continue;
                }


                String[] inputArray = input.split(HASH);
                String[] inputLinks = inputArray[1].split(SEPARATOR, 2);
                try {
                    //Do nothing, if the outLinks exists. (Means original link is being passed, on which no calculation
                    //should be performed.
                    outLinks = SEPARATOR + inputLinks[1];

                } catch (Exception e) {

                    //Means it is just the added rank from some outlink.
                    rank += Double.parseDouble(inputLinks[0]);
                }

            }

            //Write the rank, only if the link is valid in titles.
            if (isValid) {
                rank = (1.0 - d) + d * rank;

                BigDecimal bigDecimal = new BigDecimal(rank);

                context.write(word, new Text(HASH + bigDecimal.toPlainString() + outLinks.trim()));
            }
        }
    }

    /**
     * Mapper class for sorting.
     * This sorting is based on inbuilt feature of MapReduce to provide keys in sorted fashion to the reducer.
     */
    public static class MapSort extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

            String line = lineText.toString();
            String[] titleRankOutlinks = line.split(HASH);
            String title = titleRankOutlinks[0];
            double rank = 0;

            try {
                String[] rankOutlinks = titleRankOutlinks[1].split(SEPARATOR, 2);
                rank = Double.parseDouble(rankOutlinks[0]);
            } catch (Exception e) {
                rank = Double.parseDouble(titleRankOutlinks[0]);
            }

            context.write(new DoubleWritable(rank * (-1.0)), new Text(title));
        }
    }

    /**
     * Reducer class for sorting ranks.
     */
    public static class ReduceSort extends Reducer<DoubleWritable, Text, Text, Text> {

        public void reduce(DoubleWritable count, Iterable<Text> words, Context context) throws IOException, InterruptedException {

            for (Text word : words)
                context.write(word, new Text(String.valueOf(count.get() * (-1.0))));
        }
    }

}
