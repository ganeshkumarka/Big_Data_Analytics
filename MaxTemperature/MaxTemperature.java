import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxTemperature {

    // Mapper class
    public static class MaxTempMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text year = new Text();
        private IntWritable temperature = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Example input: "2024-01-01, 22"
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length == 2) {
                try {
                    // Extract year and temperature
                    String[] dateParts = parts[0].split("-");
                    String yearStr = dateParts[0].trim();
                    int temp = Integer.parseInt(parts[1].trim());

                    // Set the year and temperature
                    year.set(yearStr);
                    temperature.set(temp);

                    // Emit the year as the key and temperature as the value
                    context.write(year, temperature);
                } catch (NumberFormatException e) {
                    // Skip invalid records
                }
            }
        }
    }

    // Reducer class
    public static class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable maxTemperature = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxTemp = Integer.MIN_VALUE;

            // Find the maximum temperature for each year
            for (IntWritable val : values) {
                maxTemp = Math.max(maxTemp, val.get());
            }

            // Set the maximum temperature and write the result
            maxTemperature.set(maxTemp);
            context.write(key, maxTemperature);
        }
    }

    // Main driver method
    public static void main(String[] args) throws Exception {
        // Configuration and job setup
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Temperature");
        job.setJarByClass(MaxTemperature.class);
        
        // Set the Mapper and Reducer classes
        job.setMapperClass(MaxTempMapper.class);
        job.setReducerClass(MaxTempReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0])); // input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
