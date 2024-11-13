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

public class MapReduceMatrixMultiplication {

    public static class MatrixMultiplyMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            int i = Integer.parseInt(parts[0]);
            int j = Integer.parseInt(parts[1]);
            int valueOfElement = Integer.parseInt(parts[2]);

            // Emit key-value pairs for matrix A and matrix B
            if (key.toString().endsWith("A")) {
                // For matrix A, emit (i, k) => (A, value)
                context.write(new Text(i + "," + j), new Text("A:" + parts[0] + "," + parts[1]));
            } else if (key.toString().endsWith("B")) {
                // For matrix B, emit (i, j) => (B, value)
                context.write(new Text(i + "," + j), new Text("B:" + parts[0] + "," + parts[1]));
            }
        }
    }

    public static class MatrixMultiplyReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int result = 0;
            String[] indices = key.toString().split(",");
            int i = Integer.parseInt(indices[0]);
            int j = Integer.parseInt(indices[1]);

            for (Text value : values) {
                String val = value.toString();
                if (val.startsWith("A:")) {
                    String[] valueParts = val.split(",");
                    result += Integer.parseInt(valueParts[2]);
                }
            }
            context.write(new Text(i + "," + j), new IntWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(MapReduceMatrixMultiplication.class);

        job.setMapperClass(MatrixMultiplyMapper.class);
        job.setReducerClass(MatrixMultiplyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
