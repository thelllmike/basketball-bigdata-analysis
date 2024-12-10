import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopScoringPlayer {

    public static class PlayerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text playerName = new Text();
        private IntWritable score = new IntWritable();
        private boolean isHeader = true; // To skip the header line

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return; // Skip header line
            }

            String line = value.toString();
            String[] fields = line.split("\t");

            // Debugging: Print the line being processed
            System.out.println("Processing line: " + line);

            if (fields.length >= 4) {
                try {
                    String homeDescription = fields[3].trim(); // HOMEDESCRIPTION
                    String name = fields[7].trim();            // PLAYER1_NAME

                    int points = 0;
                    if (homeDescription.contains("(2 PTS)")) {
                        points = 2;
                    } else if (homeDescription.contains("(3 PTS)")) {
                        points = 3;
                    }

                    if (!name.isEmpty() && points > 0) {
                        playerName.set(name);
                        score.set(points);

                        // Debugging: Print the key-value being emitted
                        System.out.println("Emitting: " + name + " -> " + points);

                        context.write(playerName, score);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing line: " + line);
                }
            } else {
                System.err.println("Malformed line: " + line);
            }
        }
    }

    public static class PlayerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalScore = 0;

            for (IntWritable val : values) {
                totalScore += val.get();
            }

            context.write(key, new IntWritable(totalScore));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(TopScoringPlayer.class);
        job.setJobName("Top Scoring Player");

        job.setMapperClass(PlayerMapper.class);
        job.setReducerClass(PlayerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
