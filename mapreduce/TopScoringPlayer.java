import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopScoringPlayer {

    public static class PlayerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text playerName = new Text();
        private IntWritable score = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 7) {
                String player = fields[7]; // PLAYER1_NAME
                String scoreText = fields[23]; // SCORE
                
                if (!scoreText.isEmpty()) {
                    try {
                        int points = Integer.parseInt(scoreText.replaceAll("[^0-9]", ""));
                        playerName.set(player);
                        score.set(points);
                        context.write(playerName, score);
                    } catch (NumberFormatException e) {
                        // Ignore invalid scores
                    }
                }
            }
        }
    }

    public static class PlayerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalPoints = 0;
            for (IntWritable val : values) {
                totalPoints += val.get();
            }
            context.write(key, new IntWritable(totalPoints));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Scoring Player");
        job.setJarByClass(TopScoringPlayer.class);
        job.setMapperClass(PlayerMapper.class);
        job.setReducerClass(PlayerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
