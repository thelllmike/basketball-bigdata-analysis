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

public class MostScoringQuarter {

    public static class QuarterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text teamQuarter = new Text();
        private IntWritable score = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 8) {
                String team = fields[8]; // PLAYER1_TEAM_ABBREVIATION
                String period = fields[5]; // PERIOD
                String scoreText = fields[23]; // SCORE
                
                if (!scoreText.isEmpty()) {
                    try {
                        int points = Integer.parseInt(scoreText.replaceAll("[^0-9]", ""));
                        teamQuarter.set(team + "_Q" + period);
                        score.set(points);
                        context.write(teamQuarter, score);
                    } catch (NumberFormatException e) {
                        // Ignore invalid scores
                    }
                }
            }
        }
    }

    public static class QuarterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        Job job = Job.getInstance(conf, "Most Scoring Quarter");
        job.setJarByClass(MostScoringQuarter.class);
        job.setMapperClass(QuarterMapper.class);
        job.setReducerClass(QuarterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
