import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


public class CasualtiesBreakdown extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(CasualtiesBreakdown.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CasualtiesBreakdown(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "CasualtiesBreakdown");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(CasualtiesBreakdownMapper.class);
        job.setReducerClass(CasualtiesBreakdownReducer.class);
        job.setMapOutputKeyClass(CompositeGroupKey.class);
        job.setMapOutputValueClass(CasualtiesCount.class);
        job.setOutputKeyClass(CompositeGroupKey.class);
        job.setOutputValueClass(CasualtiesCount.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class CasualtiesBreakdownMapper extends Mapper<LongWritable, Text, CompositeGroupKey, CasualtiesCount> {
        private Text street = new Text();
        private Text year = new Text();
        private Text zip = new Text();
        private Integer injured_pedestrians, injured_cyclists, injured_motorists, killed_pedestrians, killed_cyclists, killed_motorists;


        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            try {
                if (offset.get() == 0)
                    return;
                else {
                    String line = lineText.toString();
                    int i = 0;
                    for (String word : line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
                    {
                        if (i == 0) {year.set(word.substring(word.lastIndexOf('/') + 1, word.lastIndexOf('/') + 5)); if(Integer.parseInt(year.toString()) <= 2012) continue;}
                        if (i == 2) {if(word.isEmpty()) continue; else zip.set(word);}
                        if (i == 7) {if(word.isEmpty()) continue; else street.set(word);}
                        if (i == 11) {injured_pedestrians = (Integer.parseInt(word));}
                        if (i == 12) {killed_pedestrians = (Integer.parseInt(word));}
                        if (i == 13) {injured_cyclists = (Integer.parseInt(word));}
                        if (i == 14) {killed_cyclists = (Integer.parseInt(word));}
                        if (i == 15) {injured_motorists = (Integer.parseInt(word));}
                        if (i == 16) {killed_motorists = (Integer.parseInt(word));}
                        i++;
                    }
                    CompositeGroupKey street_zip = new CompositeGroupKey();
                    street_zip.street = street.toString();
                    street_zip.zip = zip.toString();
                    CasualtiesCount casualties = new CasualtiesCount(injured_pedestrians, injured_cyclists, injured_motorists, killed_pedestrians, killed_cyclists, killed_motorists);
                    context.write(street_zip, casualties);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static class CasualtiesBreakdownReducer extends Reducer<CompositeGroupKey, CasualtiesCount, Text, Text> {

        public void reduce(CompositeGroupKey key, Iterable<CasualtiesCount> values,
                           Context context) throws IOException, InterruptedException {
            CasualtiesCount casualtiesCount = new CasualtiesCount(0,0,0,0,0,0);
            for (CasualtiesCount val : values) {
                casualtiesCount.addCasualtiesCount(val);
            }
            context.write(new Text(key.toString()), new Text(casualtiesCount.toString()));
        }
    }
}

