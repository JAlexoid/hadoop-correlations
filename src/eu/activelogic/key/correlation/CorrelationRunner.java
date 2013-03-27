package eu.activelogic.key.correlation;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CorrelationRunner {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		
        JobConf primary = new JobConf(CorrelationRunner.class);
        primary.setJobName("call_info");

        primary.setOutputKeyClass(Text.class);
        primary.setOutputValueClass(SimpleArray.class);
        
        primary.setMapOutputValueClass(SimpleArray.class);
        primary.setMapOutputKeyClass(Text.class);

        primary.setMapperClass(TemporalKeyMapper.class);
        primary.setReducerClass(TemporalKeyReducer.class);

        primary.setInputFormat(TextInputFormat.class);
        primary.setOutputFormat(SequenceFileOutputFormat.class);

        
        FileInputFormat.setInputPaths(primary, new Path(args[0]));
        
        String output = args[1];
        
        FileOutputFormat.setOutputPath(primary, new Path(output+"/tmp.bin"));
        
        JobConf secondary = new JobConf(CorrelationRunner.class);

        secondary.setOutputKeyClass(Text.class);
        secondary.setOutputValueClass(Text.class);
        
        secondary.setMapOutputValueClass(SimpleArray.class);
        secondary.setMapOutputKeyClass(Text.class);
        
        secondary.setReducerClass(TemporalKeyDeduper.class);
        
        secondary.setInputFormat(SequenceFileInputFormat.class);
        secondary.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(secondary, new Path(output+"/tmp.bin/"));
        FileOutputFormat.setOutputPath(secondary, new Path(output+"/final"));

        
        RunningJob rj = JobClient.runJob(primary);
        rj = JobClient.runJob(secondary);
        
	}

}
