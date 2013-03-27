/*
 * Copyright (c) 2013, Aleksandr Panzin (jalexoid)
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

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

		FileOutputFormat.setOutputPath(primary, new Path(output + "/tmp.bin"));

		JobConf secondary = new JobConf(CorrelationRunner.class);

		secondary.setOutputKeyClass(Text.class);
		secondary.setOutputValueClass(Text.class);

		secondary.setMapOutputValueClass(SimpleArray.class);
		secondary.setMapOutputKeyClass(Text.class);

		secondary.setReducerClass(TemporalKeyDeduper.class);

		secondary.setInputFormat(SequenceFileInputFormat.class);
		secondary.setOutputFormat(TextOutputFormat.class);

		FileInputFormat
				.setInputPaths(secondary, new Path(output + "/tmp.bin/"));
		FileOutputFormat.setOutputPath(secondary, new Path(output + "/final"));

		RunningJob rj = JobClient.runJob(primary);
		rj = JobClient.runJob(secondary);

	}

}
