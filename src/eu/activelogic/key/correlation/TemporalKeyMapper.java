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
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TemporalKeyMapper implements
		Mapper<LongWritable, Text, Text, SimpleArray> {

	TemporalCorrelatingKey keyGenCall = new TemporalCorrelatingKey(7 * 60, 0);
	TemporalCorrelatingKey keyGenSMS = new TemporalCorrelatingKey(7 * 60, -1 * 7 * 60);

	@Override
	public void configure(JobConf job) {

	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, SimpleArray> output, Reporter reporter)
			throws IOException {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String[] items = value.toString().split("\t");
		

		Long imsi;
		long time;
		int type;
		String number = items[3];
		int amount;
		try {
			time = sdf.parse(items[6]).getTime() / 1000l;
			imsi = Long.valueOf(items[1]);
			type = Integer.parseInt(items[5]);
			amount = Integer.parseInt(items[4]);
		} catch (Exception e) {
			reporter.incrCounter("MAP", "ERROR", 1);
			return;
		}

		StringBuilder sb = new StringBuilder();
		sb.append(Long.toHexString(imsi));
		sb.append(number);
		sb.append("-");
		String keyBase = sb.toString();

		Text[] out = new Text[5];
		out[0] = new Text(Long.toHexString(time));
		out[1] = new Text(Long.toHexString(imsi));
		out[2] = new Text(Integer.toString(type));
		out[3] = new Text(Integer.toHexString(amount));
		out[4] = new Text(number);
		
		
		SimpleArray outputArray = new SimpleArray(out);

		Text okey;
		switch (type) {
		case 3:
			sb = new StringBuilder(keyBase);
			sb.append(Long.toHexString(keyGenSMS.generateBaseKey(time)));
			okey = new Text(sb.toString());
			output.collect(okey, outputArray);
			
			sb = new StringBuilder(keyBase);
			sb.append(Long.toHexString(keyGenSMS.generateTargetKey(time)));
			okey = new Text(sb.toString());
			output.collect(okey, outputArray);
			
			break;
		case 1:
			sb = new StringBuilder(keyBase);
			sb.append(Long.toHexString(keyGenCall.generateBaseKey(time)));
			okey = new Text(sb.toString());

			output.collect(okey, outputArray);
			break;
		default:

		}

	}

	@Override
	public void close() throws IOException {

	}

}
