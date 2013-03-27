package eu.activelogic.key.correlation;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class SimpleArray extends ArrayWritable {

	public SimpleArray() {
		super(Text.class);
	}

	public SimpleArray(Text[] v) {
		super(Text.class, v);
	}

	@Override
	public String toString() {
		return Arrays.toString(super.toStrings());
	}
	
}
