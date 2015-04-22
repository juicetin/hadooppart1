package task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * A composite key used to perform Join
 * @author Ying Zhou
 *
 */
public class TextIntPair {

	private String key;
	private int order;
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getValue() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}	
	
//	public TextIntPair(){
//		this.key = new Text();
//		this.order = new IntWritable();
//	}
	
	public TextIntPair(String key, int order){
		this.key = key;
		this.order = order;
	}
	
//	public void readFields(DataInput in) throws IOException {
//		// TODO Auto-generated method stub
//		key.readFields(in);
//		order.readFields(in);
//		
//	}
//
//	@Override
//	public void write(DataOutput out) throws IOException {
//		// TODO Auto-generated method stub
//		key.write(out);
//		order.write(out);
//	}
//
//	@Override
//	public int compareTo(TextIntPair other) {
//		int cmp = order.compareTo(other.order);
//		if (cmp != 0) {
//			return cmp;
//		}
//		return key.compareTo(other.key);
//	}
//
//	@Override
//	public int hashCode() {
//		return order.hashCode() * 163 + key.hashCode();
//	}
//
//	public boolean equals(Object other) {
//		if (other instanceof TextIntPair) {
//			TextIntPair tip = (TextIntPair) other;
//			return key.equals(tip.key) && order.equals(tip.order);
//		}
//		return false;
//	}
}
