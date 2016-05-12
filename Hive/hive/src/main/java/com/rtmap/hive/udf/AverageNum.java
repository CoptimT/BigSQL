package com.rtmap.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

@SuppressWarnings("deprecation")
public class AverageNum extends UDAF{
	
	public static class MidResult{
		public int numCount;
		public double totalNum;
	}
	
	public static class AverageEvaluator implements UDAFEvaluator{
		public MidResult midResult = null;
		
		public AverageEvaluator() {
			super();
			midResult = new MidResult();
			init();
		}

		@Override
		public void init() {
			midResult.numCount = 0;
			midResult.totalNum = 0;
		}
		
		public boolean iterate(IntWritable val){
			if(val != null){
				midResult.numCount ++;
				midResult.totalNum += val.get();
			}
			return true;
		}
		
		public MidResult terminatePartial(){
			return midResult.numCount == 0? null:midResult;
		}
		
		public boolean merge(MidResult result){
			if(result != null){
				midResult.numCount += result.numCount;
				midResult.totalNum += result.totalNum;
			}
			return true;
		}
		
		public DoubleWritable terminate(){
			DoubleWritable doubleWritable = null;
			if(midResult.numCount > 0){
				doubleWritable = new DoubleWritable(midResult.totalNum/midResult.numCount);
			}
			return doubleWritable;
		}
	}
}
