package qp.operators;

import java.io.ObjectInputStream;

import qp.utils.Batch;

public class BlockNestedJoin extends Join {
	int batchSize; //number of tuples per batch
	
	int leftIndex; //index of join attribute in left table
	int rightIndex; //index of join attribute in right table
	
	String rfname; // file name of right table
	
	static int filenum=0; //unique filenum for this operation
	
	Batch outputBatch; // output buffer
	Batch leftBatch; // buffer for left input stream
	Batch rightBatch; //buffer for right input stream
	ObjectInputStream in; /// file pointer to the right materialized file
	
	int leftCursor; //pointer for left side buffer
	int rightCursoer; //pointer for right side buffer
	
	boolean eosr; //end of stream(right table)
	public BlockNestedJoin(Join jn) {
		super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}
	
	public boolean open() {
		int tupleSize = schema.getTupleSize();
		
		batchSize = Batch.getPageSize() / tupleSize;
		//Read N-1 buffer worth of pages of left relation and store in hashmap
		for(int i=0; i< (numBuff-1); i++) {
			
		}
		return eosr;
	}
	
	public Batch next() {
		return null;
	}
	
	public boolean close() {
		
		return true;
	}
}
