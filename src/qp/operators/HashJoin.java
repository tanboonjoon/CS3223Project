/** hash join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class HashJoin extends Join {

	int batchsize; // Number of tuples per out batch

	/**
	 * The following fields are useful during execution of the HashJoin
	 * operation
	 **/
	int leftIndex; // Index of the join attribute in left table
	int rightIndex; // Index of the join attribute in right table

	String rfname; // The file name where the right table is materialize

	static int filenum = 0; // To get unique filenum for this operation

	Batch outBatch; // Output buffer
	Batch leftBatch; // Buffer for left input stream
	Batch rightBatch; // Buffer for right input stream
	ObjectInputStream in; // File pointer to the right hand materialized file

	int lcurs; // Cursor for left side buffer
	int rcurs; // Cursor for right side buffer
	int kcurs; // Cursor for the list containing searchKeys;

	boolean eosl; // check if left table has been partitioned completely
	boolean eosr; // check if right table has been partitioned completely
	boolean checkKeySet; // check if keySet has been obtained for probing phase
	boolean checkHj; // check if hashjoin has been completed;
	boolean build; // check if there is a need to partition again

	HashMap<Object, ArrayList<Tuple>> leftHashTable; // hashtable for left input
														// stream
	HashMap<Object, ArrayList<Tuple>> rightHashTable; // hashtable for right
														// input stream
	HashMap<Object, ArrayList<Tuple>> probeHashTable; // for probing
	List<Object> searchKeyList; // list containing all the searchKey for left
								// table;
	List<Object> rightKeyList; // list containing all the searchKey for right
								// table;

	int iHashFn; // Hash fn for partitioning phase
	int fHashFn; // Hash fn for probing phase

	public HashJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	/**
	 * During open finds the index of the join attributes Materializes the right
	 * hand side into a file Opens tWWhe connections
	 **/

	public boolean open() {

		/** select number of tuples per batch **/
		int tuplesize = schema.getTupleSize();
		batchsize = Batch.getPageSize() / tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftIndex = left.getSchema().indexOf(leftattr);
		rightIndex = right.getSchema().indexOf(rightattr);
		Batch rightpage;

		leftHashTable = new HashMap<Object, ArrayList<Tuple>>();
		rightHashTable = new HashMap<Object, ArrayList<Tuple>>();
		probeHashTable = new HashMap<Object, ArrayList<Tuple>>();

		/** initialize the cursors of input buffers **/

		lcurs = 0;
		rcurs = 0;
		kcurs = 0; // this cursors keep track of the current searchKey;

		/**
		 * initiate the hash fn, hash fn should be a prime number to avoid
		 * collision
		 **/

		iHashFn = 53;
		fHashFn = 97;

		eosl = false;
		eosr = false;
		checkKeySet = false;
		checkHj = false;
		build = false;
		/**
		 * Right hand side table is to be materialized for the Nested join to
		 * perform
		 **/

		if (!right.open()) {
			return false;
		} else {
			/**
			 * If the right operator is not a base table then Materialize the
			 * intermediate result from right into a file
			 **/

			// if(right.getOpType() != OpType.SCAN){
			filenum++;
			rfname = "NJtemp-" + String.valueOf(filenum);
			try {
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
				while ((rightpage = right.next()) != null) {
					out.writeObject(rightpage);
				}
				out.close();
			} catch (IOException io) {
				System.out.println("NestedJoin:writing the temporay file error");
				return false;
			}
			// }
			if (!right.close())
				return false;
		}
		if (left.open())
			return true;
		else
			return false;
	}

	/**
	 * from input buffers selects the tuples satisfying join condition And
	 * returns a page of output tuples
	 **/

	public Batch next() {
		// System.out.print("HashJoin:--------------------------in
		// next----------------");
		// Debug.PPrint(con);
		// System.out.println();
		int l, r, k;
		if (checkHj) {
			close();
			return null;
		}
		outBatch = new Batch(batchsize);
		/*
		 * Partitioning phase will only happen the first time, dividing
		 */
		if (eosl == false) {
			Batch leftBatch = left.next();
			while (leftBatch != null) {
				for (int j = 0; j < leftBatch.size(); j++) {
					Tuple leftTuple = leftBatch.elementAt(j);
					// hash the searchKey with initial hashFn
					int hKey = leftTuple.dataAt(leftIndex).hashCode() % iHashFn;
					if (leftHashTable.containsKey(hKey)) { // check if the
															// hashed searchKey
															// exist already
						leftHashTable.get(hKey).add(leftTuple);
					} else {
						ArrayList<Tuple> leftTupleList = new ArrayList<Tuple>();
						leftTupleList.add(leftTuple);
						leftHashTable.put(hKey, leftTupleList);

					}
				}
				leftBatch = left.next();
			}
			System.out.println("Left Table is partitioned");
			eosl = true;
		}
		// repeat the partitioning for right table
		if (eosr == false) {
			try {
				in = new ObjectInputStream(new FileInputStream(rfname));
				System.out.println("partitioning right table");
			} catch (IOException io) {
				System.err.println("NestedJoin:error in reading the file");
				System.exit(1);
			}
			try {
				Batch rightBatch = (Batch) in.readObject();
				while (rightBatch != null) {
					for (int j = 0; j < rightBatch.size(); j++) {
						Tuple rightTuple = rightBatch.elementAt(j);
						int hKey = rightTuple.dataAt(rightIndex).hashCode() % iHashFn;
						if (rightHashTable.containsKey(hKey)) {
							rightHashTable.get(hKey).add(rightTuple);
						} else {
							ArrayList<Tuple> rightTupleList = new ArrayList<Tuple>();
							rightTupleList.add(rightTuple);
							rightHashTable.put(hKey, rightTupleList);
						}
					}
					rightBatch = (Batch) in.readObject();
				}
			} catch (EOFException e) {
				try {
					System.out.println("Right Table is partitioned");

					in.close();
				} catch (IOException io) {
					System.out.println("HashJoin:Error in temporary file reading");
				}
				eosr = true;
			} catch (ClassNotFoundException c) {
				System.out.println("HashJoin:Some error in deserialization ");
				System.exit(1);
			} catch (IOException io) {
				System.out.println("HashJoin:temporary file reading error");
				System.exit(1);
			}
			eosr = true;
		}

		if (!checkKeySet) {
			searchKeyList = new ArrayList<Object>(leftHashTable.keySet());
			checkKeySet = true;
		}
		while (!outBatch.isFull() && kcurs < searchKeyList.size()) {
			for (k = kcurs; k < searchKeyList.size(); k++) {
				Object searchKey = searchKeyList.get(k);
				System.out.println(searchKey + " fail");
				if (rightHashTable.containsKey(searchKey)) {
					System.out.println(searchKey);
					ArrayList<Tuple> leftTupleList = leftHashTable.get(searchKey);
					ArrayList<Tuple> rightTupleList = rightHashTable.get(searchKey);
					if (!build) {
						for (int i = 0; i < leftTupleList.size(); i++) {
							Tuple leftTuple = leftTupleList.get(i);
							int leftHash = leftTuple.dataAt(leftIndex).hashCode() % fHashFn;
							if (probeHashTable.containsKey(leftHash)) {
								probeHashTable.get(leftHash).add(leftTuple);
							} else {
								ArrayList<Tuple> probeLeftList = new ArrayList<Tuple>();
								probeLeftList.add(leftTuple);
								probeHashTable.put(leftHash, probeLeftList);
							}
						}
						build = true;
					}

					for (r = rcurs; r < rightTupleList.size(); r++) {
						Tuple rightTuple = rightTupleList.get(r);
						int rightHash = rightTuple.dataAt(rightIndex).hashCode() % fHashFn;
						if (probeHashTable.containsKey(rightHash)) {
							ArrayList<Tuple> probeLeftList = probeHashTable.get(rightHash);
							for (l = lcurs; l < probeLeftList.size(); l++) {
								Tuple leftTuple = probeLeftList.get(l);
								Tuple outputTuple = leftTuple.joinWith(rightTuple);
								Debug.PPrint(outputTuple);
								outBatch.add(outputTuple);
								if (outBatch.isFull()) {
									if (l == probeLeftList.size() - 1 && r == rightTupleList.size() - 1) {
										kcurs = k + 1;
										lcurs = 0;
										rcurs = 0;
										build = false;
										probeHashTable.clear();
									} else if (r != rightTupleList.size() - 1 && l == probeLeftList.size() - 1) {
										rcurs = r + 1;
										lcurs = 0;
									} else {
										lcurs = l + 1;
									}
									return outBatch;
								}
							}
						}
						rcurs++;
						lcurs = 0;	
					}
				}
				kcurs++;
				lcurs = 0;
				rcurs = 0;
				build = false;
				probeHashTable.clear();
			}
		}


		if (kcurs >= searchKeyList.size()) {
			checkHj = true;
		}
		return outBatch;

	}

	/** Close the operator */
	public boolean close() {

		File f = new File(rfname);
		f.delete();
		return true;

	}

}
