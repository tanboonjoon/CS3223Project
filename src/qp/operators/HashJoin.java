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
	
	int leftSize; //count the size of leftRelation;
	int rightSize; //count the size of rightRelation

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
		 * 
		 * Keep track of the size of the relations,
		 * The last hash function must be bigger than the biggest relation
		 * if the hash function is smaller than the relation size, collisions will happen
		 **/
		leftSize = rightSize = 0;
		
		/**
		 * The first hash fn is a prime number to distrbute the relation equallys
		 * The second hash fn is also a prime number but will be incremental by the size of the biggest relation
		 * to reduce the risk of collisions
		 **/
		iHashFn = 295433;
		fHashFn = 295439;

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
				System.out.println("HashJoin:writing the temporay file error");
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
		/**
		 * Partitioning phase, split both left and right relation
		 * into buckets and add them to hashmap using initil hash function
		 **/
		if (eosl == false) {
			System.out.println("Partitioning Left table");
			Batch leftBatch = left.next();
			while (leftBatch != null) {
				for (int j = 0; j < leftBatch.size(); j++) {
					leftSize++;
					Tuple leftTuple = leftBatch.elementAt(j);
					// hash the searchKey with initial hashFn
					int hKey = leftTuple.dataAt(leftIndex).hashCode() % iHashFn;
					if (leftHashTable.containsKey(hKey)) { // check if the hashKey exist alreaday
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
		/**
		 * repeat the partitioning with the right relations
		 */
		if (eosr == false) {
			try {
				in = new ObjectInputStream(new FileInputStream(rfname));
				System.out.println("partitioning right table");
			} catch (IOException io) {
				System.err.println("HashJoin:error in reading the file");
				System.exit(1);
			}
			try {
				Batch rightBatch = (Batch) in.readObject();
				while (rightBatch != null) {
					for (int j = 0; j < rightBatch.size(); j++) {
						Tuple rightTuple = rightBatch.elementAt(j);
						rightSize++;
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
			System.out.println("Right Table is partitioned");
		}

		if (!checkKeySet) {
			searchKeyList = new ArrayList<Object>(leftHashTable.keySet());
			checkKeySet = true;
			//get the biggest size out of the two relation and assign them to the final hash relatioon
		//int biggestSize = Math.max(leftSize, rightSize);
		//	fHashFn = biggestSize + fHashFn;
		}

		
		/**
		 * Probing phase
		 * Partition the left relation and build a hashmap using the final hashfunction
		 * and probe the hashmap with the right relation
		 */
		while (!outBatch.isFull() && kcurs < searchKeyList.size()) {
			for (k = kcurs; k < searchKeyList.size(); k++) {
				Object searchKey = searchKeyList.get(k);
				if (rightHashTable.containsKey(searchKey)) { //check if both left and right are the same partition
					ArrayList<Tuple> leftTupleList = leftHashTable.get(searchKey);
					ArrayList<Tuple> rightTupleList = rightHashTable.get(searchKey);
					/**
					 * build a hash table for the partition using final hash function
					 */
					if (!build) {
						System.out.println("Building partition using searchKey: " + searchKey);
						for (int i = 0; i < leftTupleList.size(); i++) {
							Tuple leftTuple = leftTupleList.get(i);
							int leftHash = leftTuple.dataAt(leftIndex).hashCode() % fHashFn + 1;
							if (probeHashTable.containsKey(leftHash)) {
								probeHashTable.get(leftHash).add(leftTuple);
							} else {
								ArrayList<Tuple> probeLeftList = new ArrayList<Tuple>();
								probeLeftList.add(leftTuple);
								probeHashTable.put(leftHash, probeLeftList);
							}
						}
						build = true;
						System.out.println("build completed");
					}

					for (r = rcurs; r < rightTupleList.size(); r++) {			
						Tuple rightTuple = rightTupleList.get(r);
						/** 
						 * hash the right tuple with the final hash function and check for matches
						 * if there is matches, then all the tuples in the hashMap will be joined
						 * with the tuple tuple using the hashKey
						 */
						int rightHash = rightTuple.dataAt(rightIndex).hashCode() % fHashFn + 1;
						if (probeHashTable.containsKey(rightHash)) {
							ArrayList<Tuple> probeLeftList = probeHashTable.get(rightHash);
							for (l = lcurs; l < probeLeftList.size(); l++) {
								Tuple leftTuple = probeLeftList.get(l);
								Tuple outputTuple = leftTuple.joinWith(rightTuple);
								Debug.PPrint(outputTuple);
								outBatch.add(outputTuple);
					
								if (outBatch.isFull()) {
									//case 1 probeList and right partition exhausted, 
									if (l == probeLeftList.size() - 1 && r == rightTupleList.size() - 1) {
										kcurs = k + 1;
										lcurs = 0;
										rcurs = 0;
										build = false;
										probeHashTable.clear();
									//case 2 right partition is not probed completely;
									} else if (r != rightTupleList.size() - 1 && l == probeLeftList.size() - 1) {
										rcurs = r + 1;
										lcurs = 0;
									//other case where probeList is not probed completely against the right tuple
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
		kcurs++;
		lcurs = 0;
		rcurs = 0;
		build = false;
		probeHashTable.clear(); 

		//if all searchKey is exhausted, the join is completed with no more tuples to compare
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
