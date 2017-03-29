/** Enumeration of join algorithm types
	Change this class depending on actual algorithms
	you have implemented in your query processor 

**/


package qp.operators;

import qp.utils.*;
public class JoinType{



   public static final int NESTEDJOIN = 0;
   public static final int BLOCKNESTED = 1;
   public static final int HASHJOIN = 2;


   public static int numJoinTypes(){
	
	return HASHJOIN;

        // return k for k joins
    }

}

