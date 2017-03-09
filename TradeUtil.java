package tradeprocessing;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.mapr.multiclusterdb.TableUtil;

public class TradeUtil {

    private final static byte[] baseCF = Bytes.toBytes("CF1");
    private final static byte[] priceCol = Bytes.toBytes("price");
    private final static byte[] volumeCol = Bytes.toBytes("vol");

    private final static char delimChar = '_';
    
	public static Get createGet(String symbol, long tradeTime) {
		
		//Get g = new Get(Bytes.toBytes(formRowkey(symbol, tradeTime)));
		//g.addFamily(baseCF);
		
		//return g;
		ArrayList<byte[]> parms = new ArrayList<byte[]>();
		parms.add(baseCF);
		
		return TableUtil.buildGet(formRowkey(symbol,tradeTime), parms);
	}
	
	public static Trade createTradeFromResult(Result result) {
		if (result == null || result.isEmpty()) {
			return null;
		}
	    String rowkey = Bytes.toString(result.getRow());

	    // tokenize rowkey
	    String[] rowkeyTokens = rowkey.split(String.valueOf(delimChar));
	    // reconstitute a valid timestamp from the rowkey digits
	    Long time = Long.MAX_VALUE - Long.parseLong(rowkeyTokens[1]);
	    // Extract price & volume from the result
	    // (Price*100) is stored as long byte-array. 
	    // Divide by 100 to extract to a Float.
	    Float price = (float) (Bytes.toLong(result.getValue(baseCF, priceCol))) / 100f;
	    Long volume = Bytes.toLong(result.getValue(baseCF, volumeCol));

	    //System.out.println(rowkeyTokens[0] + " " + price + " " + volume + " " + time);
		return new Trade(rowkeyTokens[1], price, volume, time);
	}
	
    /**
     * generates a rowkey for tall table implementation. rowkeys descend as the
     * timestamp parameter ascends. rowkey format = SYMBOL_REVERSETIMESTAMP
     * Example: GOOG_9223370654476953800
     *          AMZN_9223370654175261908
     * @param symbol
     * @param time
     * @return
     */
    public static String formRowkey(String symbol, Long time) {
		// Construct the rowkey
		String timeString = String.format("%d", (Long.MAX_VALUE - time));
		String rowkey = symbol + delimChar + timeString;
		//System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey); // DEBUG
	
		return rowkey;
    }
}
