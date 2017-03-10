package schemadesign;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class GetTrades {

    private static String tablePath;
    private static String inputFilePath;
    private final static byte[] baseCF = Bytes.toBytes("CF1");
    private final static byte[] priceCF = Bytes.toBytes("price");
    private final static byte[] volumeCF = Bytes.toBytes("vol");
    private final static byte[] statsCF = Bytes.toBytes("stats");
    
	public static void main(String[] args) throws IOException {
		// 1. Check for correct args
		// args[0]: tall or flat (required)
		// args[1]: path to input data file (optional)
		if (args.length < 1) { // quit if not enough args provided
		    System.out.println("Usage: GetTrades {tall | flat}  [<input file>]");
		    System.out.println("Example: GetTrades tall  input.txt");
		    return;
		}
		String tableType = args[0];
		// quit if args[0]: tall or flat, not specified
		if (!(tableType.equals("tall") || tableType.equals("flat"))) {
		    System.out.println("Specify a schema type of 'tall' or 'flat'.");
		    return;
		}
		String file = null;
		if (args.length > 1) {
		    file = args[1];
		}

		// Create a table, create a DAO passing table path in constructor
		Configuration conf = HBaseConfiguration.create();
		TradeDAO tradeDao;

		if (tableType.equals("tall")) {
		    tablePath = TradeDAOTall.tablePath;
		    //CreateTableUtils.createTable(conf, tablePath, new byte[][] { baseCF });
		    tradeDao = new TradeDAOTall(conf);
		} else if (tableType.equals("flat")) {
		    tablePath = TradeDAOFlat.tablePath;
		    //CreateTableUtils.createTable(conf, tablePath, new byte[][] { priceCF,
			 //   volumeCF, statsCF });
		    tradeDao = new TradeDAOFlat(conf);
		} else {
		    tradeDao = null;
		}

		// Generate a test data set
		List<Trade> testTradeSet = generateData(file);
		// store the data set to the table via the DAO
		getData(tradeDao, testTradeSet);
		return;
	}

    public static void getData(TradeDAO tradeDao, List<Trade> testTradeSet)
    	    throws IOException {
    	long start_time = 0;
    	//long end_time = 0;
    	System.out.println("Using DAO: " + tradeDao.getClass());
    	System.out.println("Getting the test data set...");
    	for (Trade trade : testTradeSet) {
    		start_time = System.nanoTime();
    	    tradeDao.getTrade(trade.getSymbol(), trade.getTime());
    	    System.out.println("Processing time: " + (System.nanoTime() - start_time)/1e6);
    	}
    }

    public static List<Trade> generateData(String file) throws IOException {
		List<Trade> testTradeSet;
		// If no input file specified, use a predefined data set
		if (file == null) {
		    System.out
			    .println("No input data provided. Creating a small, pre-defined data set.");
		    testTradeSet = CreateTableUtils.generateDataSet();
		} else { // Read the specified input file.
		    inputFilePath = file;
		    testTradeSet = CreateTableUtils.getDataFromFile(inputFilePath);
		}
		return testTradeSet;
    }
}
