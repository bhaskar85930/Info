package tradeprocessing;

import java.io.IOException;
import java.util.List;

import com.maprdemo.multiclusterdb.TableService;


public class GetTrades {
    
	public static void main(String[] args) throws IOException {
		// 1. Check for correct args
		// args[0]: path to input data file (optional)
		if (args.length < 1) { // quit if not enough args provided
		    System.out.println("Usage: GetTrades [<input file>]");
		    System.out.println("Example: GetTrades input.txt");
		    return;
		}

		String file = args[0];

		// Generate a test data set
		List<Trade> testTradeSet = generateData(file);
		// store the data set to the table via the DAO
		getData(testTradeSet);
		//return;
	}

    public static void getData(List<Trade> testTradeSet)
    	    throws IOException {
    	TableService ts = new TableService("/user/ec2-user/trades_tall");
    	Trade result = null;
    	//long end_time = 0;
    	System.out.println("Starting Test...");
    	for (Trade trade : testTradeSet) {
    	    result = TradeUtil.createTradeFromResult(ts.getFromTable(TradeUtil.createGet(trade.getSymbol(), trade.getTime())));
    	    if (result != null)
    	    	System.out.println("Trade: " + result.toString());
    	    else
    	    	System.out.println("Not result for symbol: " + trade.getSymbol() + " time: " + trade.getTime());
    	}
    	System.out.println("Test Complete....");
    }

    public static List<Trade> generateData(String file) throws IOException {
		List<Trade> testTradeSet;
		// If no input file specified, use a predefined data set
		if (file == null) {
		    System.out
			    .println("No input data provided. Creating a small, pre-defined data set.");
		    testTradeSet = CreateTableUtils.generateDataSet();
		} else { // Read the specified input file.
		    testTradeSet = CreateTableUtils.getDataFromFile(file);
		}
		return testTradeSet;
    }
}
