package edu.ucla.cs.cs144;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.*;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchConstraint;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {

	/* 
         * You will probably have to use JDBC to access MySQL data
         * Lucene IndexSearcher class to lookup Lucene index.
         * Read the corresponding tutorial to learn about how to use these.
         *
         * Your code will need to reference the directory which contains your
	 * Lucene index files.  Make sure to read the environment variable 
         * $LUCENE_INDEX with System.getenv() to build the appropriate path.
	 *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
         * so that they are not exposed to outside of this class.
         *
         * Any new classes that you create should be part of
         * edu.ucla.cs.cs144 package and their source files should be
         * placed at src/edu/ucla/cs/cs144.
         *
         */
	
	private IndexSearcher searcher = null;
	private QueryParser parser = null;
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn) throws CorruptIndexException, IOException, ParseException {
		
		ArrayList<SearchResult> results = new ArrayList<SearchResult>();
		
		searcher = new IndexSearcher(System.getenv("LUCENE_INDEX") + "/index1");
		parser = new QueryParser("content", new StandardAnalyzer());
	
		Query q = parser.parse(query);
		Hits hits = searcher.search(q);
		
		int stopIndex = numResultsToReturn == 0 ? hits.length() : numResultsToReturn + numResultsToSkip;	
		for(int i = numResultsToSkip; i < hits.length() && i < stopIndex; i++) {
		  Document doc = hits.doc(i);
		  
		  SearchResult s = new SearchResult();
		  s.setItemId(doc.get("itemId"));
		  s.setName(doc.get("name"));
		  
		  results.add(s);
		}
		
		SearchResult[] r = {};
		r = results.toArray(r);
		return r;
	}

	public SearchResult[] advancedSearch(SearchConstraint[] constraints, 
			int numResultsToSkip, int numResultsToReturn) throws CorruptIndexException, IOException,
      ParseException, SQLException {
    parser = null;
    searcher = new IndexSearcher(System.getenv("LUCENE_INDEX") + "/index1");
    LinkedHashMap<String, String> results = null;
    for(int i = 0; i < constraints.length; i++)
    {
      if(constraints[i].getFieldName().equals("ItemName"))
      {
        parser = new QueryParser("name", new StandardAnalyzer());
      }
      else if(constraints[i].getFieldName().equals("Category"))
      {
        parser = new QueryParser("category", new StandardAnalyzer());
      }
      else if(constraints[i].getFieldName().equals("Description"))
      {
        parser = new QueryParser("description", new StandardAnalyzer());
      }

      if(parser != null)
      {
        Query query = parser.parse(constraints[i].getValue());
        Hits hits = searcher.search(query);
        if(results == null)
        {
          results = new LinkedHashMap<String, String>(((int)(hits.length()/.75))+1);
          for(int j = 0; j < hits.length(); j++)
          {
            Document doc = hits.doc(j);
            results.put(doc.get("itemId"), doc.get("name"));
          }
        }
        else
        {
          LinkedHashMap<String, String> temp = new LinkedHashMap(((int)(hits.length()/.75))+1);
          
          for(int j = 0; j < hits.length(); j++)
          {
            Document doc = hits.doc(j);
            if(results.containsKey(doc.get("itemId")))
            {
              temp.put(doc.get("itemId"), doc.get("name"));
            }
          }
          results = temp;
        }
        parser = null;
      }
    }

    Connection con = DbManager.getConnection(true);

    PreparedStatement stmt = null;
    for(int i = 0; i < constraints.length; i++)
    {
      if(constraints[i].getFieldName().equals("SellerId"))
      {
        stmt = con.prepareStatement("SELECT ItemId, Name FROM Item WHERE Seller=\"" + constraints[i].getValue() + "\"");
      }
      else if(constraints[i].getFieldName().equals("BuyPrice"))
      {
        stmt = con.prepareStatement("SELECT ItemId, Name FROM Item WHERE BuyPrice=" + constraints[i].getValue());
      }
      else if(constraints[i].getFieldName().equals("BidderId"))
      {
        stmt = con.prepareStatement("SELECT I.ItemId, I.Name FROM Bid AS B INNER JOIN Item AS I ON I.ItemId=B.ItemId WHERE Bidder=\"" + constraints[i].getValue() + "\"");
      }
      else if(constraints[i].getFieldName().equals("EndTime"))
      {
        stmt = con.prepareStatement("SELECT ItemId, Name FROM Item WHERE Ends=\"" + formatDate(constraints[i].getValue()) + "\"");
      }

      if(stmt != null)
      {
        ResultSet rs = stmt.executeQuery();
        if(results == null)
        {
          rs.last();
          results = new LinkedHashMap<String, String>(((int)(rs.getRow()/.75))+1);
          rs.beforeFirst();
          while(rs.next())
          {
            results.put(rs.getString("ItemId"), rs.getString("Name"));
          }
        }
        else
        {
          rs.last();
          LinkedHashMap<String, String> temp = new LinkedHashMap<String, String>(((int)(rs.getRow()/.75))+1);
          rs.beforeFirst();
          while(rs.next())
          {
            if(results.containsKey(rs.getString("ItemId")))
            {
              temp.put(rs.getString("ItemId"), rs.getString("Name"));
            }
          }
          results = temp;
        }
        stmt = null;
      }
    }

    SearchResult[] r = new SearchResult[results.size()];
    int i = 0;
    for(Map.Entry<String, String> entry : results.entrySet())
    {
      SearchResult temp = new SearchResult(entry.getKey(), entry.getValue());
      r[i] = temp;
      i++;
    }
    
		return r;
	}

	public static String formatXMLDate(String indate) {
		Date out = null;
		SimpleDateFormat outformat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
		SimpleDateFormat informat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			out = informat.parse(indate);
		} catch (java.text.ParseException e) {
			System.err.println("bad date format");
			return indate;
		}
		return outformat.format(out);
	}
	
	public static String escapeXML( String s ) {
		s = s.replace("&", "&amp;");
		s = s.replace("\"", "&quot;");
		s = s.replace("\'", "&apos;");
		s = s.replace("<", "&lt;");
		s = s.replace(">", "&gt;");
		
		return s;
	}
    
	public String getXMLDataForItemId(String itemId) throws SQLException {
		// TODO: Your code here!
		StringBuilder out = new StringBuilder();
		
		//get the item from db
		
		Connection con = DbManager.getConnection(true);
		
		Statement selectItem = con.createStatement();
		Statement selectCat = con.createStatement();
		Statement selectBid = con.createStatement();
        ResultSet rs = selectItem.executeQuery("SELECT * from Item join User on Item.Seller=User.UserId where ItemId=" + itemId);
        ResultSet catrs = selectCat.executeQuery("SELECT Category from ItemCategory WHERE ItemId=" + itemId);
        ResultSet bidrs = selectBid.executeQuery("SELECT * from Bid join User on Bid.Bidder=User.UserId where ItemId=" + itemId);
        
        if( !rs.next() ) {
        	System.err.println("no results returned");
        	System.exit(-1);
        }
        
        out.append("<Item ItemId=\"" + rs.getString("ItemId") + "\">\n");
        out.append("  <Name>" + escapeXML(rs.getString("Name")) + "</Name>\n");
        while( catrs.next() ) {
        	out.append("    <Category>" + escapeXML(catrs.getString("Category")) + "</Category>\n");
        }
        out.append("  <Currently>$" + rs.getString("Currently") + "</Currently>\n");
	    out.append("  <Buy_Price>$" + rs.getString("BuyPrice") + "</Buy_Price>\n");
	    out.append("  <First_Bid>$" + rs.getString("FirstBid") + "</First_Bid>\n");
   	    out.append("  <Number_of_Bids>" + rs.getString("NumberOfBids") + "</Number_of_Bids>\n");
   	    out.append("  <Bids>\n");
   	    while( bidrs.next() ) {
   	    	out.append("    <Bid>\n");
   	        out.append("      <Bidder UserID=\"" + escapeXML(bidrs.getString("User.UserId")) + "\" Rating=\"" + bidrs.getString("Rating") + "\">\n");
   	        out.append("        <Location>" + escapeXML(bidrs.getString("Location")) + "</Location>\n" );
   	        out.append("        <Country>" + escapeXML(bidrs.getString("Country")) + "</Country>\n" );
   	        out.append("      </Bidder>\n");
   	        out.append("      <Time>" + formatXMLDate( bidrs.getString("Time") )+ "</Time>\n");
   	        out.append("      <Amount>$" + bidrs.getString("Amount") + "</Amount>\n");
   	        out.append("    </Bid>\n");
   	    }
   	    out.append("  </Bids>\n");
	    out.append("  <Location>" + escapeXML(rs.getString("Location")) + "<Location/>\n");	
		out.append("  <Country>" + escapeXML(rs.getString("Country")) + "<Country/>\n");   
		out.append("  <Started>" + formatXMLDate( rs.getString("Started") ) + "<Started/>\n"); 
		out.append("  <Ends>" + formatXMLDate( rs.getString("Ends") ) + "<Ends/>\n"); 	
		out.append("  <Seller UserID=\"" + escapeXML(rs.getString("User.UserId")) + "\" Rating=\"" + rs.getString("Rating") + "\">\n");
		out.append("  <Description>" + escapeXML(rs.getString("Description")) + "</Description>\n");
		out.append("</Item>\n");
	
		
		selectItem.close();
		selectCat.close();
		selectItem.close();
		rs.close();
		catrs.close();
		bidrs.close();
		
		return out.toString();
	}
	
	public String echo(String message) {
		return message;
	}
	

  private static String formatDate(String indate)
  {
    SimpleDateFormat informat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
    SimpleDateFormat outformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      Date started = informat.parse(indate);
      return outformat.format(started);
    } catch(java.text.ParseException a) {
      System.err.println("Caught IOException: " + a.getMessage());
    }
    System.exit(1);
    return "SHOULD NEVER DO THIS";
  }


}
