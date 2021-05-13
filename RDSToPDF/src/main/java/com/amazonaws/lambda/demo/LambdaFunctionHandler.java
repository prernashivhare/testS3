package com.amazonaws.lambda.demo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        
        try {
			createPDF(context);
		} catch (DocumentException | URISyntaxException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        // TODO: implement your handler
        return "Hello from Lambda!";
    }
    
    public Connection getConnection() {
		 String url = "jdbc:postgresql://database-1.cjpl3vuuema8.us-east-1.rds.amazonaws.com:5432/";
	      String username = "postgres";
	      String password = "ecaredb2021";
	      
	      try {
			return DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

    public static void addTableHeader(PdfPTable table) {
	    Stream.of("ID", "NAME")
	      .forEach(columnTitle -> {
	        PdfPCell header = new PdfPCell();
	        header.setBackgroundColor(BaseColor.LIGHT_GRAY);
	        header.setBorderWidth(2);
	        header.setPhrase(new Phrase(columnTitle));
	        table.addCell(header);
	    });
	}
    
    public void createPDF(Context context) throws DocumentException, URISyntaxException, IOException {
		
    	context.getLogger().log("Inside createPDF ");
    	
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
		Document document = new Document();
		PdfWriter.getInstance(document, out);
		document.open();

		PdfPTable table = new PdfPTable(2);
		addTableHeader(table);
		
		try {
		      Connection conn = getConnection();
		      if(conn != null) {
		    	  Statement stmt = conn.createStatement();
		    	  String query = "select * from employee";
			      ResultSet  rs = stmt.executeQuery(query);
			      while(rs.next())
			      {
			    	  table.addCell(rs.getString(1));
			    	  table.addCell(rs.getString(2));
			      }

		      }
	      

	    } catch (Exception e) {
	      e.printStackTrace();
	    }
		
		document.add(table);
		document.close();
		InputStream in = new ByteArrayInputStream(out.toByteArray());
		
		AWSCredentials credentials = new BasicAWSCredentials(
				"AKIA2VHSFEHAI6ASTDJT", 
				"YBuS8JkLF99ox0ZRDyYlUbcXuwK36OPEgainFieW"
				);
		
		AmazonS3 s3client = AmazonS3ClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(Regions.US_EAST_1)
				  .build();
		
		String bucketName = "new-test-bucket-ps02";
		
		ObjectMetadata meta = null;
		s3client.putObject(
		  bucketName, 
		  "RDS_data.pdf", 
		  in, meta
		);
		
		
		
	}
    
}
