package com.prifender.des.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.CharEncoding;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public final class DocExtractionUtil 
{
	public static String extractFileText(final String filePath) throws IOException, SAXException, TikaException 
	{
		Parser parser = new AutoDetectParser();
		BodyContentHandler handler = new BodyContentHandler(10000000);
		Metadata metadata = new Metadata();
		InputStream content;
		try {
			content = new FileInputStream(filePath);
			parser.parse(content, handler, metadata, new ParseContext());
		} catch (IOException | SAXException | TikaException e) {
			e.printStackTrace();
			throw e;
		}    
		return handler.toString();
	}
	
	public static String extractFileText(final File file) throws IOException, SAXException, TikaException 
	{
		/*Parser parser = new AutoDetectParser();
		BodyContentHandler handler = new BodyContentHandler(10000000);
		Metadata metadata = new Metadata();
		InputStream input = new FileInputStream(file);    
		parser.parse(input, handler, metadata, new ParseContext());*/
		
		Metadata meta = new Metadata();
		ContentHandler handler = new BodyContentHandler(-1);
		Parser parser = new AutoDetectParser(new TikaConfig(TikaConfig.class.getClassLoader()));
		
		InputStream is = new FileInputStream(file);
		parser.parse(is, handler, meta, new ParseContext());
		
		is.close();
		return handler.toString();
	}

	public static String createDir(String dirName) 
	{
		String fielStore = System.getProperty("java.io.tmpdir") + "/" + dirName + "/";
		
		File tempPath = new File(fielStore);
		
		if (!tempPath.exists()) 
		{
			tempPath.mkdirs();
		}
		
		return fielStore;
	}
	
	public static void deleteFiles(String dirName) 
	{
		try{
			File tmpDir = new File(dirName);
			
			if (tmpDir.exists() && tmpDir.isDirectory()) 
			{
				for (File file: tmpDir.listFiles()) 
				{
	                file.delete();
	            }
			} 
			tmpDir.delete();
		} catch(Exception e) 
		{
			// Do Nothing
		}
	}
	
	public static List<String> splitStringToChunks(final String contentString, final int chunkSize) throws UnsupportedEncodingException 
	{
		final String regExp = "\\r?\\n";
		List<String> chunkList = new ArrayList<>();
		
		String tokens[] = contentString.split(regExp);
		StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i < tokens.length; i++) 
		{
			sb.append(tokens[i]).append(" ");
			String tempStr = sb.toString();
			if(tempStr.getBytes(CharEncoding.UTF_8).length > chunkSize) 
			{
				chunkList.add(tempStr);
				sb = new StringBuilder();
			}
		}
		
		if(chunkList.size() == 0) 
		{
			chunkList.add(sb.toString());
		}
		
		return chunkList;
	}
	
}