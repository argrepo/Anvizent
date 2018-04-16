package com.prifender.des.mock.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.prifender.des.mock.relational.RelationalDataExtractionThread;

public abstract class DataSet<T>
{
    public static final Random RNG = new Random( System.currentTimeMillis() );
    
    private final List<T> data = new ArrayList<T>();
    
    protected DataSet( final String file )
    {
        try( final BufferedReader reader = new BufferedReader( new InputStreamReader( RelationalDataExtractionThread.class.getResourceAsStream( "/data/" + file ), "UTF-8" ) ) )
        {
            for( String line = reader.readLine(); line != null; line = reader.readLine() )
            {
                line = line.trim();
                
                if( line.length() > 0 )
                {
                    this.data.add( parse( line ) );
                }
            }
        }
        catch( final IOException e )
        {
            e.printStackTrace();
        }
    }
    
    protected abstract T parse( String line );
    
    public final T random()
    {
        return this.data.get( RNG.nextInt( this.data.size() ) );
    }

}
