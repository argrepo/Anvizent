package com.prifender.des.mock.data;

import static com.prifender.des.mock.data.DataSet.RNG;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.prifender.des.mock.relational.RelationalDataExtractionThread;

public abstract class WeightedDataSet<T>
{
    public static final class Entry<T>
    {
        public final T value;
        public final int weight;
        
        public Entry( final T value, final int weight )
        {
            this.value = value;
            this.weight = weight;
        }
    }

    private final List<Entry<T>> data = new ArrayList<Entry<T>>();
    private final int sumOfAllWeights;
    
    protected WeightedDataSet( final String file )
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
        
        int totalWeights = 0;
        
        for( final Entry<T> entry : this.data )
        {
            totalWeights += entry.weight;
        }
        
        this.sumOfAllWeights = totalWeights;
    }
    
    protected abstract Entry<T> parse( String line );
    
    public final T random()
    {
        final int rnd = RNG.nextInt( this.sumOfAllWeights );
        int total = 0;
        
        for( final Entry<T> entry : this.data )
        {
            total += entry.weight;
            
            if( rnd < total )
            {
                return entry.value;
            }
        }
        
        throw new IllegalStateException();
    }
    
    public void testDistribution()
    {
        final Map<T,Integer> counts = new HashMap<T,Integer>();
        final int sampleSize = 1000 * 1000;
        
        for( int i = 0; i < sampleSize; i++ )
        {
            final T random = random();
            
            Integer count = counts.get( random );
            
            if( count == null )
            {
                count = 1;
            }
            else
            {
                count = count + 1;
            }
            
            counts.put( random, count );
        }
        
        for( final Entry<T> entry : this.data )
        {
            final double actual = (double) counts.get( entry.value ) / ( sampleSize / 100 );
            System.out.println( entry.value + ": expected " + entry.weight + ", actual " + actual );
        }
    }

}
