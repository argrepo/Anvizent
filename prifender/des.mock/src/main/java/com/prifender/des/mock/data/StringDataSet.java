package com.prifender.des.mock.data;

public final class StringDataSet extends DataSet<String>
{
    protected StringDataSet( final String file )
    {
        super( file );
    }
    
    protected final String parse( final String line )
    {
        return line;
    }

}
