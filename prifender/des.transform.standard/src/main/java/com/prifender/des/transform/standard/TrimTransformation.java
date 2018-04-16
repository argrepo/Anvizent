package com.prifender.des.transform.standard;

import com.prifender.des.transform.Transformation;
import com.prifender.des.transform.TransformationException;

public final class TrimTransformation implements Transformation
{
    @Override
    public String getName()
    {
        return "Trim";
    }
    
    @Override
    public String getDescription()
    {
        return "Removes leading and trailing whitespace from a string.";
    }

    @Override
    public Object transform( final Object object, final Object[] args ) throws TransformationException
    {
        if( object == null )
        {
            return null;
        }
        
        if( ! ( object instanceof String ) )
        {
            throw new TransformationException( "The Trim transformation requires a String input, but received " + object.getClass().getName() );
        }
        
        return ( (String) object ).trim();
    }

}
