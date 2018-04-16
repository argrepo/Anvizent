package com.prifender.des.transform.standard;

import com.prifender.des.transform.Transformation;
import com.prifender.des.transform.TransformationException;

public final class UpperCaseTransformation implements Transformation
{
    @Override
    public String getName()
    {
        return "UpperCase";
    }

    @Override
    public String getDescription()
    {
        return "Converts a string to upper case.";
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
            throw new TransformationException( "The UpperCase transformation requires a String input, but received " + object.getClass().getName() );
        }
        
        return ( (String) object ).toUpperCase();
    }

}
