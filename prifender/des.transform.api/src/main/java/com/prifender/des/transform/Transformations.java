package com.prifender.des.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

public final class Transformations
{
    private final List<Transformation> transformations;
    
    public Transformations()
    {
        final List<Transformation> transformations = new ArrayList<Transformation>();
        
        for( final Transformation t : ServiceLoader.load( Transformation.class ) )
        {
            transformations.add( t );
        }
        
        this.transformations = Collections.unmodifiableList( transformations );
    }
    
    public List<Transformation> list()
    {
        return this.transformations;
    }
    
    public Transformation find( final String name )
    {
        if( name == null )
        {
            throw new IllegalArgumentException();
        }
        
        for( final Transformation tr : this.transformations )
        {
            if( name.equals( tr.getName() ))
            {
                return tr;
            }
        }
        
        return null;
    }

}
