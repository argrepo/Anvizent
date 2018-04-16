package com.prifender.des.transform;

/**
 * Interface implemented by transformation functions used by the Data Extraction Service.
 */

public interface Transformation
{
    /**
     * Returns the name of the transformation. The transformation name must be unique within a system.
     */

    String getName();
    
    /**
     * Returns the description of the transformation.
     */
    
    String getDescription();
    
    /**
     * Applies a transformation to the specified object. The transformation must not modify the input.
     * 
     * @param object the input for the transformation; either a value like String or a Map
     * @param args the arguments to parameterize the transformation function
     * @return the output after the transformation
     * @throws TransformationException if the transformation has failed
     */
    
    Object transform( Object object, Object[] args ) throws TransformationException;

}
