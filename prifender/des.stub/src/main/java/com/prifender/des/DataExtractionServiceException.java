package com.prifender.des;

import com.prifender.des.model.Problem;

public final class DataExtractionServiceException extends Exception
{
    private static final long serialVersionUID = 1L;

    private final Problem problem;
    
    public DataExtractionServiceException( final Problem problem )
    {
        super( problem.getMessage() );
        
        this.problem = problem;
    }
    
    public Problem getProblem()
    {
        return this.problem;
    }

}
