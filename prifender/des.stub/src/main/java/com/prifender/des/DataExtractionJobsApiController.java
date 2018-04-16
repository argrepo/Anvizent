package com.prifender.des;

import static com.prifender.des.DataExtractionServiceProblems.unknownDataExtractionJob;
import static com.prifender.des.DataExtractionServiceProblems.unsupportedOperation;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.Problem;

@Controller
public final class DataExtractionJobsApiController implements DataExtractionJobsApi
{
    @Autowired
    private DataExtractionService dataExtractionService;

    @Override
    public ResponseEntity<List<DataExtractionJob>> listDataExtractionJobs()
    {
        return new ResponseEntity<List<DataExtractionJob>>( this.dataExtractionService.getDataExtractionJobs(), HttpStatus.OK );
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity findDataExtractionJobById( @PathVariable( "id" ) final String id )
    {
        final DataExtractionJob job = this.dataExtractionService.getDataExtractionJob( id );
        
        if( job == null )
        {
            return new ResponseEntity<Problem>( unknownDataExtractionJob( id ), HttpStatus.BAD_REQUEST );
        }
        
        return new ResponseEntity<DataExtractionJob>( job, HttpStatus.OK );
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity startDataExtractionJob( @RequestBody DataExtractionSpec dataExtractionSpec )
    {
        try
        {
            return new ResponseEntity<DataExtractionJob>( this.dataExtractionService.startDataExtractionJob( dataExtractionSpec ), HttpStatus.OK );
        }
        catch( final DataExtractionServiceException e )
        {
            return new ResponseEntity<Problem>( e.getProblem(), HttpStatus.BAD_REQUEST );
        }
    }
    
    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity deleteDataExtractionJobById( @PathVariable( "id" ) final String id )
    {
        try
        {
            this.dataExtractionService.deleteDataExtractionJob( id );
            return new ResponseEntity<Void>( HttpStatus.NO_CONTENT );
        }
        catch( final DataExtractionServiceException e )
        {
            return new ResponseEntity<Problem>( e.getProblem(), HttpStatus.BAD_REQUEST );
        }
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity pauseDataExtractionJob( @PathVariable( "id" ) final String id )
    {
        return new ResponseEntity<Problem>( unsupportedOperation(), HttpStatus.BAD_REQUEST );
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity resumeDataExtractionJob( @PathVariable( "id" ) final String id )
    {
        return new ResponseEntity<Problem>( unsupportedOperation(), HttpStatus.BAD_REQUEST );
    }

}
