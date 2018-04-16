package com.prifender.des;

import static com.prifender.des.DataExtractionServiceProblems.unknownDataSource;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.Problem;

@Controller
public final class DataSourcesApiController implements DataSourcesApi
{
    @Autowired
    private DataExtractionService dataExtractionService;
    
    @Override
    public ResponseEntity<List<DataSource>> listDataSources()
    {
        return new ResponseEntity<List<DataSource>>( this.dataExtractionService.getDataSources(), HttpStatus.OK );
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity findDataSourceById( @PathVariable( "id" ) final String id )
    {
        final DataSource ds = this.dataExtractionService.getDataSource( id );
        
        if( ds == null )
        {
            return new ResponseEntity<Problem>( unknownDataSource( id ), HttpStatus.BAD_REQUEST );
        }
        
        return new ResponseEntity<DataSource>( ds, HttpStatus.OK );
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity addDataSource( @RequestBody final DataSource ds )
    {
        try
        {
            return new ResponseEntity<DataSource>( this.dataExtractionService.addDataSource( ds ), HttpStatus.OK );
        }
        catch( final DataExtractionServiceException e )
        {
            return new ResponseEntity<Problem>( e.getProblem(), HttpStatus.BAD_REQUEST );
        }
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity updateDataSource( @PathVariable( "id" ) String id, @RequestBody DataSource ds )
    {
        try
        {
            this.dataExtractionService.updateDataSource( id, ds );
            return new ResponseEntity<DataSource>( HttpStatus.NO_CONTENT );
        }
        catch( final DataExtractionServiceException e )
        {
            return new ResponseEntity<Problem>( e.getProblem(), HttpStatus.BAD_REQUEST );
        }
    }

    @Override
    public ResponseEntity<Void> deleteDataSourceById( @PathVariable( "id" ) final String id )
    {
        this.dataExtractionService.deleteDataSource( id );
        
        return new ResponseEntity<Void>( HttpStatus.NO_CONTENT );
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity testConnection( @PathVariable( "id" ) final String id )
    {
        try
        {
            return new ResponseEntity<ConnectionStatus>( this.dataExtractionService.testConnection( id ), HttpStatus.OK );
        }
        catch( final DataExtractionServiceException e )
        {
            return new ResponseEntity<Problem>( e.getProblem(), HttpStatus.BAD_REQUEST );
        }
    }
    
    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity getMetadata( @PathVariable( "id" ) final String id )
    {
        try
        {
            return new ResponseEntity<Metadata>( this.dataExtractionService.getMetadata( id ), HttpStatus.OK );
        }
        catch( final DataExtractionServiceException e )
        {
            return new ResponseEntity<Problem>( e.getProblem(), HttpStatus.BAD_REQUEST );
        }
    }

}
