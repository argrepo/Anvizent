package com.prifender.des;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.multipart.MultipartFile;

import com.prifender.des.model.Problem;
import com.prifender.des.model.TransformationDef;

@Controller
public final class TransformationsApiController implements TransformationsApi
{
    @Autowired
    private DataExtractionService dataExtractionService;
    
    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity listTransformations()
    {
        try
        {
            return new ResponseEntity<List<TransformationDef>>( this.dataExtractionService.getTransformations(), HttpStatus.OK );
        }
        catch( final DataExtractionServiceException e )
        {
            return new ResponseEntity<Problem>( e.getProblem(), HttpStatus.BAD_REQUEST );
        }
    }

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity addTransformation( @RequestPart("file") MultipartFile trFile )
    {
        try
        {
            return new ResponseEntity<TransformationDef>( this.dataExtractionService.addTransformation( trFile ), HttpStatus.OK );
        }
        catch( final DataExtractionServiceException e )
        {
            return new ResponseEntity<Problem>( e.getProblem(), HttpStatus.BAD_REQUEST );
        }
    }

}
