package com.prifender.des;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

import com.prifender.des.model.DataSourceType;

@Controller
public final class SupportedDataSourceTypesApiController implements SupportedDataSourceTypesApi
{
    @Autowired
    private DataExtractionService dataExtractionService;
    
    @Override
    public ResponseEntity<List<DataSourceType>> listSupportedDataSourceTypes()
    {
        return new ResponseEntity<List<DataSourceType>>( this.dataExtractionService.getSupportedDataSourceTypes(), HttpStatus.OK );
    }

}
