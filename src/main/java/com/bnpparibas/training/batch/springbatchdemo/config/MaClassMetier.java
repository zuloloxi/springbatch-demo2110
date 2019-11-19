package com.bnpparibas.training.batch.springbatchdemo.config;

import com.bnpparibas.training.batch.springbatchdemo.dto.BookDto;
//import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class MaClassMetier {
    public BookDto maMethodMetier(BookDto book){
        if (book.getPublishedOn()>2019) {
            return null;
        }
        return book;

    }

}
