/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ozonecft.preproc;

import java.lang.IndexOutOfBoundsException;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class PositionNotFoundException extends IndexOutOfBoundsException {

    public PositionNotFoundException() {
        super();
    }

    public PositionNotFoundException(String s) {
        super(s);
    }
    
}
