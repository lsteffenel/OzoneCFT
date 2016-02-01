/* *************************************************************** *
 * OzoneHadoop Project
 * Author : Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 * *************************************************************** */

package ozonecft.preproc;

import java.util.Calendar;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class O3 {
    private int ozone[][];
    private float lat[],lon[];
    private Calendar date;
    private float initlat = -90;
    private float initlon = -180;
    private float dlat = 1;
    private float dlon = 1;
    
    public O3 (float dLat, float dLon) {
        this.dlat = dLat;
        this.dlon = dLon;
        
        lon = new float[(int)(360/dlon)];
        lat = new float[(int)(180/dlat)];
        ozone = new int[(int)(180/dlat)][(int)(360/dlon)];
       
    }
    
//    public void setdLat (float dLat) {
//        this.dlat = dLat;
//        lat = new float[(int)(180/dLat)];
//        ozone = new int[(int)(360/dlon)][(int)(180/dlat)];
//    }
//    
//    public void setdLon (float dLon) {
//        this.dlon = dLon;
//        lon = new float[(int)(360/dlon)];
//        ozone = new int[(int)(360/dlon)][(int)(180/dlat)];
//    }
    
    public void setLat (float init)
    {
        initlat = init;
        for (int i=0; i<180/dlat; i++) {
            lat[i] = init+i*dlat;
        }
    }
    
    public void setLon (float init)
    {
        initlon = init;
        for (int i=0; i<360/dlon; i++) {
            lon[i] = init+i*dlon;
        }
    }
    
    
    public float getinitLat ()
    {
        return initlat;
    }
    
    public float getinitLon ()
    {
        return initlon;
    }
    
    public float getdLat ()
    {
        return dlat;
    }
 
    public float getdLon ()
    {
        return dlon;
    }
    
    public void setDate (Calendar date) {
        this.date = date;
    }
    
    public Calendar getDate ()
    {
        return this.date;
    }
    
    public void setOzone(int latit, int longit, int value) {
        ozone[latit][longit] = value;
    }
    
    public int getValue(float latitude, float longitude) {
        int xpos=0,ypos=0;
        
        for (int i = 0; i<lat.length;++i) {
            if (latitude>=(lat[i]-dlat) && latitude < (lat[i]+dlat))
            {
                xpos = i;
                break;
            }
        }
        
        for (int i = 0; i<lon.length;++i) {
            if (longitude>=(lon[i]-dlon) && longitude < (lon[i]+dlon))
            {
                ypos = i;
                break;
            }
        }
        return ozone[xpos][ypos];
        
        
    }
}
