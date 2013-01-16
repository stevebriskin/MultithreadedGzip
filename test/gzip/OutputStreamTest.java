package gzip;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class OutputStreamTest {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        
        final String filename = "/home/steve/tmp/site_data.5";

        final InputStream is = new BufferedInputStream( new FileInputStream(filename));
        final OutputStream os = new MultiThreadedGzipOutputStream(new BufferedOutputStream( new FileOutputStream(filename + ".multi.gz")),
                8);

        byte[] bytesRead = new byte[64 * 1024];
        int read;
        while ((read = is.read(bytesRead)) > 0) {
            if(read == bytesRead.length) {
                os.write(bytesRead);
            }
            else {
                os.write(bytesRead, 0, read);
            }
        }
        
        is.close();

        os.flush();
        os.close();
        
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
