package gzip;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

public class MultiThreadedGzipOutputStream extends FilterOutputStream 
                        implements Closeable, Flushable {
    
    private int blockSize;
    private int bufferSize;
    
    private GzipThreadPoolExecutor executor;
    private AtomicReference<IOException> thrown = new AtomicReference<IOException>();
    
    private ByteArrayOutputStream currentBlock;
    private CountDownLatch previousLatch;
    
    public MultiThreadedGzipOutputStream(OutputStream out, int numThreads) {
        this(out, numThreads, 128 * 1024);
    }
    
    public MultiThreadedGzipOutputStream(OutputStream out, int numThreads, int blockSize) {
        this(out, numThreads, blockSize, 10);
    }

    public MultiThreadedGzipOutputStream(OutputStream out, int numThreads, int blockSize, int bufferSize) {
        super(out);
        
        this.out = out;
        this.blockSize = blockSize;
        this.bufferSize = bufferSize;
        
        executor = new GzipThreadPoolExecutor(numThreads);
        currentBlock = new ByteArrayOutputStream(blockSize);
        previousLatch = new CountDownLatch(0);
    }

    
    //TODO trim amount written to currentBlock to make sure it doesn't exceed configured size
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkAndThrowException();
        
        currentBlock.write(b, off, len);
        if (currentBlock.size() >= blockSize) {
            enqueueBlock();
        }
    }
    
    @Override
    public void write(int b) throws IOException {
        checkAndThrowException();

        currentBlock.write(b);
        
        if (currentBlock.size() >= blockSize) {
            enqueueBlock();
        }
    }

    @Override
    public void close() throws IOException {
        checkAndThrowException();
        
        executor.shutdownNow();
        super.close();
    }

    @Override
    public void flush() throws IOException {
        checkAndThrowException();
        
        if (currentBlock.size() > 0) {
            enqueueBlock();
        }
        
        executor.shutdown();
        
        try{
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
            super.flush();
        } catch(InterruptedException intExc) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            
            throw new IOException("Unable finish compression", intExc);
        }
    }
    
    protected void enqueueBlock() throws IOException {
        CountDownLatch newLatch = new CountDownLatch(1);
        executor.execute( new GzipRunnable(currentBlock, previousLatch, newLatch));
        
        currentBlock = new ByteArrayOutputStream(blockSize);
        previousLatch = newLatch;
    }
    
    protected void checkAndThrowException() throws IOException {
        if (thrown.get() != null) {
            executor.shutdownNow();
            throw thrown.get();
        }
    }
    
    class GzipRunnable implements Runnable {
        ByteArrayOutputStream rawBytes;
        CountDownLatch previousLatch;
        CountDownLatch myLatch;

        GZIPOutputStream gzipOut;
        ByteArrayOutputStream byteOut;

        public GzipRunnable(ByteArrayOutputStream rawBytes, CountDownLatch previousLatch, CountDownLatch myLatch) throws IOException {
            this.rawBytes = rawBytes;
            this.previousLatch = previousLatch;
            this.myLatch = myLatch;

            byteOut = new ByteArrayOutputStream(blockSize);
            gzipOut = new GZIPOutputStream(byteOut);
        }

        @Override
        public void run() {         
            try{
                rawBytes.writeTo(gzipOut);
                gzipOut.flush();
                gzipOut.close();                
                
                previousLatch.await();
                byteOut.writeTo(out);
                myLatch.countDown();
            } catch(InterruptedException intExc) {
                Thread.currentThread().interrupt();
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    class GzipThreadPoolExecutor extends ThreadPoolExecutor {

        public GzipThreadPoolExecutor(int numThreads) {
            super(numThreads, numThreads, 0, TimeUnit.MILLISECONDS, new AlwaysBlockingRunnableLinkedBlockingQueue(bufferSize), new DaemonThreadFactory());
        }

        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);

            if (t != null) {
                handleException(t);
            }

            /* The executor wraps Runnables into FutureTasks, which captures all exceptions.
             * So we must look into the Future result to see if there was an exception.
             */
            if (t == null && r instanceof Future<?>) {
                try {
                    Future<?> future = (Future<?>) r;
                    if (future.isDone())
                        future.get();
                } catch (CancellationException ce) {
                    //Don't care if it was cancelled
                } catch (ExecutionException ee) {
                    handleException(ee.getCause());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void handleException(Throwable t) {
            thrown.compareAndSet(null, new IOException("Gzip thread exception", t));
            shutdownNow();
        }
    }
}
