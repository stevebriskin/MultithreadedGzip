package gzip;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import java.util.zip.GZIPOutputStream;

public class MultiThreadedGzipPOC {

	public static int CHUNK_SIZE = 1024 * 1024;
	public static int NUM_THREADS = 4;
	public static int BUFFER_SIZE = 100;

	static final ArrayBlockingQueue<byte[]> resultBuffer = new ArrayBlockingQueue<byte[]>(BUFFER_SIZE);

	public static void main(String[] args) throws Exception {		
		long start = System.currentTimeMillis();
		
		final String filename = "/Users/stevebriskin/temp/file_1GB_zero";

		final InputStream is = new BufferedInputStream( new FileInputStream(filename));
		final OutputStream os = new BufferedOutputStream( new FileOutputStream(filename + ".multi.gz"));
		
		Thread writer = new Thread() {
			public void run() {
				byte[] toWrite;
				boolean done = false;
				while (!done) {
					try{
						toWrite = resultBuffer.take();
						
						if(toWrite.length == 0) {
							done = true;
						}
						else {
							os.write(toWrite);
						}
					} catch(Exception e) {
						done = true;
						System.err.println(e);
						e.printStackTrace(System.err);

						System.exit(1);
					}
				}
			}
		};

		writer.start();

		GzipThreadPoolExecutor executor = new GzipThreadPoolExecutor(NUM_THREADS);

		byte[] bytesRead = new byte[CHUNK_SIZE];
		int read;
		CountDownLatch preLatch = new CountDownLatch(0);
		while ((read = is.read(bytesRead)) > 0) {			
			CountDownLatch newLatch = new CountDownLatch(1);
			if(read == CHUNK_SIZE)
				executor.submit(new GzipRunnable(bytesRead, preLatch, newLatch));
			else
				executor.submit(new GzipRunnable(Arrays.copyOf(bytesRead, read), preLatch, newLatch));

			preLatch = newLatch;
			bytesRead = new byte[CHUNK_SIZE];
		}

		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		resultBuffer.put(new byte[0]);

		writer.join();
		
		os.flush();
		os.close();
		
		long end = System.currentTimeMillis();
		
		System.out.println(end - start);
	}

	static class GzipThreadPoolExecutor extends ThreadPoolExecutor {

		public GzipThreadPoolExecutor(int numThreads) {
			super(numThreads, numThreads, 0, TimeUnit.MILLISECONDS, new AlwaysBlockingRunnableLinkedBlockingQueue(BUFFER_SIZE), new DaemonThreadFactory());
		}

		protected void afterExecute(Runnable r, Throwable t) {
			super.afterExecute(r, t);

			if (t != null) {
				handleException(t);
			}

			/* The executor wraps Runnables into FutureTasks, which captures all exceptions.
			 * So we must look into the Future result to see if there was an exception.
			 * Argh, this is is messy!
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
			System.err.println(t);
			t.printStackTrace(System.err);

			System.exit(1);
		}
	}

	static class GzipRunnable implements Runnable {
		byte[] rawBytes;
		CountDownLatch previousLatch;
		CountDownLatch myLatch;

		GZIPOutputStream gzipOut;
		ByteArrayOutputStream byteOut;

		public GzipRunnable(byte[] rawBytes, CountDownLatch previousLatch, CountDownLatch myLatch) throws IOException {
			this.rawBytes = rawBytes;
			this.previousLatch = previousLatch;
			this.myLatch = myLatch;

			byteOut = new ByteArrayOutputStream(CHUNK_SIZE);
			gzipOut = new GZIPOutputStream(byteOut);
		}

		@Override
		public void run() {			
			try{
				gzipOut.write(rawBytes);
				gzipOut.flush();
				gzipOut.close();            	

				byte[] compressedBytes = byteOut.toByteArray();
				byteOut.close();

				//System.out.println("Compressed bytes " + compressedBytes.length);
				
				previousLatch.await();
				//previousLatch = null;
				resultBuffer.put(compressedBytes);
				myLatch.countDown();
			} catch(InterruptedException intExc) {
				Thread.currentThread().interrupt();
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	static class DaemonThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable runnable) {
			Thread thread = new Thread(runnable);
			thread.setDaemon(true);

			return thread;
		}
	}
	
	static class AlwaysBlockingRunnableLinkedBlockingQueue extends LinkedBlockingQueue<Runnable> {
		private static final long serialVersionUID = 8567856831618477719L;

		public AlwaysBlockingRunnableLinkedBlockingQueue(int capacity) {
	        super(capacity);
	    }

	    @Override
	    public boolean offer(Runnable e) {
	        try {
	            put(e);
	        } catch (InterruptedException e1) {
	            return false;
	        }
	        return true;
	    }

	    @Override
	    public boolean add(Runnable e) {
	        try {
	            put(e);
	        } catch (InterruptedException e1) {
	            return false;
	        }
	        return true;
	    }
	}
}
