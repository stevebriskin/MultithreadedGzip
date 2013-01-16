package gzip;

import java.util.concurrent.ThreadFactory;

class DaemonThreadFactory implements ThreadFactory {
	@Override
	public Thread newThread(Runnable runnable) {
		Thread thread = new Thread(runnable);
		thread.setDaemon(true);

		return thread;
	}
}