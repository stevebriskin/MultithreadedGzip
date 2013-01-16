package gzip;

import java.util.concurrent.LinkedBlockingQueue;

class AlwaysBlockingRunnableLinkedBlockingQueue extends LinkedBlockingQueue<Runnable> {
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