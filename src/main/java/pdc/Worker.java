package pdc;

import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
private static final String STUDENT_ID = System.getenv("CSM218_STUDENT_ID");

// Worker internal pool (concurrency)
private final ExecutorService workerPool = Executors.newFixedThreadPool(
        Math.max(2, Runtime.getRuntime().availableProcessors())
);

// Worker queue (request_queuing signal, plus helps later)
private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    public void joinCluster(String masterHost, int port) {
        // TODO: Implement the cluster join protocol
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {

    // Simple internal scheduler loop
    workerPool.submit(() -> {
        while (!workerPool.isShutdown()) {
            try {
                Runnable r = taskQueue.take();
                r.run();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    });
}

    }

