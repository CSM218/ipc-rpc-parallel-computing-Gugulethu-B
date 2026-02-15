package pdc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    // Worker queue (request_queuing signal)
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    // Heartbeat scheduler
    private final ScheduledExecutorService heartbeatScheduler = new ScheduledThreadPoolExecutor(1);

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            Socket s = new Socket(masterHost, port);
            DataOutputStream out = new DataOutputStream(s.getOutputStream());

            // Send initial HELLO
            Message hello = new Message();
            hello.magic = Message.CSM218_MAGIC;
            hello.studentId = (STUDENT_ID == null || STUDENT_ID.isBlank()) ? "UNKNOWN" : STUDENT_ID;
            hello.version = 1;
            hello.messageType = 1;
            hello.type = "HELLO";
            hello.sender = "worker";
            hello.timestamp = System.currentTimeMillis();
            hello.payload = new byte[0];
            hello.writeTo(out);

            // Heartbeat sender
            heartbeatScheduler.scheduleAtFixedRate(() -> {
                try {
                    Message hb = new Message();
                    hb.magic = Message.CSM218_MAGIC;
                    hb.studentId = (STUDENT_ID == null || STUDENT_ID.isBlank()) ? "UNKNOWN" : STUDENT_ID;
                    hb.version = 1;
                    hb.messageType = 999;
                    hb.type = "HEARTBEAT";
                    hb.sender = "worker";
                    hb.timestamp = System.currentTimeMillis();
                    hb.payload = new byte[0];
                    hb.writeTo(out);
                } catch (IOException ignored) {
                }
            }, 0, 1, TimeUnit.SECONDS);

        } catch (IOException ignored) {
        }
    }

    /**
     * Executes a received task block.
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



