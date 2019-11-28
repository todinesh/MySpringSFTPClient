package com.dishatech.myspringsftp.client;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.file.FileHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

@Component
public class FileTransferService {
	@Value("${sftp.bufferSize:1024}")
	private int bufferSize;
	private final static Long MILLS_IN_DAY = 86400000L;
	public void downloadFile(Message<?> message) {
		String fileName = message.getHeaders().get(FileHeaders.REMOTE_FILE).toString();
		System.out.println("Downloading file: " + fileName);
		if (fileName.endsWith("zip")||fileName.endsWith("ZIP")) processZipFileAsync(new ZipInputStream(new ByteArrayInputStream((byte[]) message.getPayload())));
	}

	private Future<Void> processZipFileAsync(ZipInputStream zipIn) {
		return execute(future -> {
			try {
				processFileSync(zipIn);
				future.complete();
			} catch (IOException e) {
				future.fail("Failed to process file stream" + e);
			}
		}, Vertx.vertx());
	}

	private void processFileSync(ZipInputStream zipIn) throws IOException {
		try {
			ZipEntry entry = zipIn.getNextEntry();
			while (entry != null) {
				System.out.println("Filename: "+entry.getName()+" , Size:"+entry.getSize() +" , Last Modified:" + LocalDate.ofEpochDay(entry.getTime() / MILLS_IN_DAY));
				BufferedReader br = new BufferedReader(new InputStreamReader(zipIn), bufferSize);
				String readLine = "";
				while ((readLine = br.readLine()) != null) {
					System.out.println(readLine);
				}
				zipIn.closeEntry();
				entry = zipIn.getNextEntry();
			}
		} finally {
			zipIn.close();
		}
	}
	
	public <E> Future<E> execute(Handler<Future<E>> op, Vertx vertx) {
		Future<E> future = Future.future();
		vertx.executeBlocking(op, false, ar -> {
			if (ar.succeeded()) {
				future.complete(ar.result());
			} else {
				future.fail(ar.cause());
			}
		});
		return future;
	}
}