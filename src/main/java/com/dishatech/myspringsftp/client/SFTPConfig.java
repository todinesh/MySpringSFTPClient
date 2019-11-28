package com.dishatech.myspringsftp.client;

import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.JschProxyFactoryBean;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.transformer.StreamTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.Proxy;

@Configuration
@IntegrationComponentScan
@EnableIntegration
public class SFTPConfig {

	@Value("${sftp.host}")
	private String host;
	@Value("${sftp.port}")
	private int port;
	@Value("${sftp.user}")
	private String user;
	@Value("${sftp.password}")
	private String password;
	@Value("${sftp.folder}")
	private String remoteDir;
	@Value("${sftp.privateKey}")
	private Resource sftpPrivateKey;
	@Value("${sftp.fileFilter}")
	private String sftpRemoteDirectoryDownloadFilter;

	String proxyServer = null;
	
	@Autowired FileTransferService fileTransferService;

	@Bean
	public JschProxyFactoryBean jschProxyFactoryBean() {
		return new JschProxyFactoryBean(JschProxyFactoryBean.Type.HTTP, proxyServer, 80, null, null);
	}

	@Bean
	public Proxy proxy(JschProxyFactoryBean jschProxyFactoryBean) throws Exception {
		return jschProxyFactoryBean.getObject();
	}

	@Bean
	public SessionFactory<LsEntry> sftpSessionFactory(Proxy proxy) {
		DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
		factory.setHost(host);
		factory.setPort(port);
		factory.setUser(user);
		if (proxyServer != null) {
			factory.setProxy(proxy);
		}
		if (sftpPrivateKey != null) {
			factory.setPrivateKey(sftpPrivateKey);
		} else {
			factory.setPassword(password);
		}
		factory.setAllowUnknownKeys(true);
		return new CachingSessionFactory<>(factory);
	}

	@Bean
	public SessionFactory<LsEntry> sftpSessionFactory() {
		DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
		factory.setHost(host);
		factory.setPort(port);
		factory.setUser(user);
		if (sftpPrivateKey != null) {
			factory.setPrivateKey(sftpPrivateKey);
		} else {
			factory.setPassword(password);
		}
		factory.setAllowUnknownKeys(true);
		return new CachingSessionFactory<>(factory);
	}
	
	@Bean
	@InboundChannelAdapter(channel = "stream", poller = @Poller(fixedDelay = "1000", maxMessagesPerPoll = "-1"))
	public MessageSource<InputStream> ftpMessageSource() {
		SftpStreamingMessageSource messageSource = new SftpStreamingMessageSource(template(), null);
		messageSource.setRemoteDirectory(remoteDir);
		messageSource.setFilter(new SftpSimplePatternFileListFilter(sftpRemoteDirectoryDownloadFilter));
		messageSource.setMaxFetchSize(1);
		return messageSource;
	}
	
	@Bean
	public SftpRemoteFileTemplate template() {
		return new SftpRemoteFileTemplate(sftpSessionFactory());
	}
	
	@Bean
    @Transformer(inputChannel = "stream", outputChannel = "dataStream")
    public org.springframework.integration.transformer.Transformer transformer() {
        return new StreamTransformer();
    }

	@Bean
	@ServiceActivator(inputChannel = "dataStream", outputChannel = "download")
	public MessageHandler resultFileHandler() {
		return new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) {
				fileTransferService.downloadFile(message);
			}
		};
	}
}