/*
 * Created on Aug 28, 2010
 * Created by Paul Gardner
 * 
 * Copyright 2010 Vuze, Inc.  All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details ( see the LICENSE file ).
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */



package com.aelitis.azureus.core.networkmanager.impl.utp;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.biglybt.core.logging.LogEvent;
import com.biglybt.core.logging.LogIDs;
import com.biglybt.core.logging.Logger;
import com.biglybt.core.util.AERunnable;
import com.biglybt.core.util.AESemaphore;
import com.biglybt.core.util.AEThread2;
import com.biglybt.core.util.AsyncDispatcher;
import com.biglybt.core.util.Average;
import com.biglybt.core.util.ByteFormatter;
import com.biglybt.core.util.Constants;
import com.biglybt.core.util.CopyOnWriteList;
import com.biglybt.core.util.Debug;
import com.biglybt.core.util.IdentityHashSet;
import com.biglybt.core.util.SystemTime;
import com.biglybt.pif.PluginInterface;

import com.biglybt.core.networkmanager.ConnectionEndpoint;
import com.biglybt.core.networkmanager.ProtocolEndpoint;
import com.biglybt.core.networkmanager.ProtocolEndpointFactory;
import com.biglybt.core.networkmanager.impl.IncomingConnectionManager;
import com.biglybt.core.networkmanager.impl.ProtocolDecoder;
import com.biglybt.core.networkmanager.impl.TransportCryptoManager;
import com.biglybt.core.networkmanager.impl.TransportHelperFilter;
import com.biglybt.core.stats.CoreStats;
import com.biglybt.core.stats.CoreStatsProvider;
import com.vuze.client.plugins.utp.UTPPlugin;
import com.vuze.client.plugins.utp.UTPProvider;
import com.vuze.client.plugins.utp.UTPProviderCallback;
import com.vuze.client.plugins.utp.UTPProviderFactory;
import com.vuze.client.plugins.utp.UTPSocket;


public class 
UTPConnectionManager
	implements CoreStatsProvider
{
	private static final int MIN_MSS	= 256;
	private static final int MAX_HEADER	= 128;
	
	public static final int MIN_WRITE_PAYLOAD		= MIN_MSS - MAX_HEADER;
	public static final int MAX_BUFFERED_PAYLOAD	= 512;

	private static final int CLOSING_TIMOUT			= 15*1000;
	private static final int UTP_PROVIDER_TIMEOUT	= 30*1000;
	
	public static final String ST_NET_UTP_PACKET_SENT_COUNT			= "net.utp.packet.sent.count";
	public static final String ST_NET_UTP_PACKET_RECEIVED_COUNT		= "net.utp.packet.received.count";
	public static final String ST_NET_UTP_CONNECTION_COUNT			= "net.utp.connection.count";
	public static final String ST_NET_UTP_SOCKET_COUNT				= "net.utp.socket.count";

	private static final String[][] ST_ALL = {
			{ ST_NET_UTP_PACKET_SENT_COUNT,			CoreStats.CUMULATIVE },
			{ ST_NET_UTP_PACKET_RECEIVED_COUNT,		CoreStats.CUMULATIVE },
			{ ST_NET_UTP_CONNECTION_COUNT,			CoreStats.POINT },
			{ ST_NET_UTP_SOCKET_COUNT,				CoreStats.POINT },
	};
	
	static{
		CoreStats.addStatsDefinitions( ST_ALL );
	}
	
	private static final LogIDs LOGID = LogIDs.NET;
	
	private boolean		initialised;
	
	private UTPPlugin				plugin;
	
	private IncomingConnectionManager	incoming_manager = IncomingConnectionManager.getSingleton();

	private AEThread2 									dispatch_thread;
	private final LinkedBlockingQueue<AERunnable>		msg_queue = new LinkedBlockingQueue<>();
	//private final Average dispatch_rate	= Average.getInstance(1000, 10);

	private UTPSelector		selector;
	
	private CopyOnWriteList<UTPConnection>				connections 			= new CopyOnWriteList<UTPConnection>();
	private volatile int								connection_count;
	
	private Map<InetAddress,CopyOnWriteList<UTPConnection>>		address_connection_map 	= new ConcurrentHashMap<InetAddress, CopyOnWriteList<UTPConnection>>();
	
	//private Map<Long,UTPConnection>								socket_connection_map 	= new HashMap<Long, UTPConnection>();
	
	private Set<UTPConnection>							closing_connections		= new IdentityHashSet<UTPConnection>();
		
	private static final long	MAX_INCOMING_QUEUED			= 4*1024*1024;
	private static final long	MAX_INCOMING_QUEUED_LOG_OK	= MAX_INCOMING_QUEUED - 256*1024;
	
	public static final int	DEFAULT_RECV_BUFFER_KB		= UTPProvider.DEFAULT_RECV_BUFFER_KB;
	public static final int	DEFAULT_SEND_BUFFER_KB		= UTPProvider.DEFAULT_SEND_BUFFER_KB;
	
	private AtomicLong			total_incoming_queued = new AtomicLong();
	private volatile int		total_incoming_queued_log_state;
	
	private volatile long		packet_sent_count;
	private volatile long		packet_received_count;
	
	private int					current_local_port;
	
	private boolean	available;
		
	private boolean	prefer_utp;
	
	private UTPProvider	utp_provider = UTPProviderFactory.createProvider();
	
	private volatile AESemaphore poll_waiter;
	
	public
	UTPConnectionManager(
		UTPPlugin		_plugin )
	{
		plugin		= _plugin;
		
		dispatch_thread = 
				AEThread2.createAndStartDaemon2( 
					"uTP:CM", 
					()->{
						while( true ){
							try{
								AERunnable	target = msg_queue.take();
								
								// dispatch_rate.addValue(1);
								
								try{
									target.runSupport();
									
								}catch( Throwable e ){
									
									Debug.out( e );
								}
								
							}catch( Throwable e ){
								
								try{
									Thread.sleep( 1000 );
									
								}catch( Throwable f ){
									
									Debug.out( e );
									
									break;
								}
							}
						}
					});
		
		dispatch_thread.setPriority( Thread.MAX_PRIORITY - 1 );
		
		Set	types = new HashSet();
		
		types.add( ST_NET_UTP_PACKET_SENT_COUNT );
		types.add( ST_NET_UTP_PACKET_RECEIVED_COUNT );
		
		types.add( ST_NET_UTP_CONNECTION_COUNT );
		types.add( ST_NET_UTP_SOCKET_COUNT );			

		CoreStats.registerProvider( types, this );
	}
	
	@Override
	public void
	updateStats(
		Set		types,
		Map		values )
	{
		if ( types.contains( ST_NET_UTP_PACKET_SENT_COUNT )){

			values.put( ST_NET_UTP_PACKET_SENT_COUNT, new Long( packet_sent_count ));
		}
		if ( types.contains( ST_NET_UTP_PACKET_RECEIVED_COUNT )){

			values.put( ST_NET_UTP_PACKET_RECEIVED_COUNT, new Long( packet_received_count ));
		}	
		if ( types.contains( ST_NET_UTP_CONNECTION_COUNT )){

			values.put( ST_NET_UTP_CONNECTION_COUNT, new Long( connection_count ));
		}
		if ( types.contains( ST_NET_UTP_SOCKET_COUNT )){

			values.put( ST_NET_UTP_SOCKET_COUNT, new Long( utp_provider.getSocketCount()));
		}
	}
	
	public UTPProvider
	getProvider()
	{
		return( utp_provider );
	}
	
	public int
	getProviderVersion()
	{
		return( utp_provider.getVersion());
	}
	
	private void
	checkThread()
	{
		if ( !dispatch_thread.isCurrentThread()){
			
			Debug.out( "eh" );
		}
	}
	
	public void
	activate()
	{
		synchronized( this){
			
			if ( initialised ){
			
				return;
			}
		
			initialised = true;
		}
				
		final AESemaphore	init_sem = new AESemaphore( "uTP:init" );
		
		PluginInterface pi = plugin.getPluginInterface();
		
		final File plugin_user_dir 	= pi.getPluginconfig().getPluginUserFile( "plugin.properties" ).getParentFile();

		File plugin_install_dir	= new File( pi.getPluginDirectoryName());
		
		if ( plugin_install_dir == null || !plugin_install_dir.exists()){
			
			plugin_install_dir = plugin_user_dir;
		}
		
		final File f_plugin_install_dir = plugin_install_dir;
		
		try{
			available = utp_provider.load( 
					new UTPProviderCallback()
					{
						public File
						getPluginUserDir()
						{
							return( plugin_user_dir );
						}
		
						public File
						getPluginInstallDir()
						{
							return( f_plugin_install_dir );
						}
						
						public void
						log(
							String		str,
							Throwable	error )
						{
							plugin.log(str,error);
						}
						
						@Override
						public void 
						checkThread()
						{
							if ( Constants.IS_CVS_VERSION ){
								UTPConnectionManager.this.checkThread();
							}
						}
						
						public int
						getRandom()
						{
							return( UTPUtils.UTP_Random());
						}
						
						public long
						getMilliseconds()
						{
							return( UTPUtils.UTP_GetMilliseconds());
						}
						
						public long
						getMicroseconds()
						{
							return( UTPUtils.UTP_GetMicroseconds());
						}
						
						public void
						incomingConnection(
							String		host,
							int			port,
							UTPSocket	utp_socket,
							long		con_id )
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}
							
							init_sem.reserve();
							
							accept( current_local_port, new InetSocketAddress( host, port),	utp_socket, con_id );
						}
						
						public void
						incomingConnection(
							InetSocketAddress	adress,
							UTPSocket			utp_socket,
							long				con_id )
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}
							
							init_sem.reserve();
							
							accept( current_local_port, adress,	utp_socket, con_id );
						}
												
						public boolean
						send(
							InetSocketAddress	adress,
							byte[]				buffer,
							int					length )
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}
							
							packet_sent_count++;
							
							return( plugin.send( current_local_port, adress, buffer, length ));
						}
						
						public void
						read(
							UTPSocket 	utp_socket,
							ByteBuffer	bb )
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}

							UTPConnection connection = utp_socket.getUTPConnection();
							
							if ( connection == null ){
								
								Debug.out( "read: unknown socket!" );
								
							}else{
								
								try{
									connection.receive( bb );
									
								}catch( Throwable e ){
																	
									connection.close( Debug.getNestedExceptionMessage(e));
								}
							}
						}
						
						public int
						getReadBufferSize(
							UTPSocket 	utp_socket )
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}

							UTPConnection connection = utp_socket.getUTPConnection();
							
							if ( connection == null ){
								
									// can get this during socket shutdown
								
								return( 0 );
								
							}else{
								
								int res = connection.getReceivePendingSize();
									
									// we lie here if we have a fair bit queued as this allows
									// us to control the receive window
								
								if ( res > 512*1024 ){
									
										// forces us to advertize a window of 0 bytes
										// to prevent peer from sending us more data until
										// we've managed to flush this to disk
									
									res = Integer.MAX_VALUE;
								}
								
								return( res );
							}
						}
						
						public void
						setState(
							UTPSocket 	utp_socket,
							int			state )
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}

							UTPConnection connection = utp_socket.getUTPConnection();
							
							if ( connection == null ){
								
									// can get this during socket shutdown
								
							}else{
																
								if ( state == STATE_CONNECT ){
									
									connection.setConnected();
								}
								
								if ( state == STATE_CONNECT || state == STATE_WRITABLE ){
								
									connection.setCanWrite( true );
									
								}else if ( state == STATE_EOF ){
									
									connection.close( "EOF" );
									
								}else if ( state == STATE_DESTROYING ){
									
									connection.setUnusable();
									
									connection.close( "Connection destroyed" );
																		
									if ( closing_connections.remove( connection )){
										
										removeConnection( connection );
									}
								}
							}
						}
						
						@Override
						public void 
						setCloseReason(
							UTPSocket 	utp_socket, 
							int 		reason)
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}

							UTPConnection connection = utp_socket.getUTPConnection();
							
							if ( connection != null ){
							
								connection.setCloseReason( reason );
							}
						}
						
						public void
						error(
							UTPSocket 	utp_socket,
							int			error )
						{	
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}

							UTPConnection connection = utp_socket.getUTPConnection();
							
							if ( connection == null ){
								
								// can get this during socket shutdown
								
							}else{
								
								connection.close( "Socket error: code=" + error );
							}
						}
						
						public void
						overhead(
							UTPSocket 	utp_socket,
							boolean		send,
							int			size,
							int			type )
						{
							//System.out.println( "overhead( " + send + "," + size + "," + type + " )" );
						}
					});
			
			if ( available ){
							
				selector = new UTPSelector( this );
				
				ProtocolEndpointUTP.register( this );
			}
		}finally{
			
			init_sem.releaseForever();
		}
	}
		
	public UTPConnection
	connect(
		final InetSocketAddress		target,
		final UTPTransportHelper	transport )
	
		throws IOException
	{
		if ( target.isUnresolved()){
			
			throw( new UnknownHostException( target.getHostName()));
		}
		
		final Object[] result = { null };
	
		final AESemaphore sem = new AESemaphore( "uTP:connect" );
		
		dispatch(
				new AERunnable()
				{
					public void
					runSupport()
				  	{
						current_local_port = transport.getLocalPort();
						
						try{
							Object[] x = utp_provider.connect( target.getAddress().getHostAddress(), target.getPort());
						
							if ( x != null ){
						
								result[0] = addConnection( target, transport, (UTPSocket)x[0], (Long)x[1] );
								
							}else{
								
								result[0] = new IOException( "Connect failed" );
							}
						}catch( Throwable e ){
							
							e.printStackTrace();
							
							result[0] = new IOException( "Connect failed: " + Debug.getNestedExceptionMessage(e));
							
						}finally{
							
							sem.release();
						}
				  	}
				});
		
		if ( !sem.reserve( UTP_PROVIDER_TIMEOUT )){
			
			Debug.out( "Deadlock probably detected" );
			
			throw( new IOException( "Deadlock" ));
		}
		
		if ( result[0] instanceof UTPConnection ){
			
			return((UTPConnection)result[0]);
			
		}else{
			
			throw((IOException)result[0]);
		}
	}
	
	public boolean
	receive(
		int						local_port,
		InetSocketAddress		from,
		byte[]					data,
		int						length )
	{	
		if ( !available ){
			
			return( false );
		}
		
		InetAddress address = from.getAddress();
					
		if ( length >= 20 ){
			
			byte first_byte = data[0];

			// System.out.println( "UDP: " + ByteFormatter.encodeString( data, 0, length ) + " from " + from  + " - " + new String( data, 0, length ));

			if ( 	first_byte == 0x41 &&		// SYN + version 1 
					data[8] == 0 && data[9] == 0 && data[10] == 0 && data[11] == 0 &&	// time diff = 0 
					// data[16] == 0 && data[17] == 1 ){	// seq = 1
					data[18] == 0 && data[19] == 0 ){	// ack = 0
				
				/* 4102C5F60499238B00000000003800000001000000080000000000000000
					4102 CDF2 	// SYN, ver 1, ext 2, con id CDF2
					6A39693A	// usec
					00000000	// rep micro
					00380000	wnd = 3.5MB
					00010000	seq = 1, ack = 0
	
					00080000	ext len = 8, no more ext
					00000000
					0000
				*/

					// then modified to use random initial sequence number
				
				// 4102e5331fb2e61900000000003800003aee000000080000000000000000

								
				// System.out.println( "Looks like uTP incoming connection from " + from );

				return( doReceive( local_port, address.getHostAddress(), from.getPort(), data, length ));
										
			}else if ( (first_byte&0x0f)==0x01 ){
				
				/* 0100B5621AE099301AD4C472003800000002482213426974546F7272656E742070726F746F636F6C0000000000100005A
					0100		// (x+1) + ext type
					B562		// con id
					1AE09930	// usec
					1AD4C472	// rep micro
					00380000	// recv win bytes
					0002		// seq
					4822		// ack
					13426974546F7272656E742070726F746F636F6C0000000000100005A
				*/
				
				// 210063CB1EFC51C01BA91F010003200036B56BFD
				
				int type = (data[0]>>>4)&0x0f;
				
				if ( type >= 0 && type <= 4 ){
					
					int	con_id = ((data[2]<<8)&0xff00) | (data[3]&0x00ff);
				
					UTPConnection connection = null;
											
					CopyOnWriteList<UTPConnection> l = address_connection_map.get( address );
					
					if ( l != null ){
						
						for ( UTPConnection c:l ){
							
							if ( c.getConnectionID() == con_id ){
								
								connection = c;
								
								break;
							}
						}
					}
					
					/*
					if ( connection == null ){
						
						String existing = "";
						
						for ( Map.Entry<InetAddress, List<UTPConnection>> entry: address_connection_map.entrySet()){
							
							String str = entry.getKey() + "->";
							
							for (UTPConnection u: entry.getValue()){
								
								str += u.getConnectionID() + ",";
							}
							
							existing += str + " ";
						}
						
						System.out.println( "Connection not found for " + from + "/" + con_id + ": " + existing );
					}
					*/
					
					if ( connection != null ){
						
						// System.out.println( "Looks like uTP incoming data from " + from );
							
						return( doReceive( local_port, address.getHostAddress(), from.getPort(), data, length ));
							
					}else{
						
						// System.out.println( "No match from " + from  + ": " + ByteFormatter.encodeString( data, 0, length ));
					}
				}
			}
		}
		
		return( false );
	}
	
	private boolean
	doReceive(
		final int 			local_port,
		final String		from_address,
		final int			from_port,
		final byte[]		data,
		final int			length )
	{
		if ( !utp_provider.isValidPacket( data, length )){
			
			return( false );
		}
			
		packet_received_count++;
		
		if ( total_incoming_queued.get() > MAX_INCOMING_QUEUED ){
			
			if ( total_incoming_queued_log_state == 0 ){
				
				Debug.out( "uTP pending packet queue too large, discarding..." );
				
				total_incoming_queued_log_state = 1;
			}
			
			return( true );
		}
		
		if ( total_incoming_queued_log_state == 1 ){
			
			if ( total_incoming_queued.get() < MAX_INCOMING_QUEUED_LOG_OK ){

				Debug.out( "uTP pending packet queue emptied, processing..." );
			
				total_incoming_queued_log_state	= 0;
			}
		}
			
		total_incoming_queued.addAndGet( length );
		
		dispatch(
			new AERunnable()
			{
				public void
				runSupport()
			  	{
					current_local_port = local_port;
											
					total_incoming_queued.addAndGet( -length );
					
					//System.out.println( "recv " + from_address + ":" + from_port + " - " + ByteFormatter.encodeString( data, 0, length ));
					
					try{
						if ( !utp_provider.receive( from_address, from_port, data, length )){
							
							if ( Constants.IS_CVS_VERSION ){
							
								Debug.out( "Failed to process uTP packet: " + ByteFormatter.encodeString( data, 0, length ) + " from " + from_address);
							}
						}
					}catch( Throwable e ){
						
						Debug.out( e );
					}
				}
			});
		
		return( true );
	}
	
	private void
	accept(
		int						local_port,
		final InetSocketAddress	remote_address,
		UTPSocket				utp_socket,
		long					con_id )
	{		
		final UTPConnection	new_connection = addConnection( remote_address, null, utp_socket, con_id );
		
		final UTPTransportHelper	helper = new UTPTransportHelper( this, local_port, remote_address, new_connection );

		if ( !UTPNetworkManager.UTP_INCOMING_ENABLED ){
			
			helper.close( "Incoming uTP connections disabled" );
			
			return;
		}
		
		log( "Incoming connection from " + remote_address );

		try{
			new_connection.setTransport( helper );
			
			TransportCryptoManager.getSingleton().manageCrypto( 
				helper, 
				null, 
				true, 
				null,
				new TransportCryptoManager.HandshakeListener() 
				{
					public void 
					handshakeSuccess( 
						ProtocolDecoder	decoder,
						ByteBuffer		remaining_initial_data ) 
					{
						TransportHelperFilter	filter = decoder.getFilter();
						
						ConnectionEndpoint	co_ep = new ConnectionEndpoint( remote_address);
	
						ProtocolEndpointUTP	pe_utp = (ProtocolEndpointUTP)ProtocolEndpointFactory.createEndpoint( ProtocolEndpoint.PROTOCOL_UTP, co_ep, remote_address );
	
						UTPTransport transport = new UTPTransport( UTPConnectionManager.this, pe_utp, filter );
								
						helper.setTransport( transport );
						
						incoming_manager.addConnection( local_port, filter, transport );
						
						log( "Connection established to " + helper.getAddress());
	        		}
	
					public void 
					handshakeFailure( 
	            		Throwable failure_msg ) 
					{
						if (Logger.isEnabled()){
							Logger.log(new LogEvent(LOGID, "incoming crypto handshake failure: " + Debug.getNestedExceptionMessage( failure_msg )));
						}
	 
						log( "Failed to established connection to " + helper.getAddress() + ": " + Debug.getNestedExceptionMessage(failure_msg) );
						
						new_connection.close( "handshake failure: " + Debug.getNestedExceptionMessage(failure_msg));
					}
	            
					public void
					gotSecret(
						byte[]				session_secret )
					{
					}
					
					public int
					getMaximumPlainHeaderLength()
					{
						return( incoming_manager.getMaxMinMatchBufferSize());
					}
	    		
					public int
					matchPlainHeader(
						ByteBuffer			buffer )
					{
						Object[]	match_data = incoming_manager.checkForMatch( helper, local_port, buffer, true );

						if ( match_data == null ){

							return( TransportCryptoManager.HandshakeListener.MATCH_NONE );

						}else{

							IncomingConnectionManager.MatchListener match = (IncomingConnectionManager.MatchListener)match_data[0];

							if ( match.autoCryptoFallback()){

								return( TransportCryptoManager.HandshakeListener.MATCH_CRYPTO_AUTO_FALLBACK );

							}else{

								return( TransportCryptoManager.HandshakeListener.MATCH_CRYPTO_NO_AUTO_FALLBACK );

							}
						}
					}
	        	});
			
		}catch( Throwable e ){
			
			Debug.printStackTrace( e );
			
			helper.close( Debug.getNestedExceptionMessage(e));
		}
	}
	
	private UTPConnection
	addConnection(
		InetSocketAddress		remote_address,
		UTPTransportHelper		transport_helper,			// null for incoming
		UTPSocket				utp_socket,
		long					con_id )
	{
		List<UTPConnection>	to_destroy = null;
		
		final UTPConnection 	new_connection = new UTPConnection( this, remote_address, transport_helper, utp_socket, con_id );
		  
		if ( Constants.IS_CVS_VERSION ){
			checkThread();
		}
				
		CopyOnWriteList<UTPConnection> l = address_connection_map.get( remote_address.getAddress());
			
		if ( l != null ){
			
			for ( UTPConnection c: l ){
				
				if ( c.getConnectionID() == con_id ){
					
					if ( to_destroy == null ){
						
						to_destroy = new ArrayList<UTPConnection>();
					}
					
					to_destroy.add( c );
					
					l.remove( c );
					
					connections.remove( c );
					
					connection_count = connections.size();
					
					break;
				}
			}
		}else{
			
			l = new CopyOnWriteList<UTPConnection>();
			
			address_connection_map.put( remote_address.getAddress(), l );
		}
		
		l.add( new_connection );
		
		connections.add( new_connection );
		
		connection_count = connections.size();
		
		UTPConnection existing = utp_socket.getUTPConnection();
		
		utp_socket.setUTPConnection( new_connection );
		
		// System.out.println( "Add connection: " + remote_address + ": total=" + connections.size() + "/" + address_connection_map.size() + "/" + utp_provider.getSocketCount());

		if ( existing != null ){
			
			Debug.out( "Existing socket found at same address!!!!" );
			
			if ( to_destroy == null ){
				
				to_destroy = new ArrayList<UTPConnection>();
			}
			
			to_destroy.add( existing );
		}
		
		if ( to_destroy != null ){
			
			for ( UTPConnection c: to_destroy ){
			
				c.close( "Connection replaced" );
			}
		}
		
		AESemaphore sem = poll_waiter;
		
		if ( sem != null ){
			
			poll_waiter = null;
			
			sem.release();
		}
		
		return( new_connection );
	}
	
	private void
	removeConnection(
		UTPConnection		c )
	{	
		if ( Constants.IS_CVS_VERSION ){
			checkThread();
		}
		
		if ( connections.remove( c )){

			connection_count = connections.size();
			
			InetAddress address = c.getRemoteAddress().getAddress();
			
			CopyOnWriteList<UTPConnection> l = address_connection_map.get( address );
			
			if ( l != null ){
				
				l.remove( c );
				
				if ( l.size() == 0 ){
					
					address_connection_map.remove( address );
				}
			}
			
			// System.out.println( "Remove connection: "+ c.getSocket().getID() + "/" + c.getRemoteAddress() + ": total=" + connections.size() + "/" + address_connection_map.size() + "/" + utp_provider.getSocketCount());
		}
	}
	
	public List<UTPConnection>
	getConnections()
	{
		return( connections.getList());
	}
	
	public UTPSelector
	getSelector()
	{
		return( selector );
	}
	
	/*
	public long
	getDispatchRate()
	{
		return( dispatch_rate.getAverage());
	}
	*/
	
	protected int
	poll(
		AESemaphore		wait_sem,
		long			now )
	{		
			// called every 500ms or so
		
		dispatch(
			new AERunnable()
			{
				public void
				runSupport()
				{
					//System.out.println( "poll");
					utp_provider.checkTimeouts();
					
					//System.out.println("UTPProvider socket count=" +  utp_provider.getSocketCount());
					
					if ( closing_connections.size() > 0 ){
						
						long	now = SystemTime.getMonotonousTime();
						
						Iterator<UTPConnection> it = closing_connections.iterator();
						
						while( it.hasNext()){
						
							UTPConnection c = it.next();
							
							long 	close_time = c.getCloseTime();
							
							if ( close_time > 0 ){
								
								if ( now - close_time > CLOSING_TIMOUT ){
									
									it.remove();
									
									removeConnection( c );
									
									log( "Removing " + c.getString() + " due to close timeout" );
								}
							}
							
						}
					}
				}
			});
		
		int result =  connection_count;
		
		if ( result == 0 ){
			
			poll_waiter = wait_sem;
		}
		
		return( result );
	}
			
	protected int
	write(
		final UTPConnection		c,
		final ByteBuffer[]		buffers,
		final int				start,
		final int				len )
	
		throws IOException
	{
		final AESemaphore sem = new AESemaphore( "uTP:write" );
		
		final Object[] result = {null};
		
		dispatch(
			new AERunnable()
			{
				public void
				runSupport()
				{
					boolean	log_error = true;

					try{						
						if ( c.isUnusable()){

							log_error = false;
							
							throw( new Exception( "Connection is closed" ));
							
						}else if ( !c.isConnected()){

							log_error = false;
							
							throw( new Exception( "Connection is closed" ));
								
						}else if ( !c.canWrite()){
							
							Debug.out( "Write operation on non-writable connection" );
							
							result[0] = 0;						
							
						}else{
								
							int	pre_total = 0;
							
							for (int i=start;i<start+len;i++){
								
								pre_total += buffers[i].remaining();
							}
																
							boolean still_writable = utp_provider.write( c.getSocket(), buffers, start, len );
							
							c.setCanWrite( still_writable );
							
							int	post_total = 0;
							
							for (int i=start;i<start+len;i++){
								
								post_total += buffers[i].remaining();
							}
							
							result[0] = pre_total - post_total;
						}
					}catch( Throwable e ){
						
						if ( log_error ){
						
							Debug.out( e );
						}
						
						c.close( Debug.getNestedExceptionMessage(e));
						
						result[0] = new IOException( "Write failed: " + Debug.getNestedExceptionMessage(e));
						
					}finally{
					
						sem.release();
					}
				}
			});
		
		if ( !sem.reserve( UTP_PROVIDER_TIMEOUT )){
			
			Debug.out( "Deadlock probably detected" );
			
			throw( new IOException( "Deadlock" ));
		}
		
		if ( result[0] instanceof Integer ){
			
			return((Integer)result[0]);
		}
		
		throw((IOException)result[0]);
	}
	
	private AERunnable inputIdleDispatcher =
		new AERunnable()
		{
			public void
			runSupport()
			{
				try{
					utp_provider.incomingIdle();
						
				}catch( Throwable e ){
					
					Debug.out( e );
				}
			}
		};
		
	protected void
	inputIdle()
	{
		dispatch( inputIdleDispatcher );
	}
	
	protected void
	readBufferDrained(
		final UTPConnection		c )
	{
		dispatch(
			new AERunnable()
			{
				public void
				runSupport()
				{
					if ( !c.isUnusable()){
						
						try{
							utp_provider.receiveBufferDrained( c.getSocket());
							
						}catch( Throwable e ){
							
							Debug.out( e );
						}
					}
				}
			});
	}
	
	protected void
	close(
		UTPConnection	c,
		String			r,
		int				close_reason )
	{
		dispatch(
			new AERunnable()
			{
				public void
				runSupport()
				{
					boolean	async_close = false;
		
					try{
						if ( !c.isUnusable()){
							
							log( "Closed connection to " + c.getRemoteAddress() + ": " + r + " (" + c.getState() + ")" );
			
							try{
								c.setUnusable();
			
								utp_provider.close( c.getSocket(), close_reason );
								
									// wait for the destroying callback
								
								async_close = true;
								
							}catch( Throwable e ){
								
								Debug.out( e );
							}
						}
					}finally{
						
						if ( async_close ){
										
							closing_connections.add( c );
						
						}else{
															
							if ( closing_connections.contains( c )){
									
								return;
							}
							
							removeConnection( c );
						}
					}
				}
			});
	}
	
	private final void
	dispatch(
		AERunnable	target )
	{
		try{
			msg_queue.put(target);
			
		}catch( Throwable e ){
			
			Debug.out( "Failed to enqueue task", e );
		}
	}
	public void
	preferUTP(
		boolean		b )
	{
		prefer_utp = b;
	}
	
	protected boolean
	preferUTP()
	{
		return( prefer_utp );
	}
	
	public void
	setReceiveBufferSize(
		int		size )
	{
		utp_provider.setOption( UTPProvider.OPT_RECEIVE_BUFFER, size==0?DEFAULT_RECV_BUFFER_KB:size );
	}
	
	public void
	setSendBufferSize(
		int		size )
	{
		utp_provider.setOption( UTPProvider.OPT_SEND_BUFFER, size==0?DEFAULT_SEND_BUFFER_KB:size );
	}
	
	protected void
	log(
		String		str )
	{
		plugin.log( str );
	}
}
