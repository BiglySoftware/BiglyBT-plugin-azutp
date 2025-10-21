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
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.biglybt.core.logging.LogEvent;
import com.biglybt.core.logging.LogIDs;
import com.biglybt.core.logging.Logger;
import com.biglybt.core.util.AESemaphore;
import com.biglybt.core.util.CopyOnWriteList;
import com.biglybt.core.util.Debug;
import com.biglybt.core.util.RandomUtils;
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
	
	private final IncomingConnectionManager	incoming_manager = IncomingConnectionManager.getSingleton();

	private UTPSelector		selector;
	
	private final CopyOnWriteList<UTPConnection>		connections 			= new CopyOnWriteList<UTPConnection>();
	private volatile int								connection_count;
	
	private final Map<InetAddress,CopyOnWriteList<UTPConnection>>		address_connection_map 	= new ConcurrentHashMap<InetAddress, CopyOnWriteList<UTPConnection>>();
					
	public static final int	DEFAULT_RECV_BUFFER_KB		= UTPProvider.DEFAULT_RECV_BUFFER_KB;
	public static final int	DEFAULT_SEND_BUFFER_KB		= UTPProvider.DEFAULT_SEND_BUFFER_KB;
		
	private int					current_local_port;
		
	private boolean	available;
		
	private long	packet_received_count;
	private long	packet_sent_count;
	
	private boolean	prefer_utp;
		
	private final int							NUM_PROCESSORS = 3;
	private final UTPConnectionProcessor[]		processors;
	private final UTPProvider[]					providers;
	
	public
	UTPConnectionManager(
		UTPPlugin		_plugin )
	{
		plugin		= _plugin;
		
		processors	= new UTPConnectionProcessor[NUM_PROCESSORS];
		providers	= new UTPProvider[ NUM_PROCESSORS ];
		
		for ( int i=0; i<NUM_PROCESSORS; i++ ){
			
			processors[i]	= new UTPConnectionProcessor( this, i+1 );
			providers[i]	= processors[i].getProvider();
		}
		
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

			long sc = 0;
			
			for ( UTPProvider provider: providers ){
				
				sc += provider.getSocketCount();
			}
			values.put( ST_NET_UTP_SOCKET_COUNT, sc );
		}
	}
	
	public UTPProvider[]
	getProviders()
	{
		return( providers );
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
				
		try{
			available = true;
			
			for ( UTPConnectionProcessor processor: processors ){
			
				available = processor.activate();
				
				if ( !available ){
					
					break;
				}
			}
		
			if ( available ){
							
				selector = new UTPSelector( this );
				
				ProtocolEndpointUTP.register( this );
			}
		}finally{
			
			init_sem.releaseForever();
		}
	}
		
	private UTPConnectionProcessor
	allocateProcessor()
	{
		if ( NUM_PROCESSORS == 1 ){
			
			return( processors[0] );
		}
		
		UTPConnectionProcessor result = null;
		
		int min = Integer.MAX_VALUE;
		
		for ( UTPConnectionProcessor processor: processors ){
			
			int socks = processor.getProvider().getSocketCount();
			
			if ( socks < min ){
				
				min		= socks;
				
				result	= processor;
			}
		}
		
		return( result );
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

		return( allocateProcessor().connect(target, transport));
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

				return( doReceive( allocateProcessor(), local_port, address.getHostAddress(), from.getPort(), data, length ));
										
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
							
						return( doReceive( connection.getProcessor(), local_port, address.getHostAddress(), from.getPort(), data, length ));
							
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
		UTPConnectionProcessor	processor,
		int 					local_port,
		String					from_address,
		int						from_port,
		byte[]					data,
		int						length )
	{
		if ( processor.doReceive(local_port, from_address, from_port, data, length)){
			
			packet_received_count++;
			
			return( true );
			
		}else{
			
			return( false );
		}
	}
	
	protected void
	accept(
		int						local_port,
		InetSocketAddress		remote_address,
		UTPConnectionProcessor	processor,
		UTPSocket				utp_socket,
		long					con_id )
	{		
		final UTPConnection	new_connection = addConnection( remote_address, null, processor, utp_socket, con_id );
		
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
	
	protected UTPConnection
	addConnection(
		InetSocketAddress		remote_address,
		UTPTransportHelper		transport_helper,			// null for incoming
		UTPConnectionProcessor	processor,
		UTPSocket				utp_socket,
		long					con_id )
	{
		List<UTPConnection>	to_destroy = null;
		
		final UTPConnection 	new_connection = new UTPConnection( this, remote_address, transport_helper, processor, utp_socket, con_id );
				
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
		
		selector.wakeup();
		
		return( new_connection );
	}
	
	protected void
	removeConnection(
		UTPConnection		c )
	{	
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
	
	protected int
	poll(
		long			now )
	{	
		for ( UTPConnectionProcessor processor: processors ){
			
			processor.poll( now );
		}
		
		return( connection_count );
	}
			
	protected int
	write(
		UTPConnection	connection,
		ByteBuffer[]	buffers,
		int				start,
		int				len )
	
		throws IOException
	{
		return( connection.getProcessor().write(connection, buffers, start, len));
	}
	
	protected void
	inputIdle()
	{
		for ( UTPConnectionProcessor processor: processors ){
			
			processor.inputIdle();
		}
	}
	
	protected void
	readBufferDrained(
		UTPConnection		connection )
	{
		connection.getProcessor().readBufferDrained( connection );
	}
	
	protected void
	close(
		UTPConnection	connection,
		String			reason,
		int				close_reason )
	{
		connection.getProcessor().close(connection, reason, close_reason);
	}
	
	protected void
	dispatchSend(
		InetSocketAddress	address,
		byte[]				buffer,
		int					length )
	{
		packet_sent_count++;

		plugin.send( current_local_port, address, buffer, length );
	}
	
	/*
	LinkedBlockingQueue<AERunnable> send_queue = new LinkedBlockingQueue<>();
	
	{
		for ( int i=0;i<8;i++){
			AEThread2 send_thread = 
					AEThread2.createAndStartDaemon2( 
						"uTP:CM", 
						()->{
							while( true ){
								try{
									AERunnable task = send_queue.take();
									
									if ( task != null ){
										
										task.runSupport();
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
			
			
				send_thread.setPriority( Thread.MAX_PRIORITY - 1 );
		}
	}
	*/
	
	/*
	List<Object[]>	pending_sends = new ArrayList<>();
	
	private void
	dispatchSend(
		InetSocketAddress	address,
		byte[]				buffer,
		int					length )
	{
		// send operations can take a while so we want to get them off the main dispatch
		// thread to avoid blocking other operations such as packet reception
	
		if ( length != 0 ){
			if ( length ==  0 ){
				plugin.send( current_local_port, address, buffer, length );
			
			}else{
				//System.out.println( send_queue.size());
				send_queue.add( 
					new AERunnable()
					{
						public void
						runSupport()
						{
							plugin.send( current_local_port, address, buffer, length );
						}
					});
			}
		}else{
			pending_sends.add( new Object[]{ address, buffer, length });
		}
	}
	
	private void
	dispatchSends()
	{
		try{
	
				// send operations can take a while so we want to get them off the main dispatch
				// thread to avoid blocking other operations such as packet reception
			
			if ( pending_sends ==  null ){
				
				for ( Object[] entry: pending_sends ){
					plugin.send( current_local_port, (InetSocketAddress)entry[0], (byte[])entry[1], (int)entry[2] );
				}
				
			}else{
				
				List<Object[]> ps = new ArrayList<>( pending_sends );
				
				send_queue.add( 
					new AERunnable()
					{
						public void
						runSupport()
						{
							for ( Object[] entry: ps ){
								plugin.send( current_local_port, (InetSocketAddress)entry[0], (byte[])entry[1], (int)entry[2] );
							}
						}
					});
			}
		}finally{
			
			pending_sends.clear();
		}
	}
	*/
	
	/*
	 * 	private void
	dispatchSend(
		InetSocketAddress	address,
		byte[]				buffer,
		int					length )
	{
			// send operations can take a while so we want to get them off the main dispatch
			// thread to avoid blocking other operations such as packet reception
		
		if ( length ==  0 ){
			plugin.send( current_local_port, address, buffer, length );
		
		}else{
			System.out.println( send_queue.size());
			send_queue.add( 
				new AERunnable()
				{
					public void
					runSupport()
					{
						plugin.send( current_local_port, address, buffer, length );
					}
				});
		}
	}
	 */
	
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
		for ( UTPConnectionProcessor processor: processors ){
			
			processor.setReceiveBufferSize(size);
		}
	}
	
	public void
	setSendBufferSize(
		int		size )
	{
		for ( UTPConnectionProcessor processor: processors ){
		
			processor.setSendBufferSize(size);
		}
	}
	
	protected void
	log(
		String		str )
	{
		plugin.log( str );
	}
	
	protected void
	log(
		String		str,
		Throwable	error )
	{
		plugin.log(str,error);
	}
}