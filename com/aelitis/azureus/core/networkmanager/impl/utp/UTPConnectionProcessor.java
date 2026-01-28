/*
 * Copyright (C) Bigly Software, Inc, All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

package com.aelitis.azureus.core.networkmanager.impl.utp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.biglybt.core.util.AEDiagnostics;
import com.biglybt.core.util.AERunnable;
import com.biglybt.core.util.AESemaphore;
import com.biglybt.core.util.AEThread2;
import com.biglybt.core.util.ByteFormatter;
import com.biglybt.core.util.Constants;

import com.biglybt.core.util.Debug;
import com.biglybt.core.util.IdentityHashSet;
import com.biglybt.core.util.SystemTime;
import com.vuze.client.plugins.utp.UTPProvider;
import com.vuze.client.plugins.utp.UTPProviderCallback;
import com.vuze.client.plugins.utp.UTPProviderFactory;
import com.vuze.client.plugins.utp.UTPSocket;

public class 
UTPConnectionProcessor
{
	private static final int MIN_MSS	= 256;
	private static final int MAX_HEADER	= 128;
	
	public static final int MIN_WRITE_PAYLOAD		= MIN_MSS - MAX_HEADER;
	public static final int MAX_BUFFERED_PAYLOAD	= 512;

	private static final int CLOSING_TIMOUT			= 15*1000;
	private static final int UTP_PROVIDER_TIMEOUT	= 30*1000;
	
	private boolean		initialised;
		
	private final UTPConnectionManager		manager;
	
	private AEThread2 									dispatch_thread;
	private final LinkedBlockingQueue<DispatchTask>		msg_queue = new LinkedBlockingQueue<>();
	//private final Average dispatch_rate	= Average.getInstance(1000, 10);
	
	private Set<UTPConnection>							closing_connections		= new IdentityHashSet<UTPConnection>();
		
	private static final long	MAX_INCOMING_QUEUED			= 4*1024*1024;
	private static final long	MAX_INCOMING_QUEUED_LOG_OK	= MAX_INCOMING_QUEUED - 256*1024;
	
	public static final int	DEFAULT_RECV_BUFFER_KB		= UTPProvider.DEFAULT_RECV_BUFFER_KB;
	public static final int	DEFAULT_SEND_BUFFER_KB		= UTPProvider.DEFAULT_SEND_BUFFER_KB;
	
	private AtomicLong			total_incoming_queued = new AtomicLong();
	private volatile int		total_incoming_queued_log_state;
		
	private int					current_local_port;
		
	private boolean	available;
			
	private AtomicBoolean		inputIdlePending = new AtomicBoolean( false );

	private UTPProvider	utp_provider = UTPProviderFactory.createProvider();
		
	public
	UTPConnectionProcessor(
		UTPConnectionManager		_manager,
		int							_index )
	{
		manager		= _manager;
		
		dispatch_thread = 
				AEThread2.createAndStartDaemon2( 
					"uTP:CM " + _index, 
					()->{
						while( true ){
							try{
								DispatchTask	target = msg_queue.take();
								
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
		
		dispatch_thread.setPriority( Thread.MAX_PRIORITY );
		
		AEDiagnostics.addEvidenceGenerator(
				(writer)->{
					writer.println( "UTP Connection Processor - " + _index );

					try{
						writer.indent();
						
						writer.println( "closing=" + closing_connections.size());
												
						try{
							writer.indent();

							for ( UTPConnection con: closing_connections ){
								
								writer.println( con.getString());
							}
						}finally{
							
							writer.exdent();
						}
						final AESemaphore sem = new AESemaphore( "uTP:connect" );
						
						dispatch(
								new DispatchTask( "generate" )
								{
									public void
									runTask()
									{
										try{
											utp_provider.generate( writer );
											
										}finally{
											
											sem.release();
										}
									}
								});
						
						sem.reserve();
						
					}finally{
						
						writer.exdent();
					}
				});
	}
	
	public UTPProvider
	getProvider()
	{
		return( utp_provider );
	}
	
	private void
	checkThread()
	{
		if ( !dispatch_thread.isCurrentThread()){
			
			Debug.out( "eh" );
		}
	}
	
	public boolean
	activate()
	{
		synchronized( this){
			
			if ( initialised ){
			
				return( available );
			}
		
			initialised = true;
		}
				
		final AESemaphore	init_sem = new AESemaphore( "uTP:init" );
				
		try{
			available = utp_provider.load( 
					new UTPProviderCallback()
					{					
						public void
						log(
							String		str,
							Throwable	error )
						{
							manager.log(str,error);
						}
						
						@Override
						public void 
						checkThread()
						{
							if ( Constants.IS_CVS_VERSION ){
								UTPConnectionProcessor.this.checkThread();
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
							InetSocketAddress	address,
							UTPSocket			utp_socket,
							long				con_id )
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}
							
							init_sem.reserve();
							
							manager.accept( current_local_port, address, UTPConnectionProcessor.this, utp_socket, con_id );
						}
												
						public void
						send(
							InetSocketAddress	address,
							byte[]				buffer,
							int					length )
						{
							if ( Constants.IS_CVS_VERSION ){
								checkThread();
							}
														
							if ( length != buffer.length ){
								
								Debug.out( "optimise this" );
								
								byte[] temp = new byte[length];
								
								System.arraycopy(buffer, 0, temp, 0, length );
								
								buffer = temp;
							}
							
							manager.dispatchSend( address, buffer, length );
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
										
										manager.removeConnection( connection );
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
			
		}finally{
			
			init_sem.releaseForever();
		}
		
		return( available );
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
				new DispatchTask( "connect" )
				{
					public void
					runTask()
				  	{
						current_local_port = transport.getLocalPort();
						
						try{
							Object[] x = utp_provider.connect( target.getAddress().getHostAddress(), target.getPort());
						
							if ( x != null ){
						
								result[0] = manager.addConnection( target, transport, UTPConnectionProcessor.this, (UTPSocket)x[0], (Long)x[1] );
								
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
	
	protected boolean
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
			new DispatchTask( "receive" )
			{
				public void
				runTask()
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
	
	
	/*
	public long
	getDispatchRate()
	{
		return( dispatch_rate.getAverage());
	}
	*/
	
	protected void
	poll(
		long			now )
	{		
			// called every 500ms or so
		
		dispatch(
			new DispatchTask( "poll" )
			{
				public void
				runTask()
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
									
									manager.removeConnection( c );
									
									log( "Removing " + c.getString() + " due to close timeout" );
								}
							}
							
						}
					}
				}
			});
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
			new DispatchTask( "write" )
			{
				public void
				runTask()
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
	
	protected void
	inputIdle()
	{
		if ( !inputIdlePending.compareAndSet( false, true )){
			
			return;
		}
		
		dispatch( 
			new DispatchTask( "inputIdle" )
			{
				public void
				runTask()
				{
					inputIdlePending.set( false );
					
					try{
						utp_provider.incomingIdle();
							
					}catch( Throwable e ){
						
						Debug.out( e );
					}
				}
			});
	}
	
	protected void
	readBufferDrained(
		final UTPConnection		c )
	{
		dispatch(
			new DispatchTask( "readDrained")
			{
				public void
				runTask()
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
			new DispatchTask( "close" )
			{
				public void
				runTask()
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
							
							manager.removeConnection( c );
						}
					}
				}
			});
	}
	
	private final void
	dispatch(
		DispatchTask	target )
	{
		try{
			msg_queue.put(target);
			
		}catch( Throwable e ){
			
			Debug.out( "Failed to enqueue task", e );
		}
	}
	
	public void
	setReceiveBufferSize(
		int		size )
	{
		dispatch(
			new DispatchTask( "setReceiveBufferSize" )
			{
				public void
				runTask()
				{
					utp_provider.setOption( UTPProvider.OPT_RECEIVE_BUFFER, size==0?DEFAULT_RECV_BUFFER_KB:size );
				}
			});
	}
	
	public void
	setSendBufferSize(
		int		size )
	{
		dispatch(
			new DispatchTask( "setSendBufferSize" )
			{
				public void
				runTask()
				{
					utp_provider.setOption( UTPProvider.OPT_SEND_BUFFER, size==0?DEFAULT_SEND_BUFFER_KB:size );
				}
			});
	}
	
	public void
	setUDPMTUDefault(
		int		size )
	{
		dispatch(
			new DispatchTask( "setUDPMTUDefault" )
			{
				public void
				runTask()
				{
					utp_provider.setOption( UTPProvider.OPT_UDP_MTU_DEFAULT, size );
				}
			});
	}
	
	protected void
	log(
		String		str )
	{
		manager.log( str );
	}
	
	long dispatch_id = 0;
	
	abstract class
	DispatchTask
		extends AERunnable
	{
		final String	name;
		
		// final long queued = SystemTime.getHighPrecisionCounter();
		
		DispatchTask(
			String 	_name )
		{
			name	= _name;
		}
		
		public abstract void
		runTask();
		
		public void
		runSupport()
		{
			try{
				/*
				long start = SystemTime.getHighPrecisionCounter();
				
				long delay = start - queued;
				
				if ( delay > 1000000 ){
					
					System.out.println( name + ": delay: " + delay );
				}
				*/
				runTask();
				/*
				long end = SystemTime.getHighPrecisionCounter();
				
				long elapsed = end - start;
						
				if ( elapsed > 1000000 ){
					
					System.out.println( name + ": took " + elapsed );
				}
				*/
			}finally{
				
				//dispatchSends();
			}
		}
	}
}
