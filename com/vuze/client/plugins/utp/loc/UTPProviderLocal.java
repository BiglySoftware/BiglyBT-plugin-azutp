/*
 * Created on Jan 23, 2013
 * Created by Paul Gardner
 * 
 * Copyright 2013 Azureus Software, Inc.  All rights reserved.
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



package com.vuze.client.plugins.utp.loc;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.biglybt.core.util.Constants;
import com.biglybt.core.util.Debug;
import com.biglybt.core.util.HostNameToIPResolver;

import com.vuze.client.plugins.utp.UTPProvider;
import com.vuze.client.plugins.utp.UTPProviderCallback;
import com.vuze.client.plugins.utp.UTPProviderException;
import com.vuze.client.plugins.utp.UTPProviderStats;
import com.vuze.client.plugins.utp.UTPSocket;
import com.vuze.client.plugins.utp.loc.v2.UTPTranslatedV2;


public class 
UTPProviderLocal
	implements 
		UTPProvider, 
		UTPProviderStats,
		UTPTranslated.SendToProc, 
		UTPTranslated.UTPGotIncomingConnection, 
		UTPTranslated.UTPFunctionTable
{
	private boolean				test_mode;
	private UTPTranslated		impl;
	private UTPProviderStats	impl_stats;
	
	private UTPProviderCallback		callback;
		
		// all access to this is single threaded by caller
		
	private Map<Integer,Integer>	pending_options = new HashMap<Integer, Integer>();
	
	public 
	UTPProviderLocal()
	{
		this( false );
		
		impl_stats = new UTPProviderStats(){
			
			@Override
			public long getSentPacketCount(){
				return 0;
			}
			
			@Override
			public long getReceivedPacketCount(){
				return 0;
			}
			
			@Override
			public long getPacketReceiveRate(){
				return 0;
			}
			@Override
			public long getPacketSendRate(){
				return 0;
			}
			
			@Override
			public long getDataSendTotal(){
				return 0;
			}
			
			@Override
			public long getDataSendRate(){
				return 0;
			}
			
			@Override
			public long getDataReceiveTotal(){
				return 0;
			}
			
			@Override
			public long getDataReceiveRate(){
				return 0;
			}
			
			@Override
			public long getOverheadRate(){
				return 0;
			}
		};
	}
	
	public 
	UTPProviderLocal(
		boolean	_test_mode )
	{
		test_mode	= _test_mode;
	}
	
	public boolean
	load(
		UTPProviderCallback			_callback )
	{
		synchronized( pending_options ){
			
			if ( impl != null ){
				
				Debug.out( "Already loaded ");
				
				return( false );
			}
			
			callback	= _callback;
			
			impl = new UTPTranslatedV2( callback, this, this, this, test_mode );
			
			impl_stats = impl.getStats();
			
			if ( pending_options.size() > 0 ){
				
				for ( Map.Entry<Integer,Integer> entry: pending_options.entrySet()){
					
					setOption( entry.getKey(), entry.getValue());
				}
				
				pending_options.clear();
			}
			
			return( true );
		}
	}
	
	public int
	getVersion()
	{
		return( 2 );
	}
	
	public boolean
	isValidPacket(
		byte[]		data,
		int			length )
	{
		return( impl.isValidPacket( data, length ));
	}
	
	public int
	getSocketCount()
	{
		return( impl.UTP_GetSocketCount());
	}
	
		// callbacks from implementation
	
	public void 
	send_to_proc(
		Object				user_data,
		byte[]				data,
		InetSocketAddress	addr )
	{
		callback.send( addr, data, data.length );
	}
	
	public void
	got_incoming_connection(
		UTPSocket	socket )
	{
		if ( Constants.IS_CVS_VERSION ){
			callback.checkThread();
		}
					
		// System.out.println( "socket_map: " + socket_map.size());
		
		InetSocketAddress[] addr_out = {null};
		
		impl.UTP_GetPeerName( socket, addr_out );
		
		callback.incomingConnection( addr_out[0], socket, impl.UTP_GetSocketConnectionID( socket ) & 0x0000ffffL );
	}
	
	public void 
	on_read(
		UTPSocket	socket,
		ByteBuffer 	bytes, 
		int 		count )
	{
		callback.read( socket, bytes );
	}
		
	public int  
	get_rb_size(
		UTPSocket	socket )
	{
		return( callback.getReadBufferSize( socket ));
	}
	
	public void 
	on_state(
		UTPSocket	socket, 
		int 		state )
	{
		callback.setState( socket, state);
		
		if ( state == UTPTranslated.UTP_STATE_DESTROYING ){
				
			if ( Constants.IS_CVS_VERSION ){
				callback.checkThread();
			}
		}
	}
	
	public void 
	on_close_reason(
		UTPSocket	socket, 
		int 		reason )
	{
		callback.setCloseReason(socket, reason);
	}
	
	public void 
	on_error(
		UTPSocket	socket, 
		int 		errcode )
	{
		callback.error( socket, errcode );
	}
	
	public void 
	on_overhead(
		UTPSocket	socket, 
		boolean 	send, 
		int 		count, 
		int 		type)
	{
		callback.overhead( socket, send, count, type );
	}
	
	
		// incoming calls from plugin
	
	public  void
	checkTimeouts()
	{
		impl.UTP_CheckTimeouts();
	}
	
	public void
	incomingIdle()
	{
		impl.UTP_IncomingIdle();
	}
	
	public Object[] 
	connect(
		String		to_address,
		int			to_port )
	
		throws UTPProviderException
	{
		try{
			UTPSocket socket = impl.UTP_Create();
			
			if ( socket == null ){
				
				throw( new UTPProviderException( "Failed to create socket" ));
			}
			
			if ( Constants.IS_CVS_VERSION ){
				callback.checkThread();
			}
																
			impl.UTP_Connect( socket, new InetSocketAddress( HostNameToIPResolver.syncResolve( to_address), to_port ));
			
			return( new Object[]{ socket, impl.UTP_GetSocketConnectionID( socket ) & 0x0000ffffL });
			
		}catch( UTPProviderException e ){
			
			throw( e );
			
		}catch( Throwable e ){
			
			throw( new UTPProviderException( "connect failed", e ));
		}
	}
	
	public boolean
	receive(
		String		from_address,
		int			from_port,
		byte[]		data,
		int			length )
	
		throws UTPProviderException
	{
		try{
			return( impl.UTP_IsIncomingUTP(this, this, "", data, length, new InetSocketAddress( HostNameToIPResolver.syncResolve( from_address), from_port )));
			
		}catch( Throwable e ){
			
			throw( new UTPProviderException( "receive failed", e ));
		}
	}
		
	public boolean
	write(
		UTPSocket		utp_socket,
		ByteBuffer[]	buffers,
		int				start,
		int				len )
	
		throws UTPProviderException
	{
		if ( Constants.IS_CVS_VERSION ){
			callback.checkThread();
		}
		
		return( impl.UTP_Write( utp_socket, buffers, start, len ));
	}
	
	public void
	receiveBufferDrained(
		UTPSocket		utp_socket )
	
		throws UTPProviderException
	{
		if ( Constants.IS_CVS_VERSION ){
			callback.checkThread();
		}
		
		impl.UTP_RBDrained( utp_socket );
	}
		
	public void
	close(
		UTPSocket	utp_socket,
		int			close_reason )
	
		throws UTPProviderException
	{
		if ( Constants.IS_CVS_VERSION ){
			callback.checkThread();
		}
		
		impl.UTP_Close( utp_socket, close_reason );
	}
	
	
	public void
	setSocketOptions(
		long		fd )
	
		throws UTPProviderException
	{
		// this is supposed to be a native method to enable/disable fragmentation
	}
	
	public void
	setOption(
		int		option,
		int		value )
	{
		synchronized( pending_options ){
				
			if ( impl == null ){
				
				pending_options.put( option, value );
				
			}else{
		
				impl.UTP_SetOption( option, value );
			}
		}
	}
	
	@Override
	public UTPProviderStats 
	getStats()
	{
		return( this );
	}
	
	public long
	getSentPacketCount()
	{
		return( impl_stats.getSentPacketCount());
	}
	
	public long
	getReceivedPacketCount()
	{
		return( impl_stats.getReceivedPacketCount());
	}

	@Override
	public long 
	getPacketReceiveRate()
	{
		return( impl_stats.getPacketReceiveRate());
	}
	
	@Override
	public long 
	getPacketSendRate()
	{
		return( impl_stats.getPacketSendRate());
	}
	
	public long
	getDataSendTotal()
	{
		return( impl_stats.getDataSendTotal());
	}
	
	public long
	getDataReceiveTotal()
	{
		return( impl_stats.getDataReceiveTotal());
	}
	
	public long
	getDataSendRate()
	{
		return( impl_stats.getDataSendRate());
	}
	
	public long
	getDataReceiveRate()
	{
		return( impl_stats.getDataReceiveRate());
	}
	
	public long
	getOverheadRate()
	{
		return( impl_stats.getOverheadRate());
	}
}
