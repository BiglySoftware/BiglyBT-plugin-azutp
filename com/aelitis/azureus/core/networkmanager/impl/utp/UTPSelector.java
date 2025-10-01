/*
 * Created on 23 Jun 2006
 * Created by Paul Gardner
 * Copyright (C) 2006 Aelitis, All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 * 
 * AELITIS, SAS au capital de 46,603.30 euros
 * 8 Allee Lenotre, La Grille Royale, 78600 Le Mesnil le Roi, France.
 *
 */

package com.aelitis.azureus.core.networkmanager.impl.utp;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.biglybt.core.config.COConfigurationManager;
import com.biglybt.core.util.AEThread2;
import com.biglybt.core.util.Debug;
import com.biglybt.core.util.SystemTime;

import com.biglybt.core.networkmanager.impl.TransportHelper;

public class 
UTPSelector 
{
	private static final int MANAGER_POLL_FREQUENCY		= 250;
	private static final int SELECTOR_POLL_FREQUENCY	= COConfigurationManager.getIntParameter( "network.utp.poll.time", 50 );
	
	private AEThread2	thread;
	
	private LinkedBlockingQueue<Object[]>	ready_set	= new LinkedBlockingQueue<Object[]>();
	
	//private final Average select_rate	= Average.getInstance(1000, 10);

	private volatile boolean destroyed;
	
	protected
	UTPSelector(
		final UTPConnectionManager		manager )
	{
		thread = 
			new AEThread2( "UTPSelector", true )
			{
				public void
				run()
				{
					boolean	quit		= false;
					long	last_poll	= 0;
					
					int		last_connection_count = 0;
					
					while( !quit ){
					
						if ( destroyed ){
							
								// one last dispatch cycle
							
							quit	= true;
						}
						
						long	now = SystemTime.getMonotonousTime();
						
						if ( now - last_poll >= MANAGER_POLL_FREQUENCY ){
							
							last_connection_count = manager.poll( now );
							
							last_poll	= now;
						}
						
						if ( ready_set.isEmpty()){
							
							manager.inputIdle();
						}
						
						try{
							Object[] entry = ready_set.poll( last_connection_count==0?1000:(SELECTOR_POLL_FREQUENCY/2 ), TimeUnit.MILLISECONDS );
								
							if ( entry == null || entry.length == 0 ){
										
								continue;
							}
							
							TransportHelper	transport 	= (TransportHelper)entry[0];
							
							TransportHelper.selectListener	listener = (TransportHelper.selectListener)entry[1];
							
							if ( listener == null ){
								
								Debug.out( "Null listener" );
								
							}else{
								
								Object	attachment = entry[2];
								
								try{
									if ( entry.length == 3 ){
										
										listener.selectSuccess( transport, attachment );
										
									}else{
										
										listener.selectFailure( transport, attachment, (Throwable)entry[3] );
										
									}
								}catch( Throwable e ){
									
									Debug.printStackTrace(e);
								}
							}
						}catch( InterruptedException e ){
							
							Debug.out( e );
							
							try{
								Thread.sleep(1000);
								
							}catch( Throwable f ){
								
								Debug.out( f );
								
								break;
							}
						}
					}
				}
			};
			
		thread.setPriority( Thread.MAX_PRIORITY-1 );
		
		thread.start();
	}
	
	/*
	public long
	getSelectRate()
	{
		return( select_rate.getAverage());
	}
	*/
	
	protected void
	destroy()
	{
		synchronized( ready_set ){
			
			destroyed	= true;
		}
	}	

	protected void
	wakeup()
	{
		ready_set.offer( new Object[0] );
	}
	
	protected void
	ready(
		TransportHelper						transport,
		TransportHelper.selectListener		listener,
		Object								attachment )
	{
		if ( destroyed ){
			
			Debug.out( "Selector has been destroyed" );
			
			throw( new RuntimeException( "Selector has been destroyed" ));
		}
		
		if ( !ready_set.isEmpty()){
			
			Iterator<Object[]>	it = ready_set.iterator();
			
			while( it.hasNext()){
			
				Object[]	entry = (Object[])it.next();
				
				if ( entry.length >= 2 &&  entry[1] == listener ){
					
					it.remove();
										
					break;
				}
			}
		}
		
		ready_set.offer( new Object[]{ transport, listener, attachment });
	}
	
	protected void
	ready(
		TransportHelper						transport,
		TransportHelper.selectListener		listener,
		Object								attachment,
		Throwable							error )
	{
		if ( destroyed ){
			
			Debug.out( "Selector has been destroyed" );
			
			throw( new RuntimeException( "Selector has been destroyed" ));
		}
	
		if ( !ready_set.isEmpty()){
			
			Iterator<Object[]>	it = ready_set.iterator();
			
			while( it.hasNext()){
			
				Object[]	entry = it.next();
				
				if ( entry.length >= 2 &&  entry[1] == listener ){
					
					it.remove();
										
					break;
				}
			}
		}
		
		ready_set.offer( new Object[]{ transport, listener, attachment, error });
	}
	
	protected void
	cancel(
		TransportHelper						transport,
		TransportHelper.selectListener		listener )
	{
		if ( !ready_set.isEmpty()){
			
			Iterator<Object[]>	it = ready_set.iterator();
			
			while( it.hasNext()){
			
				Object[]	entry = it.next();
				
				if ( entry.length >= 2 &&  entry[0] == transport && entry[1] == listener ){
					
					it.remove();
										
					break;
				}
			}
		}
	}
}
