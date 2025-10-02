/*
 * Created on Jan 30, 2008
 * Created by Paul Gardner
 * 
 * Copyright 2008 Vuze, Inc.  All rights reserved.
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



package com.vuze.client.plugins.utp.swt;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;

import com.aelitis.azureus.core.networkmanager.impl.utp.UTPConnection;
import com.aelitis.azureus.core.networkmanager.impl.utp.UTPConnectionManager;
import com.biglybt.core.util.DisplayFormatters;
import com.biglybt.core.util.SimpleTimer;
import com.biglybt.core.util.TimeFormatter;
import com.biglybt.core.util.TimerEvent;
import com.biglybt.core.util.TimerEventPerformer;
import com.biglybt.core.util.TimerEventPeriodic;
import com.biglybt.pif.ui.UIInstance;
import com.biglybt.ui.swt.Messages;
import com.biglybt.ui.swt.Utils;

import com.biglybt.ui.swt.components.graphics.SpeedGraphic;
import com.biglybt.ui.swt.components.graphics.ValueFormater;
import com.biglybt.ui.swt.mainwindow.Colors;
import com.biglybt.ui.swt.pif.UISWTInstance;

import com.biglybt.ui.swt.pif.UISWTView;
import com.biglybt.ui.swt.pif.UISWTViewBuilder;
import com.biglybt.ui.swt.pif.UISWTViewEvent;
import com.biglybt.ui.swt.pif.UISWTViewEventListener;
import com.biglybt.ui.swt.shells.GCStringPrinter;
import com.vuze.client.plugins.utp.UTPPlugin;
import com.vuze.client.plugins.utp.UTPProvider;
import com.vuze.client.plugins.utp.UTPProviderStats;
import com.vuze.client.plugins.utp.UTPSocket;


public class 
UTPViewManager 
{
	private static final String VIEW_ID = "azutp.name";
	
	final private UTPPlugin				plugin;
	final private UTPConnectionManager	connection_manager;
	final private UTPProvider			provider;
	final private UTPProviderStats		provider_stats;
	
	final private UISWTInstance		ui_instance;	
	
	private TimerEventPeriodic	view_timer;

	private Map<UISWTView,UTPViewInstance>		views = new HashMap<>();

	private boolean		unloaded;
	
	public
	UTPViewManager(
		UTPPlugin					_plugin,
		UIInstance					_ui )
	{
		plugin			= _plugin;
		ui_instance 	= (UISWTInstance)_ui;
		
		connection_manager = plugin.getConnectionManager();
		
		provider = connection_manager.getProvider();
		
		provider_stats = provider.getStats();
		
		ui_instance.registerView(UISWTInstance.VIEW_MAIN,
				ui_instance.createViewBuilder( VIEW_ID ).setListenerInstantiator(
							new UISWTViewBuilder.UISWTViewEventListenerInstantiator()
							{
								@Override
								public boolean
								supportsMultipleViews()
								{
									return( true );
								}
								
								public UISWTViewEventListener 
								createNewInstance(UISWTViewBuilder Builder, UISWTView forView) throws Exception
								{
									return( new UTPView());
								}
								@Override
								public String 
								getUID()
								{
									return( VIEW_ID );
								}
							}));
	}
		
	class
	UTPView
		implements UISWTViewEventListener
	{
		@Override
		public boolean
		eventOccurred(
			UISWTViewEvent event )
		{
			UISWTView view = event.getView();
			
			UTPViewInstance instance;
			
			synchronized( views ){
				
				if ( unloaded ){
					
					return( false );
				}
				
				instance = views.get( view );
			}
			
			switch( event.getType() ){
	
				case UISWTViewEvent.TYPE_CREATE:{
					
					if ( instance != null ){
						
						return( false );
					}
					
					instance = new UTPViewInstance( view );
					
					synchronized( views ){
						
						if ( unloaded || views.containsKey( view )){
							
							return( false );
						}
						
						views.put( view, instance );
						
						if ( view_timer == null ){
							
							view_timer = SimpleTimer.addPeriodicEvent(
									"I2PView:stats",
									1000,
									new TimerEventPerformer() 
									{
										@Override
										public void
										perform(
											TimerEvent event ) 
										{
											if ( views != null ){
												
												Utils.execSWTThread(
													new Runnable()
													{
														@Override
														public void
														run()
														{
															for ( UTPViewInstance view: views.values() ){
																	
																view.periodicUpdate();
															}
														}
													});
											}
										}
									});
						}
					}
					
					break;
				}
				case UISWTViewEvent.TYPE_INITIALIZE:{
					
					if ( instance != null ){
					
						instance.initialise((Composite)event.getData());
					}
					
					break;
				}
				case UISWTViewEvent.TYPE_REFRESH:{
					
					if ( instance != null ){
						
						instance.refresh( event );
					}
					
					break;
				}
				case UISWTViewEvent.TYPE_DESTROY:{
					
					if ( instance != null ){
						
						instance.destroy();
					}
					
					synchronized( views ){
						
						views.remove( view );
						
						if ( views.isEmpty() && view_timer != null ){
							
							view_timer.cancel();
							
							view_timer = null;
						}
					}
					
					break;
				}
			}
			
			return true;
		}
	}
	
	class
	UTPViewInstance
	{
		Label lblReceivedPackets,lblReceivedBytes;
		Label lblSentPackets,lblSentBytes;

		Composite	panel;
		
		SpeedGraphic dataInGraph, dataOutGraph, overheadGraph;
		SpeedGraphic packetInGraph, packetOutGraph;
		  
		Image	connections_img;
		Canvas	connections_canvas;
		
		boolean initialised = false;
		
		UTPViewInstance(
			UISWTView		view )
		{
			ValueFormater formater = ( v )->{
				return( v + "/" + TimeFormatter.TIME_SUFFIXES_2[ TimeFormatter.TS_SECOND ]);
			};
			
			packetInGraph = SpeedGraphic.getInstance(formater);
			packetOutGraph = SpeedGraphic.getInstance(formater);
		    dataInGraph = SpeedGraphic.getInstance();
		    dataOutGraph = SpeedGraphic.getInstance();
		    overheadGraph = SpeedGraphic.getInstance();
		}
		
		void
		initialise(
			Composite		composite )
		{
		 	panel = new Composite(composite,SWT.NULL);
		    GridLayout layout = new GridLayout();
		    layout.numColumns = 2;
		    panel.setLayout(layout);

		    initialiseTransportDetailsGroup();
		    
		    initialiseConnectionGroup();
		    
		    initialised = true;
		}
		
		private void 
		initialiseTransportDetailsGroup() 
		{
			Group gTransport = Utils.createSkinnedGroup(panel,SWT.NONE);
			Messages.setLanguageText(gTransport,"DHTView.transport.title");

			GridData data = new GridData(GridData.FILL_VERTICAL);
			data.widthHint = 400;
			gTransport.setLayoutData(data);

			GridLayout layout = new GridLayout();
			layout.numColumns = 3;
			layout.makeColumnsEqualWidth = true;
			gTransport.setLayout(layout);


			Label label = new Label(gTransport,SWT.NONE);

			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"DHTView.transport.packets");
			label.setLayoutData(new GridData(SWT.FILL,SWT.TOP,true,false));

			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"DHTView.transport.bytes");
			label.setLayoutData(new GridData(SWT.FILL,SWT.TOP,true,false));

			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"DHTView.transport.received");

			lblReceivedPackets = new Label(gTransport,SWT.NONE);
			lblReceivedPackets.setLayoutData(new GridData(SWT.FILL,SWT.TOP,true,false));

			lblReceivedBytes = new Label(gTransport,SWT.NONE);
			lblReceivedBytes.setLayoutData(new GridData(SWT.FILL,SWT.TOP,true,false));

			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"DHTView.transport.sent");

			lblSentPackets = new Label(gTransport,SWT.NONE);
			lblSentPackets.setLayoutData(new GridData(SWT.FILL,SWT.TOP,true,false));

			lblSentBytes = new Label(gTransport,SWT.NONE);
			lblSentBytes.setLayoutData(new GridData(SWT.FILL,SWT.TOP,true,false));

			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"utp.packets.in");
			data = new GridData();
			data.horizontalSpan = 3;
			label.setLayoutData(data);

			Canvas packetin = new Canvas(gTransport,SWT.NO_BACKGROUND);
			data = new GridData(GridData.FILL_BOTH);
			data.horizontalSpan = 3;
			packetin.setLayoutData(data);
			packetInGraph.initialize(packetin);

			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"utp.packets.out");
			data = new GridData();
			data.horizontalSpan = 3;
			label.setLayoutData(data);

			Canvas packetout = new Canvas(gTransport,SWT.NO_BACKGROUND);
			data = new GridData(GridData.FILL_BOTH);
			data.horizontalSpan = 3;
			packetout.setLayoutData(data);
			packetOutGraph.initialize(packetout);
			
			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"utp.bytes.in");
			data = new GridData();
			data.horizontalSpan = 3;
			label.setLayoutData(data);

			Canvas datain = new Canvas(gTransport,SWT.NO_BACKGROUND);
			data = new GridData(GridData.FILL_BOTH);
			data.horizontalSpan = 3;
			datain.setLayoutData(data);
			dataInGraph.initialize(datain);

			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"utp.bytes.out");
			data = new GridData();
			data.horizontalSpan = 3;
			label.setLayoutData(data);

			Canvas dataout = new Canvas(gTransport,SWT.NO_BACKGROUND);
			data = new GridData(GridData.FILL_BOTH);
			data.horizontalSpan = 3;
			dataout.setLayoutData(data);
			dataOutGraph.initialize(dataout);

			label = new Label(gTransport,SWT.NONE);
			Messages.setLanguageText(label,"utp.bytes.resent");
			data = new GridData();
			data.horizontalSpan = 3;
			label.setLayoutData(data);
			
			Canvas overhead = new Canvas(gTransport,SWT.NO_BACKGROUND);
			data = new GridData(GridData.FILL_BOTH);
			data.horizontalSpan = 3;
			overhead.setLayoutData(data);
			overheadGraph.initialize(overhead);

		}
		
		void
		initialiseConnectionGroup()
		{
			Group gConnections = Utils.createSkinnedGroup(panel,SWT.NONE);
			Messages.setLanguageText(gConnections,"label.connections");

			GridData data = new GridData(GridData.FILL_BOTH);
			gConnections.setLayoutData(data);

			GridLayout layout = new GridLayout();
			layout.numColumns = 1;
			layout.makeColumnsEqualWidth = true;
			gConnections.setLayout(layout);
			
			connections_canvas = new Canvas(gConnections,SWT.NO_BACKGROUND);
			data = new GridData(GridData.FILL_BOTH);
			connections_canvas.setLayoutData(data);
			
			connections_canvas.setLayout(layout);
			
			connections_canvas.addListener( SWT.Dispose, (ev)->{
				if ( connections_img != null && !connections_img.isDisposed()){
					connections_img.dispose();
					connections_img = null;
				}
			});
			
			connections_canvas.addPaintListener(
				new PaintListener(){
					@Override
					public void 
					paintControl(
						PaintEvent e ) 
					{		
						Rectangle client_bounds = connections_canvas.getClientArea();

						int	x		= e.x;
						int y		= e.y;
						int width 	= e.width;
						int height	= e.height;
							
						if ( client_bounds.width > 0 && client_bounds.height > 0 ){
													
							updateConnectionsImage();
								
							e.gc.drawImage(connections_img, x, y, width, height, x, y, width, height);			
						}
					}
				});
		}
		
		private void
		updateConnectionsImage()
		{
			if ( connections_canvas == null || connections_canvas.isDisposed()){
				
				return;
			}

			Rectangle bounds = connections_canvas.getClientArea();

			int	width 	= bounds.width;
			int height	= bounds.height;
			
			if ( width <= 0 || height <= 0 ){

				return;
			}


			boolean clearImage = 
					connections_img == null || 
					connections_img.isDisposed() ||
					connections_img.getBounds().width != bounds.width ||
					connections_img.getBounds().height != bounds.height;
			
			if ( clearImage ){
				
				if ( connections_img != null && !connections_img.isDisposed()){
					
					connections_img.dispose();
				}
				
				connections_img = new Image( connections_canvas.getDisplay(), width, height );
			}

			GC gc = new GC( connections_img );
			
			try{
				boolean dark_mode =  Utils.isDarkAppearanceNative();
				
				gc.setBackground( dark_mode?connections_canvas.getBackground():Colors.white);
				
				if ( dark_mode ){
					
					gc.setForeground( connections_canvas.getForeground());
				}
				
				gc.fillRectangle( 0, 0, width, height );
				
				List<UTPConnection> connections = connection_manager.getConnections();
				
				int num_connections = connections.size();
				
				if ( num_connections > 0 ){
					
					int[]		rtt_snap			= new int[ num_connections ];
					long[][]	windows_snap		= new long[ num_connections][];
					long[]		data_queued_snap	= new long[ num_connections ];
					int[]		data_pending_snap	= new int[ num_connections ];
					boolean[][] flags_snap			= new boolean[ num_connections ][];
					
					int	max_rtt		= 0;
					int max_window	= 0;
					int max_window2	= 0;
					long max_queued	= 0;
					int	max_pending	= 0;
					
					long total_queued = 0;
					long total_pending = 0;
					
					for ( int i=0;i<num_connections; i++ ){
						
						UTPConnection connection = connections.get(i);
						
						UTPSocket socket = connection.getSocket();
												
						int rtt = socket.getRTT();
						
						rtt_snap[i]	= rtt;
						
						max_rtt = Math.max( max_rtt, rtt );
						
						long[] windows = socket.getWindowsSizes();
						
						windows_snap[i] = windows;
						
						max_window	= Math.max( max_window, (int)windows[0] );
						max_window2	= Math.max( max_window2, (int)windows[1] );

						long queued = socket.getSendDataBufferSize();
					
						total_queued += queued;
						
						max_queued = Math.max( max_queued, queued );
						
						data_queued_snap[i] = queued;
						
						int pending = connection.getSendPendingSize();
						
						total_pending += pending;
						
						max_pending = Math.max( max_pending, pending);
						
						data_pending_snap[i] = pending;
						
						boolean[] flags = socket.getFlags();
												
						boolean [] f = flags_snap[i] = new boolean[ flags.length+1];
						
						for ( int j=0; j< flags.length; j++ ){
							
							f[j] = flags[j];
						}
						
						f[flags.length] = connection.canWrite();
					}
					
					int connection_width = width / num_connections;
					
					if ( connection_width > 32 ){
						
						connection_width = 32;
						
					}else if ( connection_width < 2 ){
						
						connection_width = 2;
					}
					
					String[] headers = 
						{	"Total Connections: " + connections.size(),
							"Window Max = " + DisplayFormatters.formatByteCountToKiBEtc( max_window ),
							"Sending Total: " + DisplayFormatters.formatByteCountToKiBEtc( total_queued ) + ", Max: " + DisplayFormatters.formatByteCountToKiBEtc( max_queued ),
							"Pending Total: " + DisplayFormatters.formatByteCountToKiBEtc( total_pending ) + ", Max: " + DisplayFormatters.formatByteCountToKiBEtc( max_pending ),
							"RTT Max: " + max_rtt,
							"Flags"
						};
					
					int header_height = 0;
					
					for ( String header: headers ){
						header_height = Math.max( header_height, GCStringPrinter.stringExtent(gc, header ).y);
					}
					
					int border = 4;
					
					int all_headers_height = ( header_height + 2*border) * headers.length;
										
					int v_pos = border;
									
						// total
					
					GCStringPrinter.printString(gc, headers[0], new Rectangle(border, v_pos, width, header_height ));

					v_pos += header_height + border;
					
						// data entries
					
					int num_data = 5;
					
					int data_height = ( height - all_headers_height ) / num_data;

					int[] data_ys = new int[num_data];

					for ( int i=0;i<num_data;i++){
						
						GCStringPrinter.printString(gc, headers[i+1], new Rectangle(border, v_pos, width, header_height ));

						v_pos += header_height + border;
						
						data_ys[i] = v_pos + data_height;
						
						v_pos += data_height + border;
					}
										
					for ( int i=0;i<num_connections; i++ ){
						
						int			rtt		= rtt_snap[i];
						long[]		windows = windows_snap[i];
						long		queued	= data_queued_snap[i];
						long		pending	= data_pending_snap[i];
						boolean[]	flags	= flags_snap[i];
						
							// Window
						
						gc.setBackground( Colors.fadedGreen );
												
						int h = max_window==0?0:( data_height * (int)windows[0] ) / max_window;
						
						gc.fillRectangle( connection_width*i,  data_ys[0] - h, connection_width-1, h);

							// Queued
						
						gc.setBackground( Colors.fadedRed );
														
						h = max_queued==0?0:(int)( ( data_height * queued ) / max_queued );
						
						gc.fillRectangle( connection_width*i,  data_ys[1] - h, connection_width-1, h);
						
							// Pending
						
						h = max_pending==0?0:(int)( ( data_height * pending ) / max_pending );
						
						gc.fillRectangle( connection_width*i,  data_ys[2] - h, connection_width-1, h);
						
							// RTT
						
						gc.setBackground( Colors.bluesFixed[Colors.BLUES_MIDDARK]);
												
						h = max_rtt==0?0:( data_height * rtt ) / max_rtt;
						
						gc.fillRectangle( connection_width*i,  data_ys[3] - h, connection_width-1, h);
						
							// Flags
						
						gc.setBackground( Colors.orange );
						
						int h_start = data_ys[4] - data_height + connection_width;
						
						h = connection_width;

						for ( int j=0;j<flags.length;j++){
							boolean b = flags[j];
							if ( b ){
								if ( j == 2 ){
									gc.setBackground( Colors.fadedGreen );
								}
								gc.fillRectangle( connection_width*i,  h_start - h, connection_width-1, h);
							}
							
							h_start += h+8;
						}
					}
				}
			}finally{
				
				gc.dispose();
			}
		}
				
		void
		refresh(
			UISWTViewEvent	event )
		{
			if ( !initialised ){
				
				return;
			}
			
			packetInGraph.refresh( false );
			
			packetOutGraph.refresh( false );
			
			dataInGraph.refresh( false );
			
			dataOutGraph.refresh( false );
			
			overheadGraph.refresh( false );
			
			Utils.execSWTThread(()->{ connections_canvas.redraw(); });
		}
		
		void
		periodicUpdate()
		{
			if ( !initialised ){
				
				return;
			}

			lblReceivedPackets.setText(String.valueOf( provider_stats.getReceivedPacketCount()));
			lblSentPackets.setText(String.valueOf( provider_stats.getSentPacketCount()));
			
			lblReceivedBytes.setText( DisplayFormatters.formatByteCountToKiBEtc( provider_stats.getDataReceiveTotal()));
			lblSentBytes.setText( DisplayFormatters.formatByteCountToKiBEtc( provider_stats.getDataSendTotal()));
			
			packetInGraph.addIntValue((int)provider_stats.getPacketReceiveRate());
			
			packetOutGraph.addIntValue((int)provider_stats.getPacketSendRate());

	    	dataInGraph.addIntValue((int)provider_stats.getDataReceiveRate());
	    	
	    	dataOutGraph.addIntValue((int)provider_stats.getDataSendRate());
	    	
	    	// dataInGraph.addIntValue((int)connection_manager.getDispatchRate());
	    	
	    	// dataOutGraph.addIntValue((int)connection_manager.getSelector().getSelectRate());
	    	
	    	overheadGraph.addIntValue((int)provider_stats.getOverheadRate());
		}
		
		void
		destroy()
		{
			packetInGraph.dispose();
			packetOutGraph.dispose();
			dataOutGraph.dispose();
			dataInGraph.dispose();
			overheadGraph.dispose();
		}
	}
	
	public void
	unload()
	{
		synchronized( this ){
			
			unloaded = true;
			
			if ( view_timer != null ){
				
				view_timer.cancel();
				
				view_timer = null;
			}
		}
		
		ui_instance.removeViews( UISWTInstance.VIEW_MAIN, VIEW_ID );
						
		List<UTPViewInstance>	to_destroy;
		
		synchronized( views ){
			
			to_destroy = new ArrayList<>( views.values());
		}
		
		for ( UTPViewInstance view: to_destroy ){
			
			view.destroy();
		}
	}
}
