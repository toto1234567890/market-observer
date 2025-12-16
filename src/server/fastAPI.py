#!/usr/bin/env python
# coding:utf-8

from typing import Dict, Any, List
from os import makedirs as osMakedirs
from os.path import join as osPathJoin, dirname as osPathDirname, abspath as osPathAbspath, \
                    exists as osPathExists
from time import time as timeTime
from json import loads as jsonLoads, dumps as jsonDumps
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from contextlib import asynccontextmanager

from src.interfaces.data_exchange import IDataExchanger


import logging
logger = logging.getLogger(__name__)



#----------------------------------------------------------------------------------------------- 
#  Websocket manager
#-----------------------------------------------------------------------------------------------  

class WebsocketManager:
    """Manage WebSocket connections for real-time updates"""

    #-----------------------------------------------------------------------------------------------      
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    #-----------------------------------------------------------------------------------------------  
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New WebSocket connection. Total: {len(self.active_connections)}")

    #-----------------------------------------------------------------------------------------------      
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    #-----------------------------------------------------------------------------------------------      
    async def broadcast(self, message: Dict[str, Any]):
        """Broadcast data to all connected clients"""
        if not self.active_connections:
            return
            
        message_json = jsonDumps(message)
        disconnected = []
        
        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
            except Exception as e:
                logger.warning(f"Failed to send to WebSocket: {e}")
                disconnected.append(connection)
        
        # Remove disconnected clients
        for connection in disconnected:
            self.disconnect(connection)


#----------------------------------------------------------------------------------------------- 
#  Fast API server  
#-----------------------------------------------------------------------------------------------  

class FastAPIServer(IDataExchanger):
    """
    FastAPI server with WebSocket support for real-time dashboard.
    Serves both REST API and real-time WebSocket updates.
    """

    #-----------------------------------------------------------------------------------------------      
    def __init__(self, config, host: str = "127.0.0.1", port: int = 8000):
        self.config = config
        self.host = host
        self.port = port
        self.connection_manager = WebsocketManager()
        self.latest_data = {
                'raw_data': {},
                'aggregations': {},
                'timestamp': 0,
                'processing_metrics': {}
            }
        
        # Create FastAPI app with lifespan context
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info("Starting FastAPI server")
            yield
            # Shutdown
            logger.info("Stopping FastAPI server")
        
        self.app = FastAPI(
            title="Market Observer", 
            lifespan=lifespan,
            docs_url="/api/docs",
            redoc_url="/api/redoc"
        )
        
        self._setup_routes()
        self._setup_static_files()

    #-----------------------------------------------------------------------------------------------      
    def _setup_routes(self):
        """Setup both REST and WebSocket routes"""
        
        # Serve dashboard HTML
        @self.app.get("/", response_class=HTMLResponse)
        async def get_dashboard():
            current_dir = osPathDirname(osPathAbspath(__file__))
            html_path = osPathJoin(current_dir, "market_observer.html")
            if osPathExists(html_path):
                return FileResponse(html_path)
            else:
                return HTMLResponse("<h1>market_observer.html file not found</h1>", status_code=404)

    #-----------------------------------------------------------------------------------------------          
        # WebSocket endpoint for real-time data
        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await self.connection_manager.connect(websocket)
            try:
                while True:
                    # Keep connection alive, client can send commands
                    data = await websocket.receive_text()
                    # Handle client commands (filter symbols, time ranges, etc.)
                    await self._handle_client_message(websocket, data)
            except WebSocketDisconnect:
                self.connection_manager.disconnect(websocket)

    #-----------------------------------------------------------------------------------------------          
        # REST API endpoints (backward compatibility)
        @self.app.get("/api/metrics")
        async def get_metrics():
            return self.latest_data.get('raw_data', {})
        
        @self.app.get("/api/config")
        async def get_config():
            return {
                "timeframes": self.config.WINDOWS_AGGREGATION
            }
        
        @self.app.get("/api/health")
        async def health_check():
            return {
                "status": "ok", 
                "connections": len(self.connection_manager.active_connections),
                "latest_update": self.latest_data.get('timestamp')
            }

    #-----------------------------------------------------------------------------------------------      
    def _setup_static_files(self):
        """Serve static files (HTML, CSS, JS)"""
        static_dir = osPathJoin(osPathDirname(__file__), "static")
        osMakedirs(static_dir, exist_ok=True)
        self.app.mount("/static", StaticFiles(directory=static_dir), name="static")

    #-----------------------------------------------------------------------------------------------      
    async def _handle_client_message(self, websocket: WebSocket, message: str):
        """Handle messages from client (filters, preferences, etc.)"""
        try:
            data = jsonLoads(message)
            command = data.get("command")
            
            if command == "subscribe":
                # regarding client type the data returned are not the same....
                client_type = data.get("clientType", "undefine")
                symbols = data.get("symbols", [])
                timeframe = data.get("timeframe", None)
                
                if client_type == "dashboard":
                    # dashboard data for subscribed symbols and timeframe
                    filtered_data = self.dashboard_response(symbols, timeframe)
                else: 
                    # client data for subscribed symbols and timeframe
                    filtered_data = self.symbol_view_response(symbols, timeframe)
                
                await websocket.send_text(jsonDumps(filtered_data))
                
        except Exception as e:
            logger.warning(f"Error while trying to handle client message : {str(e)}")
            await websocket.close()

    #-----------------------------------------------------------------------------------------------         
    def symbol_view_response(self, symbols: List[str], timeframe: str = None):        
        # Filter Raw Data (symbols only)
        # If symbols is empty, return all
        if not symbols:
            filtered_raw = self.latest_data.get('raw_data', {})
        else:
            filtered_raw = {
                sym: data for sym, data in self.latest_data.get('raw_data', {}).items() 
                if sym in symbols
            }
        
        # Filter Aggregations (symbols AND timeframe)
        all_aggs = self.latest_data.get('aggregations', {})
        filtered_agg = {}
        
        # aggregations structure is {Symbol: {Window: Data}}
        if not symbols:
            # All symbols
            # Filter windows if timeframe provided
            for sym, windows_map in all_aggs.items():
                if timeframe:
                     # Only include specific window if present
                     if timeframe in windows_map:
                         filtered_agg[sym] = {timeframe: windows_map[timeframe]}
                else:
                     # Include all windows if no timeframe specified
                     filtered_agg[sym] = windows_map
        else:
            # Specific symbols
            for sym in symbols:
                if sym in all_aggs:
                    windows_map = all_aggs[sym]
                    if timeframe:
                        if timeframe in windows_map:
                             filtered_agg[sym] = {timeframe: windows_map[timeframe]}
                    else:
                        filtered_agg[sym] = windows_map
        
        return {
            'raw_data': filtered_raw,
            'aggregations': filtered_agg,
            'timestamp': self.latest_data.get('timestamp')
        }

    #-----------------------------------------------------------------------------------------------         
    def dashboard_response(self, symbols: List[str], timeframe: str = None):
        # Filter Aggregations (symbols AND timeframe)
        all_aggs = self.latest_data.get('aggregations', {})
        filtered_agg = {}
        
        # aggregations structure is {Symbol: {Window: Data}}
        if not symbols:
            # All symbols
            # Filter windows if timeframe provided
            for sym, windows_map in all_aggs.items():
                # Only include specific window if present
                if timeframe in windows_map:
                    filtered_agg[sym] = {timeframe: windows_map[timeframe][-1]}
        else:
            # Specific symbols
            for sym in symbols:
                if sym in all_aggs:
                    windows_map = all_aggs[sym]
                    if timeframe in windows_map:
                         filtered_agg[sym] = {timeframe: windows_map[timeframe][-1]}
        
        return {
            'raw_data': {},
            'aggregations': filtered_agg,
            'timestamp': self.latest_data.get('timestamp')
        }

    #-----------------------------------------------------------------------------------------------      
    def all_datas(self, data: Dict[str, Any]):
        """Update internal state with new data"""
        data.pop('type', None)
        self.latest_data = data

    #-----------------------------------------------------------------------------------------------      
    async def broadcast(self, message: Dict[str, Any]):
        """Broadcast data to WebSocket clients"""
        # Directly broadcast the message (delta) to clients
        await self.connection_manager.broadcast(message)
        logger.debug(f"Broadcast to {len(self.connection_manager.active_connections)} clients")

    #-----------------------------------------------------------------------------------------------      
    async def start_server(self):
        """Run the server (to be called as background task)"""
        import uvicorn
        config = uvicorn.Config(
            app=self.app, 
            host=self.host, 
            port=self.port, 
            log_level="info"
        )
        server = uvicorn.Server(config)
        await server.serve()