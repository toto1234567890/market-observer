#!/usr/bin/env python
# coding:utf-8

from typing import List, Dict, Any, Tuple
from os.path import join as osPathJoin, \
                    dirname as osPathDirname, abspath as osPathAbspath
from asyncio import run as asyncioRun, create_task as asyncioCreate_task, \
                    set_event_loop_policy as asyncioSet_event_loop_policy, \
                    CancelledError as asyncioCancelledError, gather as asyncioGather, \
                    get_running_loop as asyncioGet_running_loop
from sys import platform as sysPlatform
from time import time as timeTime



from src.network.network import AsyncNetworkManager
from src.data_source.yahoo_finance import YahooFinanceSource
from src.storage.sqlite import AsyncSQLiteDB
from src.server.fastAPI import FastAPIServer
from src.analysis.analysis_facade import AnalysisFacade


from src.helpers.error_handler import ErrorHandler
from src.utils.memory_manager import SimpleMemoryManager
from src.utils.constant import calculate_max_data_points
from toReplace import get_config_logger




class MarketObserver:
    """
    Main controller for the Market Observer application.
    Uses components for consistent volume calculations and better performance.
    """
    Name = "MarketObserver"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config, logger, symbols: List[str], name: str = ""):
        self.config = config
        self.logger = logger
        if name != "":    self.Name = name
        self.symbols = symbols
        self.running = True
        
        # Enhanced error handling and memory management 
        self.error_handler = ErrorHandler(logger)
        
        # Determine max data points based on retention days
        retention_days = getattr(self.config, 'RETENTION_DAYS', 7)
        max_points = calculate_max_data_points(retention_days)
            
        # Manage memory usage (size + GC.collet() if needed) and managed memory ring buffers
        self.memory_manager = SimpleMemoryManager(logger, max_memory_mb=200, max_data_points=max_points)
        
        # Dependency Injection with components
        self.network = AsyncNetworkManager(
            proxies=self.config.PROXIES,
            timeout=self.config.REQUEST_TIMEOUT,
            retries=self.config.REQUEST_RETRIES
        )

        # data sources 
        self.source = YahooFinanceSource(self.config, self.logger, self.network)

        # database management 
        self.db = AsyncSQLiteDB(self.config, self.logger)

        # implement this later...
        ##if self.config.DB_TYPE == "postgres":
        ##    from src.storage.postgres import AsyncPostgresDB
        ##    self.db = AsyncPostgresDB(self.config, self.logger)
        ##else:
        
        # Use analysis facade
        self.analyzer = AnalysisFacade(self.config)
        self.intermediate_stats = {} # Store calculated long-term stats

        # fastAPI serve data  
        self.server = FastAPIServer(config=self.config, host=self.config.SERVER_HOST, port=self.config.SERVER_PORT)
        
        # Data source have to provide clean datas and ensure that :
        # - data is sorted by timestamp
        # - data is clean and without duplicates
        # - data is complete and without missing values
        # - data is in the correct format
        # - data is updated in real time
        # - data is fetched in chunks to avoid memory overflow
        # - data is fetched in parallel to avoid delay
        # - data is fetched in a loop to avoid missing data
        # - market is open for symbol...
        
        self.logger.info(f"Initialized MarketObserver with components for {len(symbols)} symbols")
        
        # State tracking for full updates
        self.latest_aggregations = {}

    #-----------------------------------------------------------------------------------------------
    async def run(self):
        """
        Start the application services and run the main loop.
        Entry point of the application logic.
        """
        await self.initialize()
        
        # Start server in background
        server_task = asyncioCreate_task(self.server.start_server())
        
        try:
            # Main data loop
            await self.data_loop()
        except asyncioCancelledError:
            self.logger.info("Data loop cancelled.")
        finally:
            # Graceful shutdown
            server_task.cancel()
            try:
                await server_task
            except asyncioCancelledError:
                pass
            await self.shutdown()

    #-----------------------------------------------------------------------------------------------        
    async def initialize(self):
        """
        Initialize database connections and fetch initial historical data.
        Uses stats calculator for consistent volume calculations.
        """
        self.logger.info("Initializing services with components...")
        
        # if exists clear database and recreate MCD
        await self.db.initialize()

        # fetch historical datas
        initial_data = await self._fetch_initial_data()
        
        if initial_data:
            # Parallelize independent tasks: Stats calculation (CPU) and Raw Data Saving (IO)
            # Stats calculation must finish before aggregation because aggregation uses the calculated stats.
            # Data saving is completely independent.
            await asyncioGather(
                self._process_historical_stats(initial_data),
                self._save_historical_data(initial_data)
            )
            
            # Run aggregation after stats are ready
            initial_aggregations = await self._aggregate_historical_data(initial_data)
            
            # Broadcast initial state to any connected clients
            if initial_aggregations:
                 await self._broadcast_initial(initial_data, initial_aggregations)
        else:
            self.logger.error("No initial data fetched. Something wrong with data loading please check...")
            await self.shutdown()

    #-----------------------------------------------------------------------------------------------        
    async def data_loop(self):
        """
        Start listening to data source.
        """
        self.logger.info("Starting data collection via listener...")
        
        try:
            # Delegate loop control to the source
            await self.source.listen(self.symbols, self.on_data_received)
            
        except asyncioCancelledError:
             pass
        except Exception as e:
            self.logger.critical(f"Critical error in data listener: {e}")

    #-----------------------------------------------------------------------------------------------        
    async def on_data_received(self, valid_data: Dict[str, Any]):
        """
        Callback for new data received from the source (Push model).
        Orchestrates storage, aggregation, broadcasting, and cleanup.
        """
        try:
            await self._save_incoming_data(valid_data)
            
            aggregated_data, metrics = await self._aggregate_realtime_data(valid_data)
            
            await self._broadcast_realtime_update(valid_data, aggregated_data, metrics)
            
            self._update_memory_manager(valid_data)
            
            await self._periodic_cleanup()

        except Exception as e:
            self.logger.critical(f"Error in on_data_received: {e}")

    #-----------------------------------------------------------------------------------------------
    async def shutdown(self):
        """
        Gracefully stop all services and close connections.
        """
        self.logger.info("Shutting down MarketObserver...")
        self.running = False
        await self.network.close()
        await self.db.close()
        self.memory_manager.cleanup()
        self.logger.info("Shutdown completed.")

    # ==============================================================================================
    # PRIVATE HELPER METHODS
    # ==============================================================================================

    #-----------------------------------------------------------------------------------------------
    async def _fetch_initial_data(self) -> Dict[str, Any]:
        # Fetch initial historical data using error handling
        self.logger.info("Fetching initial historical data...")
        return await self.error_handler.execute_with_retry(
            "Initial data fetch",
            self.source.fetch_initial_data,
            self.symbols,
            max_retries=3
        )

    #-----------------------------------------------------------------------------------------------
    async def _process_historical_stats(self, initial_data: Dict[str, Any]):
        # Calculate stats using Facade for consistent volume calculations
        self.intermediate_stats = {} # {symbol: {window: stats}}
        
        # Facade handles efficient calculation internally
        # But we want to run in executor to keep main loop free?
        # The facade calculation is CPU bound.
        # We can run the whole facade call in executor.
        
        loop = asyncioGet_running_loop()
        window_names = self.config.WINDOWS_AGGREGATION
        
        try:
             stats_results = await loop.run_in_executor(
                 None,
                 self.analyzer.calculate_stats_for_windows,
                 initial_data,
                 window_names
             )
             
             if stats_results:
                 self.intermediate_stats = stats_results
                 
                 # Save intermediate stats by window
                 db_tasks = []
                 for window_name in window_names:
                     stats_batch = {}
                     for symbol, windows_map in self.intermediate_stats.items():
                         if window_name in windows_map:
                             stats_batch[symbol] = windows_map[window_name]
                     
                     if stats_batch:
                         db_tasks.append(self.db.save_intermediate_stats(stats_batch, window_name))
                 
                 if db_tasks:
                     await asyncioGather(*db_tasks)
                     
                 self.logger.info(f"Initialized intermediate stats for {len(self.intermediate_stats)} symbols "
                                f"across {len(window_names)} windows.")
                                
        except Exception as e:
            self.logger.error(f"Error processing historical stats: {e}")

    #-----------------------------------------------------------------------------------------------
    async def _save_historical_data(self, initial_data: Dict[str, Any]):
        # Save raw OHLCV data
        all_data = []
        for _, data in initial_data.items():
            if "time_series" in data:
                all_data.extend(data["time_series"])
        
        if all_data:
            self.logger.info(f"Saving {len(all_data)} initial historical records to DB...")
            await self.db.save_bulk(all_data)

    #-----------------------------------------------------------------------------------------------
    async def _aggregate_historical_data(self, initial_data: Dict[str, Any]):
        # Perform initial aggregation for all windows
        self.logger.info("Performing initial aggregation for historical data...")
        initial_aggregations = {}
        
        # 1. OPTIMIZATION: Pre-process data to numpy arrays ONCE
        # Optimized payload approach prevents redundant list->numpy conversions for each window
        optimized_data = {}
        for symbol, data in initial_data.items():
            time_series = data.get("time_series", [])
            if time_series:
                 # Use static method from Facade to convert to efficient (N, 3) matrix
                 # Structure matches what aggregate_initial expects via _prepare_arrays
                 optimized_data[symbol] = {
                     "time_series": AnalysisFacade.convert_to_numpy_matrix(time_series)
                 }

        if not optimized_data:
             return {}

        # 2. OPTIMIZATION: Parallelize window aggregations
        # Aggregation is CPU-bound, so we run each window in a separate thread/process via executor
        loop = asyncioGet_running_loop()
        tasks = []
        
        for window_name in self.config.WINDOWS_AGGREGATION:
            # Prepare stats for this window
            stats_for_window = {}
            for symbol, windows_map in self.intermediate_stats.items():
                if window_name in windows_map:
                    stats_for_window[symbol] = windows_map[window_name]
            
            # Schedule aggregation task
            # We pass 'optimized_data' which contains numpy arrays
            task = loop.run_in_executor(
                None,
                self.analyzer.aggregate_initial,
                optimized_data, 
                window_name, 
                stats_for_window
            )
            tasks.append(task)
            
        # 3. Wait for all aggregations to complete
        results = await asyncioGather(*tasks)
        
        # 4. Merge results: {symbol: {window: data}}
        for window_aggs in results:
            if window_aggs:
                for symbol, data_map in window_aggs.items():
                    if symbol not in initial_aggregations:
                        initial_aggregations[symbol] = {}
                    initial_aggregations[symbol].update(data_map)
        
        if initial_aggregations:
            self.logger.info(f"Saving initial aggregations for {len(initial_aggregations)} symbols...")
            await self.db.save_aggregations(initial_aggregations)
            self.logger.info("Initial aggregation completed.")
            
        return initial_aggregations

    # --- Real-time Processing Helpers ---
    #-----------------------------------------------------------------------------------------------
    async def _save_incoming_data(self, valid_data: Dict[str, Any]):
        """Save fresh raw data to DB."""
        self.logger.info(f"Received {len(valid_data)} fresh records. Saving to {self.config.DB_TYPE}...")
        await self.db.save_bulk(list(valid_data.values()))

    #-----------------------------------------------------------------------------------------------
    async def _aggregate_realtime_data(self, valid_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Run aggregations for all windows.
        Returns (aggregated_data, processing_metrics)
        """
        aggregated_data = {}
        performance_start = timeTime()
        
        for window in self.config.WINDOWS_AGGREGATION:
            # Prepare stats for this specific window
            current_window_stats = {}
            for symbol, windows_map in self.intermediate_stats.items():
                if window in windows_map:
                    current_window_stats[symbol] = windows_map[window]
                    
            # Use aggregator for volume calculations
            # Optimization: Pass raw numpy arrays from MemoryManager for speed
            # We reconstruct a 'data' dict where keys are symbols, and values are arrays
            
            # Note: The Facade expects {symbol: {time_series: ...}} or similar structure?
            # Let's check Facade.aggregate:
            # for symbol, symbol_data in data.items():
            #    time_series = symbol_data.get("time_series", [])
            
            # We need to construct this structure efficiently.
            
            optimization_data_payload = {}
            for symbol in valid_data.keys():
                # Get full history from memory as arrays
                arrays = self.memory_manager.get_latest_arrays(symbol)
                if arrays is not None:
                     optimization_data_payload[symbol] = {"time_series": arrays}

            # Returns {symbol: {window: agg}}
            window_aggregation = self.analyzer.aggregate(
                optimization_data_payload, window, 
                intermediate_stats=current_window_stats
            )
            
            # Merge into partial aggregations 
            for symbol, val in window_aggregation.items():
                if symbol not in aggregated_data:
                    aggregated_data[symbol] = {}
                aggregated_data[symbol].update(val)
        
        # Save aggregations
        await self.db.save_aggregations(aggregated_data)
        
        aggregation_time = timeTime() - performance_start
        self.logger.debug(f"Aggregation completed in {aggregation_time:.2f}s for {len(self.config.WINDOWS_AGGREGATION)} windows")
        
        metrics = {
            'aggregation_time_seconds': aggregation_time,
            'valid_symbols': len(valid_data),
            'windows_processed': len(self.config.WINDOWS_AGGREGATION)
        }
        return aggregated_data, metrics

    #-----------------------------------------------------------------------------------------------
    async def _broadcast_initial(self, initial_data: Dict[str, Any], initial_aggregations: Dict[str, Any]):
        """Broadcast initial historical data to connected clients."""
        self.logger.info("Broadcasting initial data to clients...")
        
        # Update local cache of aggregations
        if initial_aggregations:
            self.latest_aggregations.update(initial_aggregations)
            
        broadcast_payload = {
            'type': 'INITIAL',
            'raw_data': initial_data,
            'aggregations': initial_aggregations,
            'timestamp': timeTime(),
            'processing_metrics': {'initial_load': True}
        }
        # Update server state first
        self.server.all_datas(broadcast_payload)
        # Then broadcast (optional for initial, but good for consistency)
        await self.server.broadcast(broadcast_payload)

    #-----------------------------------------------------------------------------------------------
    async def _broadcast_realtime_update(self, valid_data: Dict[str, Any], aggregated_data: Dict[str, Any], metrics: Dict[str, Any]):
        """Broadcast data to connected clients."""
        
        # 1. Update local state
        if aggregated_data:
            for sym, data in aggregated_data.items():
                if sym not in self.latest_aggregations:
                    self.latest_aggregations[sym] = {}
                self.latest_aggregations[sym].update(data)
        
        # 2. Get FULL state for Server (Source of Truth)
        # Fetch latest raw data for ALL symbols from MemoryManager
        all_raw_data = self.memory_manager.get_latest_data(all_data=True)
        
        full_state_payload = {
            'type': 'UPDATE',
            'raw_data': all_raw_data,
            'aggregations': self.latest_aggregations,
            'timestamp': timeTime(),
            'processing_metrics': metrics
        }
        
        # Update server state with COMPLETE data
        self.server.all_datas(full_state_payload)
        
        # 3. Broadcast DELTA to clients
        delta_payload = {
            'type': 'UPDATE',
            'raw_data': valid_data,          # Only new points
            'aggregations': aggregated_data, # Only new aggregations
            'timestamp': timeTime(),
            'processing_metrics': metrics
        }
        await self.server.broadcast(delta_payload)

    #-----------------------------------------------------------------------------------------------
    def _update_memory_manager(self, valid_data: Dict[str, Any]):
        """Update in-memory buffer."""
        for symbol, data_point in valid_data.items():
            self.memory_manager.add_data_point(symbol, data_point)

    #-----------------------------------------------------------------------------------------------
    async def _periodic_cleanup(self):
        """Cleanup old data roughly every hour."""
        if int(timeTime()) % 3600 < 60: 
            await self.db.cleanup_old_data(self.config.RETENTION_DAYS)


#-----------------------------------------------------------------------------------------------
async def main():
    name = "MarketObserver"

    # Use improved configuration
    base_dir = osPathDirname(osPathAbspath(__file__))
    config_path = osPathJoin(base_dir, "config", "default.yaml")

    config, logger = get_config_logger(name=name, config=config_path)
    app = MarketObserver(config, logger, config.SYMBOLS, name)
    try:
        await app.run()
    except KeyboardInterrupt:
        logger.info("{0} : KeyboardInterrupt, user stopped application.".format(name))




#================================================================
if __name__ == "__main__":
    if sysPlatform == 'win32':
        from asyncio import WindowsSelectorEventLoopPolicy as asyncioWindowsSelectorEventLoopPolicy
        asyncioSet_event_loop_policy(asyncioWindowsSelectorEventLoopPolicy())
    else:
        from uvloop import EventLoopPolicy as uvloopEventLoopPolicy
        asyncioSet_event_loop_policy(uvloopEventLoopPolicy())
    try:
        asyncioRun(main())
    except KeyboardInterrupt:
        pass