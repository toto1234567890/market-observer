#!/usr/bin/env python
# coding:utf-8

from typing import List, Dict, Any
from time import time as timeTime
from asyncio import gather as asyncioGather
from aiosqlite import connect as aiosqliteConnect

from src.interfaces.database import IDatabase



# Calculate safe batch size based on SQLite limits
# Standard limit is 32766 variables. We take 32000 for safety.
# Query has 6 parameters per row.
sqlite_max_vars = 32000
params_per_row = 6
SQLITE_BATCH_SIZE = sqlite_max_vars // params_per_row

class AsyncSQLiteDB(IDatabase):
    """
    Async SQLite storage implementation.
    """

    #-----------------------------------------------------------------------------------------------    
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.db_path = config.DB_PATH
        self.conn = None

    #-----------------------------------------------------------------------------------------------        
    async def initialize(self):
        self.conn = await aiosqliteConnect(self.db_path)

        # drop table if previously exists (truncate doesn't exist in sqlite)
        await self.conn.execute("DROP TABLE IF EXISTS stock_prices")

        # Create main stock_prices table
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL,
                price_percent_change REAL,
                volume REAL,
                volume_percent_change REAL,
                timestamp INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_time ON stock_prices(symbol, timestamp)")
        
        # Create aggregation tables for each time window
        for window in self.config.WINDOWS_AGGREGATION:
            table_name = f"aggregations_{window}"

            # drop table if previously exists (truncate doesn't exist in sqlite)
            await self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            await self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    avg_price REAL,
                    price_percent_change REAL,
                    volume_percent_change REAL,
                    price_volume_correlation REAL,
                    volume_anomaly_ratio REAL,

                    start_time INTEGER,
                    end_time INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{window}_symbol_time ON {table_name}(symbol, end_time)")
        
        # Create intermediate stats table for each window
        for window in self.config.WINDOWS_AGGREGATION:
            table_name = f"intermediate_stats_{window}"

            # drop table if previously exists (truncate doesn't exist in sqlite)
            await self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            await self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    symbol TEXT PRIMARY KEY,
                    avg_volume_history REAL,
                    std_volume_history REAL,
                    data_points_history INTEGER,
                    last_history_timestamp INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        await self.conn.commit()

    #-----------------------------------------------------------------------------------------------    
    async def save_bulk(self, data_list: List[Dict[str, Any]]):
        """
        Save a batch of raw stock data points to the database.
        Optionally chunks the data to avoid memory issues and sqlite limits.
        
        Args:
            data_list: List of dictionaries containing raw stock data.
        """
        if not data_list:
            return
            
        try:
            # Prepare data tuples once (CPU bound, fast enough for main thread usually)
            tuples = [
                (
                    d.get("symbol"),
                    d.get("price"),
                    d.get("price_percent_change"),
                    d.get("volume"),
                    d.get("volume_percent_change"),
                    d.get("timestamp")
                )
                for d in data_list
            ]
            
            # Helper for single batch execution
            async def _insert_batch(batch_tuples):
                try:
                    await self.conn.executemany("""
                        INSERT INTO stock_prices (symbol, price, price_percent_change, volume, volume_percent_change, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, batch_tuples)
                    await self.conn.commit()
                except Exception as e:
                    self.logger.error(f"DB Error inserting batch: {e}")      
            
            # If small enough, just do it
            if len(tuples) <= SQLITE_BATCH_SIZE:
                await _insert_batch(tuples)
                return

            # Otherwise, chunk and gather
            chunks = [tuples[i:i + SQLITE_BATCH_SIZE] for i in range(0, len(tuples), SQLITE_BATCH_SIZE)]
            
            tasks = []
            for chunk in chunks:
                tasks.append(_insert_batch(chunk))
            
            if tasks:
                await asyncioGather(*tasks)
                self.logger.debug(f"Saved {len(data_list)} records in {len(chunks)} parallel batches (size={SQLITE_BATCH_SIZE}).")

        except Exception as e:
            self.logger.error(f"DB Error save_bulk: {e}")
    
    #-----------------------------------------------------------------------------------------------
    async def save_aggregations(self, aggregations: Dict[str, Dict[str, Any]]):
        """
        Save time-windowed aggregations to separate tables.
        
        Each time window (e.g., '5m', '1h') has its own dedicated table.
        This method iterates through the config-defined windows and bulk inserts the data.
        
        Args:
            aggregations: Nested dictionary {symbol: {window_name: aggregated_data}}.
        """
        if not aggregations:
            return
        
        try:
            for window in self.config.WINDOWS_AGGREGATION:
                table_name = f"aggregations_{window}"
                rows = []
                
                # Collect all symbols' data for this window
                for symbol, window_data in aggregations.items():
                    if window in window_data:
                        agg_data = window_data[window]
                        
                        # Normalize to list
                        if isinstance(agg_data, dict):
                            aggs_list = [agg_data]
                        elif isinstance(agg_data, list):
                            aggs_list = agg_data
                        else:
                            continue
                            
                        for agg in aggs_list:
                            rows.append((
                                symbol,
                                agg.get("open"),
                                agg.get("high"),
                                agg.get("low"),
                                agg.get("close"),
                                agg.get("volume"),
                                agg.get("avg_price"),
                                agg.get("price_percent_change"),
                                agg.get("volume_percent_change"),
                                agg.get("price_volume_correlation", 0.0),
                                agg.get("volume_anomaly_ratio", 1.0),

                                agg.get("start_time"),
                                agg.get("end_time")
                            ))
                
                # Bulk insert for this window
                if rows:
                    await self.conn.executemany(f"""
                        INSERT INTO {table_name} 
                        (symbol, open, high, low, close, volume, avg_price, price_percent_change, volume_percent_change, 
                         price_volume_correlation, volume_anomaly_ratio,
                         start_time, end_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, rows)
            
            await self.conn.commit()
            self.logger.info(f"Saved aggregations for {len(aggregations)} symbols across {len(self.config.WINDOWS_AGGREGATION)} time windows")
            
        except Exception as e:
            self.logger.error(f"Error saving aggregations: {e}")

    #-----------------------------------------------------------------------------------------------
    async def save_intermediate_stats(self, stats: Dict[str, Any], window: str):
        """
        Save intermediate calculated stats.
        stats format: {symbol: {stat_name: value, ...}}
        """
        if not stats:
            return

        table_name = f"intermediate_stats_{window}"
        try:
            rows = []
            for symbol, stat_data in stats.items():
                rows.append((
                    symbol,
                    stat_data.get("avg_volume_history"),
                    stat_data.get("std_volume_history"),
                    stat_data.get("data_points_history"),
                    stat_data.get("last_history_timestamp")
                ))

            await self.conn.executemany(f"""
                INSERT OR REPLACE INTO {table_name} 
                (symbol, avg_volume_history, std_volume_history, data_points_history, last_history_timestamp, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, rows)
            
            await self.conn.commit()
            self.logger.info(f"Saved intermediate stats for {len(stats)} symbols (window={window}).")
        except Exception as e:
            self.logger.error(f"Error saving intermediate stats for {window}: {e}")

    #-----------------------------------------------------------------------------------------------
    async def cleanup_old_data(self, retention_days: int):
        """
        Remove raw and aggregated data older than the specified retention period.
        
        Args:
            retention_days: Number of days of data to keep.
        """
        try:
            cutoff_time = int(timeTime()) - (retention_days * 24 * 60 * 60)
            
            self.logger.info(f"Cleaning up data older than {retention_days} days (timestamp < {cutoff_time})...")
            
            # 1. Cleanup stock_prices
            await self.conn.execute("DELETE FROM stock_prices WHERE timestamp < ?", (cutoff_time,))
            
            # 2. Cleanup aggregations
            for window in self.config.WINDOWS_AGGREGATION:
                table_name = f"aggregations_{window}"
                await self.conn.execute(f"DELETE FROM {table_name} WHERE start_time < ?", (cutoff_time,))
                
            await self.conn.commit()
            self.logger.info("Cleanup completed.")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
    
    async def close(self):
        if self.conn:
            await self.conn.close()
