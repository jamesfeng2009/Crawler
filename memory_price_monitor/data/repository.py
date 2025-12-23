"""
Data repository for price records and product management.
"""

from typing import List, Optional, Dict, Any, Union
from datetime import datetime, date
from decimal import Decimal
import logging
import json

from memory_price_monitor.data.models import StandardizedProduct, PriceRecord
from memory_price_monitor.data.database import DatabaseManager
from memory_price_monitor.data.sqlite_database import SQLiteDatabaseManager
from memory_price_monitor.utils.errors import DatabaseError


logger = logging.getLogger(__name__)


class PriceRepository:
    """Repository for managing price records and product data."""
    
    def __init__(self, db_manager: Union[DatabaseManager, SQLiteDatabaseManager]):
        """
        Initialize repository with database manager.
        
        Args:
            db_manager: Database manager instance (PostgreSQL or SQLite)
        """
        self.db_manager = db_manager
        self.is_sqlite = isinstance(db_manager, SQLiteDatabaseManager)
    
    def _get_placeholder(self) -> str:
        """Get the appropriate parameter placeholder for the database type."""
        return "?" if self.is_sqlite else "%s"
    
    def save_price_record(self, product: StandardizedProduct) -> bool:
        """
        Save a price record for a product.
        
        Args:
            product: Standardized product data
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # First, ensure the product exists in the products table
            product_db_id = self._ensure_product_exists(product)
            
            # Insert price record
            placeholder = self._get_placeholder()
            insert_price_query = f"""
            INSERT INTO price_records (product_id, current_price, original_price, recorded_at, metadata)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
            """
            
            # Handle metadata serialization for SQLite
            metadata_value = json.dumps(product.metadata) if self.is_sqlite else product.metadata
            
            # Convert Decimal to float for SQLite compatibility
            current_price = float(product.current_price) if self.is_sqlite else product.current_price
            original_price = float(product.original_price) if self.is_sqlite else product.original_price
            
            params = (
                product_db_id,
                current_price,
                original_price,
                product.timestamp,
                metadata_value  # Use the properly serialized metadata
            )
            
            self.db_manager.execute_query(insert_price_query, params, fetch=False)
            logger.debug(f"Saved price record for product {product.product_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save price record: {e}")
            raise DatabaseError(
                "Failed to save price record",
                {"product_id": product.product_id, "error": str(e)}
            )
    
    def _convert_row_to_price_record(self, row: tuple) -> PriceRecord:
        """
        Convert a database row to a PriceRecord object.
        
        Args:
            row: Database row tuple
            
        Returns:
            PriceRecord object
        """
        import json
        from datetime import datetime
        
        metadata = row[5] or {}
        # Handle JSON string metadata (for SQLite compatibility)
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        
        # Handle timestamp conversion from string (SQLite compatibility)
        recorded_at = row[4]
        if isinstance(recorded_at, str):
            try:
                recorded_at = datetime.fromisoformat(recorded_at.replace('Z', '+00:00'))
            except ValueError:
                recorded_at = datetime.now()
        
        return PriceRecord(
            id=row[0],
            product_id=row[1],
            current_price=Decimal(str(row[2])) if row[2] else Decimal('0'),
            original_price=Decimal(str(row[3])) if row[3] else Decimal('0'),
            recorded_at=recorded_at,
            metadata=metadata
        )
    
    def get_price_history(
        self, 
        product_id: str, 
        start_date: date, 
        end_date: date,
        source: str = None
    ) -> List[PriceRecord]:
        """
        Get price history for a product within date range.
        
        Args:
            product_id: Product identifier
            start_date: Start date for history
            end_date: End date for history
            source: Optional source filter
            
        Returns:
            List of price records
        """
        try:
            placeholder = self._get_placeholder()
            query = f"""
            SELECT pr.id, pr.product_id, pr.current_price, pr.original_price, 
                   pr.recorded_at, pr.metadata
            FROM price_records pr
            JOIN products p ON pr.product_id = p.id
            WHERE p.product_id = {placeholder} 
            AND pr.recorded_at >= {placeholder} 
            AND pr.recorded_at <= {placeholder}
            """
            
            params = [product_id, start_date, end_date]
            
            if source:
                query += f" AND p.source = {placeholder}"
                params.append(source)
            
            query += " ORDER BY pr.recorded_at ASC"
            
            results = self.db_manager.execute_query(query, tuple(params))
            
            return [self._convert_row_to_price_record(row) for row in results or []]
            
        except Exception as e:
            logger.error(f"Failed to get price history: {e}")
            raise DatabaseError(
                "Failed to get price history",
                {"product_id": product_id, "error": str(e)}
            )
    
    def get_weekly_data(self, week_offset: int = 0) -> List[PriceRecord]:
        """
        Get price data for a specific week.
        
        Args:
            week_offset: Week offset from current week (0 = current, -1 = last week)
            
        Returns:
            List of price records for the week
        """
        try:
            placeholder = self._get_placeholder()
            
            if self.is_sqlite:
                # SQLite date arithmetic
                query = f"""
                SELECT pr.id, pr.product_id, pr.current_price, pr.original_price, 
                       pr.recorded_at, pr.metadata
                FROM price_records pr
                WHERE pr.recorded_at >= date('now', {placeholder} || ' days', 'weekday 1', '-6 days')
                AND pr.recorded_at < date('now', {placeholder} || ' days', 'weekday 1', '+1 day')
                ORDER BY pr.recorded_at ASC
                """
                start_days = week_offset * 7
                end_days = week_offset * 7
                params = (start_days, end_days)
            else:
                # PostgreSQL date arithmetic
                query = f"""
                SELECT pr.id, pr.product_id, pr.current_price, pr.original_price, 
                       pr.recorded_at, pr.metadata
                FROM price_records pr
                WHERE pr.recorded_at >= (CURRENT_DATE - INTERVAL '{placeholder} weeks' - INTERVAL '6 days')
                AND pr.recorded_at < (CURRENT_DATE - INTERVAL '{placeholder} weeks' + INTERVAL '1 day')
                ORDER BY pr.recorded_at ASC
                """
                start_offset = abs(week_offset)
                end_offset = abs(week_offset) - 1 if week_offset < 0 else abs(week_offset) + 1
                params = (start_offset, end_offset)
            
            results = self.db_manager.execute_query(query, params)
            
            return [self._convert_row_to_price_record(row) for row in results or []]
            
        except Exception as e:
            logger.error(f"Failed to get weekly data: {e}")
            raise DatabaseError(
                "Failed to get weekly data",
                {"week_offset": week_offset, "error": str(e)}
            )
    
    def get_products_by_specs(
        self, 
        brand: str = None, 
        capacity: str = None,
        type: str = None
    ) -> List[StandardizedProduct]:
        """
        Get products filtered by specifications.
        
        Args:
            brand: Brand filter
            capacity: Capacity filter
            type: Memory type filter
            
        Returns:
            List of standardized products
        """
        try:
            query = """
            SELECT source, product_id, brand, model, capacity, frequency, type, url, created_at
            FROM products
            WHERE 1=1
            """
            
            params = []
            
            if brand:
                query += " AND brand = %s"
                params.append(brand)
            
            if capacity:
                query += " AND capacity = %s"
                params.append(capacity)
            
            if type:
                query += " AND type = %s"
                params.append(type)
            
            query += " ORDER BY brand, capacity, type"
            
            results = self.db_manager.execute_query(query, tuple(params))
            
            products = []
            for row in results or []:
                # Get latest price for this product
                latest_price = self._get_latest_price(row[1], row[0])
                
                product = StandardizedProduct(
                    source=row[0],
                    product_id=row[1],
                    brand=row[2],
                    model=row[3],
                    capacity=row[4],
                    frequency=row[5] or "",
                    type=row[6],
                    current_price=latest_price.get('current_price', Decimal('0')),
                    original_price=latest_price.get('original_price', Decimal('0')),
                    url=row[7],
                    timestamp=latest_price.get('timestamp', row[8]),
                    metadata=latest_price.get('metadata', {})
                )
                products.append(product)
            
            return products
            
        except Exception as e:
            logger.error(f"Failed to get products by specs: {e}")
            raise DatabaseError(
                "Failed to get products by specs",
                {"brand": brand, "capacity": capacity, "type": type, "error": str(e)}
            )
    
    def export_data(
        self,
        format: str,
        start_date: date = None,
        end_date: date = None,
        brand: str = None,
        capacity: str = None,
        type: str = None
    ) -> str:
        """
        Export price data in specified format.
        
        Args:
            format: Export format ('csv' or 'json')
            start_date: Optional start date filter
            end_date: Optional end date filter
            brand: Optional brand filter
            capacity: Optional capacity filter
            type: Optional memory type filter
            
        Returns:
            Exported data as string
            
        Raises:
            DatabaseError: If export fails
            ValueError: If format is not supported
        """
        if format.lower() not in ['csv', 'json']:
            raise ValueError(f"Unsupported export format: {format}")
        
        try:
            # Build query with filters
            query = """
            SELECT p.source, p.product_id, p.brand, p.model, p.capacity, 
                   p.frequency, p.type, p.url, pr.current_price, pr.original_price, 
                   pr.recorded_at, pr.metadata
            FROM products p
            JOIN price_records pr ON p.id = pr.product_id
            WHERE 1=1
            """
            
            params = []
            
            if start_date:
                query += " AND pr.recorded_at >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND pr.recorded_at <= %s"
                params.append(end_date)
            
            if brand:
                query += " AND p.brand = %s"
                params.append(brand)
            
            if capacity:
                query += " AND p.capacity = %s"
                params.append(capacity)
            
            if type:
                query += " AND p.type = %s"
                params.append(type)
            
            query += " ORDER BY p.brand, p.capacity, pr.recorded_at"
            
            results = self.db_manager.execute_query(query, tuple(params))
            
            if not results:
                return "" if format.lower() == 'csv' else "[]"
            
            if format.lower() == 'csv':
                return self._export_to_csv(results)
            else:
                return self._export_to_json(results)
                
        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            raise DatabaseError(
                "Failed to export data",
                {"format": format, "error": str(e)}
            )
    
    def _export_to_csv(self, results: List[tuple]) -> str:
        """Export results to CSV format."""
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        header = [
            'source', 'product_id', 'brand', 'model', 'capacity',
            'frequency', 'type', 'url', 'current_price', 'original_price',
            'recorded_at', 'metadata'
        ]
        writer.writerow(header)
        
        # Write data rows
        for row in results:
            # Convert metadata to JSON string for CSV
            metadata = row[11] or {}
            if isinstance(metadata, dict):
                import json
                metadata_str = json.dumps(metadata)
            else:
                metadata_str = str(metadata)
            
            csv_row = list(row[:11]) + [metadata_str]
            writer.writerow(csv_row)
        
        return output.getvalue()
    
    def _export_to_json(self, results: List[tuple]) -> str:
        """Export results to JSON format."""
        import json
        from datetime import datetime
        
        data = []
        for row in results:
            # Handle timestamp conversion
            recorded_at = row[10]
            if isinstance(recorded_at, str):
                try:
                    recorded_at = datetime.fromisoformat(recorded_at.replace('Z', '+00:00'))
                except ValueError:
                    pass
            
            # Handle metadata
            metadata = row[11] or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
            
            record = {
                'source': row[0],
                'product_id': row[1],
                'brand': row[2],
                'model': row[3],
                'capacity': row[4],
                'frequency': row[5],
                'type': row[6],
                'url': row[7],
                'current_price': float(row[8]) if row[8] else 0.0,
                'original_price': float(row[9]) if row[9] else 0.0,
                'recorded_at': recorded_at.isoformat() if isinstance(recorded_at, datetime) else str(recorded_at),
                'metadata': metadata
            }
            data.append(record)
        
        return json.dumps(data, indent=2, ensure_ascii=False)
    
    def cleanup_old_data(self, retention_days: int = 365) -> int:
        """
        Clean up old price records based on retention policy.
        
        Args:
            retention_days: Number of days to retain data
            
        Returns:
            Number of records deleted
            
        Raises:
            DatabaseError: If cleanup fails
        """
        try:
            # Calculate cutoff date
            from datetime import datetime, timedelta
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # Count records to be deleted
            count_query = """
            SELECT COUNT(*) FROM price_records 
            WHERE recorded_at < %s
            """
            
            count_result = self.db_manager.execute_query(count_query, (cutoff_date,))
            records_to_delete = count_result[0][0] if count_result else 0
            
            if records_to_delete == 0:
                logger.info("No old records to clean up")
                return 0
            
            # Delete old records
            delete_query = """
            DELETE FROM price_records 
            WHERE recorded_at < %s
            """
            
            self.db_manager.execute_query(delete_query, (cutoff_date,), fetch=False)
            
            logger.info(f"Cleaned up {records_to_delete} old price records")
            return records_to_delete
            
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            raise DatabaseError(
                "Failed to cleanup old data",
                {"retention_days": retention_days, "error": str(e)}
            )
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics for monitoring.
        
        Returns:
            Dictionary with database statistics
        """
        try:
            stats = {}
            
            # Count products
            product_count_query = "SELECT COUNT(*) FROM products"
            result = self.db_manager.execute_query(product_count_query)
            stats['total_products'] = result[0][0] if result else 0
            
            # Count price records
            price_count_query = "SELECT COUNT(*) FROM price_records"
            result = self.db_manager.execute_query(price_count_query)
            stats['total_price_records'] = result[0][0] if result else 0
            
            # Get date range of data
            date_range_query = """
            SELECT MIN(recorded_at), MAX(recorded_at) 
            FROM price_records
            """
            result = self.db_manager.execute_query(date_range_query)
            if result and result[0][0]:
                stats['earliest_record'] = result[0][0]
                stats['latest_record'] = result[0][1]
            
            # Count by source
            source_count_query = """
            SELECT source, COUNT(*) 
            FROM products 
            GROUP BY source
            """
            result = self.db_manager.execute_query(source_count_query)
            stats['products_by_source'] = {row[0]: row[1] for row in result or []}
            
            # Count by brand
            brand_count_query = """
            SELECT brand, COUNT(*) 
            FROM products 
            GROUP BY brand 
            ORDER BY COUNT(*) DESC 
            LIMIT 10
            """
            result = self.db_manager.execute_query(brand_count_query)
            stats['top_brands'] = {row[0]: row[1] for row in result or []}
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            raise DatabaseError(
                "Failed to get database stats",
                {"error": str(e)}
            )
    
    def _ensure_product_exists(self, product: StandardizedProduct) -> int:
        """
        Ensure product exists in database and return its ID.
        
        Args:
            product: Standardized product
            
        Returns:
            Database ID of the product
        """
        placeholder = self._get_placeholder()
        
        # Check if product already exists
        check_query = f"""
        SELECT id FROM products 
        WHERE source = {placeholder} AND product_id = {placeholder}
        """
        
        result = self.db_manager.execute_query(
            check_query, 
            (product.source, product.product_id)
        )
        
        if result:
            return result[0][0]
        
        # Insert new product
        if self.is_sqlite:
            # SQLite doesn't support RETURNING, use lastrowid
            insert_query = f"""
            INSERT INTO products (source, product_id, brand, model, capacity, frequency, type, url)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(insert_query, (
                    product.source,
                    product.product_id,
                    product.brand,
                    product.model,
                    product.capacity,
                    product.frequency,
                    product.type,
                    product.url
                ))
                return cursor.lastrowid
        else:
            # PostgreSQL supports RETURNING
            insert_query = f"""
            INSERT INTO products (source, product_id, brand, model, capacity, frequency, type, url)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
            RETURNING id
            """
            
            params = (
                product.source,
                product.product_id,
                product.brand,
                product.model,
                product.capacity,
                product.frequency,
                product.type,
                product.url
            )
            
            result = self.db_manager.execute_query(insert_query, params)
            return result[0][0]
    
    def _get_latest_price(self, product_id: str, source: str) -> Dict[str, Any]:
        """
        Get the latest price record for a product.
        
        Args:
            product_id: Product identifier
            source: Product source
            
        Returns:
            Dictionary with latest price information
        """
        query = """
        SELECT pr.current_price, pr.original_price, pr.recorded_at, pr.metadata
        FROM price_records pr
        JOIN products p ON pr.product_id = p.id
        WHERE p.product_id = %s AND p.source = %s
        ORDER BY pr.recorded_at DESC
        LIMIT 1
        """
        
        result = self.db_manager.execute_query(query, (product_id, source))
        
        if result:
            row = result[0]
            metadata = row[3] or {}
            
            # Handle JSON string metadata (for SQLite compatibility)
            if isinstance(metadata, str):
                import json
                try:
                    metadata = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
            
            # Handle timestamp conversion from string (SQLite compatibility)
            timestamp = row[2]
            if isinstance(timestamp, str):
                from datetime import datetime
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except ValueError:
                    timestamp = datetime.now()
            
            return {
                'current_price': Decimal(str(row[0])) if row[0] else Decimal('0'),
                'original_price': Decimal(str(row[1])) if row[1] else Decimal('0'),
                'timestamp': timestamp,
                'metadata': metadata
            }
        
        return {
            'current_price': Decimal('0'),
            'original_price': Decimal('0'),
            'timestamp': datetime.now(),
            'metadata': {}
        }
    
    def get_product_by_id(self, product_id: str, source: str) -> Optional[StandardizedProduct]:
        """
        Get a specific product by ID and source.
        
        Args:
            product_id: Product identifier
            source: Product source
            
        Returns:
            StandardizedProduct if found, None otherwise
        """
        try:
            placeholder = self._get_placeholder()
            query = f"""
            SELECT source, product_id, brand, model, capacity, frequency, type, url, created_at
            FROM products
            WHERE product_id = {placeholder} AND source = {placeholder}
            """
            
            result = self.db_manager.execute_query(query, (product_id, source))
            
            if not result:
                return None
            
            row = result[0]
            latest_price = self._get_latest_price(product_id, source)
            
            return StandardizedProduct(
                source=row[0],
                product_id=row[1],
                brand=row[2],
                model=row[3],
                capacity=row[4],
                frequency=row[5] or "",
                type=row[6],
                current_price=latest_price.get('current_price', Decimal('0')),
                original_price=latest_price.get('original_price', Decimal('0')),
                url=row[7],
                timestamp=latest_price.get('timestamp', row[8]),
                metadata=latest_price.get('metadata', {})
            )
            
        except Exception as e:
            logger.error(f"Failed to get product by ID: {e}")
            raise DatabaseError(
                "Failed to get product by ID",
                {"product_id": product_id, "source": source, "error": str(e)}
            )
    
    def get_latest_prices_by_date_range(
        self, 
        start_datetime: datetime, 
        end_datetime: datetime,
        source_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取指定日期范围内每个产品的最新价格
        
        Args:
            start_datetime: 开始时间
            end_datetime: 结束时间
            source_filter: 可选的数据源过滤器
            
        Returns:
            包含产品信息和价格的字典列表
        """
        try:
            placeholder = self._get_placeholder()
            
            # 构建查询，获取每个产品在指定时间范围内的最新价格
            query = f"""
            SELECT DISTINCT
                p.id as product_id,
                p.source,
                p.brand,
                p.model,
                p.capacity,
                p.frequency,
                p.type,
                p.url,
                latest_pr.current_price,
                latest_pr.original_price,
                latest_pr.recorded_at,
                latest_pr.metadata
            FROM products p
            JOIN (
                SELECT 
                    pr.product_id,
                    pr.current_price,
                    pr.original_price,
                    pr.recorded_at,
                    pr.metadata,
                    ROW_NUMBER() OVER (PARTITION BY pr.product_id ORDER BY pr.recorded_at DESC) as rn
                FROM price_records pr
                WHERE pr.recorded_at >= {placeholder} AND pr.recorded_at <= {placeholder}
            ) latest_pr ON p.id = latest_pr.product_id AND latest_pr.rn = 1
            """
            
            params = [start_datetime, end_datetime]
            
            if source_filter:
                query += f" WHERE p.source = {placeholder}"
                params.append(source_filter)
            
            query += " ORDER BY p.brand, p.model"
            
            results = self.db_manager.execute_query(query, tuple(params))
            
            price_data = []
            for row in results or []:
                # 处理元数据
                metadata = row[11] or {}
                if isinstance(metadata, str):
                    import json
                    try:
                        metadata = json.loads(metadata)
                    except (json.JSONDecodeError, TypeError):
                        metadata = {}
                
                # 处理时间戳
                recorded_at = row[10]
                if isinstance(recorded_at, str):
                    try:
                        recorded_at = datetime.fromisoformat(recorded_at.replace('Z', '+00:00'))
                    except ValueError:
                        recorded_at = datetime.now()
                
                price_data.append({
                    'product_id': row[0],
                    'source': row[1],
                    'brand': row[2],
                    'model': row[3],
                    'capacity': row[4],
                    'frequency': row[5] or "",
                    'type': row[6],
                    'url': row[7],
                    'current_price': float(row[8]) if row[8] else 0.0,
                    'original_price': float(row[9]) if row[9] else 0.0,
                    'recorded_at': recorded_at,
                    'metadata': metadata
                })
            
            return price_data
            
        except Exception as e:
            logger.error(f"Failed to get latest prices by date range: {e}")
            raise DatabaseError(
                "Failed to get latest prices by date range",
                {"start": start_datetime, "end": end_datetime, "error": str(e)}
            )
    
    def get_daily_price_summary(self, target_date: date, source_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        获取指定日期的价格汇总信息
        
        Args:
            target_date: 目标日期
            source_filter: 可选的数据源过滤器
            
        Returns:
            包含价格汇总信息的字典
        """
        try:
            start_datetime = datetime.combine(target_date, datetime.min.time())
            end_datetime = datetime.combine(target_date, datetime.max.time())
            
            placeholder = self._get_placeholder()
            
            # 获取当日价格统计
            stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT pr.product_id) as unique_products,
                AVG(pr.current_price) as avg_price,
                MIN(pr.current_price) as min_price,
                MAX(pr.current_price) as max_price
            FROM price_records pr
            JOIN products p ON pr.product_id = p.id
            WHERE pr.recorded_at >= {placeholder} AND pr.recorded_at <= {placeholder}
            """
            
            params = [start_datetime, end_datetime]
            
            if source_filter:
                stats_query += f" AND p.source = {placeholder}"
                params.append(source_filter)
            
            stats_result = self.db_manager.execute_query(stats_query, tuple(params))
            
            if stats_result and stats_result[0][0] > 0:
                row = stats_result[0]
                return {
                    'date': target_date.isoformat(),
                    'total_records': row[0],
                    'unique_products': row[1],
                    'avg_price': float(row[2]) if row[2] else 0.0,
                    'min_price': float(row[3]) if row[3] else 0.0,
                    'max_price': float(row[4]) if row[4] else 0.0,
                    'source_filter': source_filter
                }
            else:
                return {
                    'date': target_date.isoformat(),
                    'total_records': 0,
                    'unique_products': 0,
                    'avg_price': 0.0,
                    'min_price': 0.0,
                    'max_price': 0.0,
                    'source_filter': source_filter
                }
                
        except Exception as e:
            logger.error(f"Failed to get daily price summary: {e}")
            raise DatabaseError(
                "Failed to get daily price summary",
                {"date": target_date, "error": str(e)}
            )