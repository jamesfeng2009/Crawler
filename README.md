# Memory Price Monitor (内存价格监控系统)

A modular system for monitoring memory prices across different e-commerce platforms with automated reporting and notifications.

## Features

- **Extensible Crawler Framework**: Easily add new e-commerce platforms
- **Data Standardization**: Normalize data from different sources into a unified format
- **Automated Scheduling**: Daily price collection and weekly report generation
- **Multi-channel Notifications**: WeChat and email notifications with charts
- **Price Analysis**: Trend analysis, week-over-week comparisons, and pattern detection
- **Robust Error Handling**: Comprehensive error handling and retry mechanisms

## Architecture

The system uses a modular, layered architecture:

- **Presentation Layer**: WeChat Bot, Email Service
- **Application Layer**: Report Generator, Chart Builder, Scheduler
- **Business Layer**: Data Analyzer, Price Comparator, Alerter
- **Data Access Layer**: Repository Pattern, Data Standardizer
- **Infrastructure Layer**: Crawler Framework, Database, Cache, HTTP Client

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd memory-price-monitor
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up configuration:
```bash
cp config.json.example config.json
cp .env.example .env
# Edit config.json and .env with your settings
```

4. Set up database:
```bash
# Create PostgreSQL database
createdb memory_price_monitor

# Install Redis (for caching)
# On Ubuntu/Debian: sudo apt-get install redis-server
# On macOS: brew install redis
```

## Configuration

The system supports configuration through:

1. **JSON Configuration File** (`config.json`)
2. **Environment Variables** (`.env` file)
3. **Runtime Configuration Reload**

Key configuration sections:
- Database connection settings
- Redis cache settings
- Crawler behavior and rate limiting
- Notification channels (WeChat, Email)
- Scheduling parameters

## Usage

### Running the Application

```bash
# Using the installed console script
memory-price-monitor

# Or directly with Python
python -m memory_price_monitor.main
```

### Adding New Crawlers

1. Create a new crawler class extending `BaseCrawler`
2. Implement required methods: `fetch_products()`, `parse_product()`, `get_price_history()`
3. Register the crawler with the `CrawlerRegistry`

Example:
```python
from memory_price_monitor.crawlers.base import BaseCrawler

class MyCrawler(BaseCrawler):
    def fetch_products(self):
        # Implementation here
        pass
    
    def parse_product(self, raw_data):
        # Implementation here
        pass
    
    def get_price_history(self, product_id):
        # Implementation here
        pass

# Register the crawler
registry.register("my_source", MyCrawler)
```

## Development

### Project Structure

```
memory_price_monitor/
├── crawlers/           # Crawler modules
├── data/              # Data models and database
├── services/          # Business logic services
├── utils/             # Utilities and helpers
└── main.py           # Application entry point

tests/                 # Test modules
config.py             # Configuration management
requirements.txt      # Python dependencies
```

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio hypothesis

# Run tests
pytest tests/

# Run with coverage
pytest --cov=memory_price_monitor tests/
```

### Code Quality

```bash
# Format code
black memory_price_monitor/ tests/

# Lint code
flake8 memory_price_monitor/ tests/

# Type checking
mypy memory_price_monitor/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.