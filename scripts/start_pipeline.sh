#!/bin/bash
# Start Pipeline - Ensures Redis is running and starts the data pipeline

set -e

echo "=========================================="
echo "Stock Data Pipeline Startup"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if Redis is installed
check_redis_installed() {
    if command -v redis-server &> /dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to check if Redis is running
check_redis_running() {
    if redis-cli ping &> /dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to start Redis
start_redis() {
    echo "Starting Redis..."
    
    # Try systemctl first (most Linux distributions)
    if command -v systemctl &> /dev/null; then
        sudo systemctl start redis-server 2>/dev/null || sudo systemctl start redis 2>/dev/null || {
            echo -e "${YELLOW}⚠️  Could not start Redis via systemctl${NC}"
            # Try running Redis directly in background
            redis-server --daemonize yes 2>/dev/null || {
                echo -e "${RED}✗ Failed to start Redis${NC}"
                return 1
            }
        }
    else
        # No systemctl, try running Redis directly
        redis-server --daemonize yes 2>/dev/null || {
            echo -e "${RED}✗ Failed to start Redis${NC}"
            return 1
        }
    fi
    
    # Wait a moment for Redis to start
    sleep 1
    
    # Verify it started
    if check_redis_running; then
        echo -e "${GREEN}✓ Redis started successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Redis failed to start${NC}"
        return 1
    fi
}

# Check if Redis is installed
echo "Checking Redis installation..."
if ! check_redis_installed; then
    echo -e "${RED}✗ Redis is not installed${NC}"
    echo ""
    echo "To install Redis:"
    echo "  Ubuntu/Debian: sudo apt update && sudo apt install redis-server"
    echo "  Fedora/RHEL:   sudo dnf install redis"
    echo "  macOS:         brew install redis"
    echo ""
    exit 1
fi
echo -e "${GREEN}✓ Redis is installed${NC}"

# Check if Redis is running
echo "Checking Redis status..."
if check_redis_running; then
    echo -e "${GREEN}✓ Redis is already running${NC}"
else
    echo -e "${YELLOW}⚠️  Redis is not running${NC}"
    start_redis || {
        echo ""
        echo -e "${RED}Failed to start Redis automatically${NC}"
        echo "Try starting it manually:"
        echo "  sudo systemctl start redis-server"
        echo "  or: redis-server --daemonize yes"
        exit 1
    }
fi

# Test Redis connection
echo "Testing Redis connection..."
REDIS_RESPONSE=$(redis-cli ping 2>&1)
if [[ "$REDIS_RESPONSE" == "PONG" ]]; then
    echo -e "${GREEN}✓ Redis connection successful${NC}"
else
    echo -e "${RED}✗ Redis connection failed: $REDIS_RESPONSE${NC}"
    exit 1
fi

# Check database configuration
echo ""
echo "Checking database configuration..."
if [[ -f .env ]]; then
    source .env
    
    if [[ -n "$DB_HOST" && -n "$DB_USER" && -n "$DB_PASSWORD" ]]; then
        echo -e "${GREEN}✓ Database configuration found${NC}"
        echo "  Host: $DB_HOST"
        echo "  Database: $DB_NAME"
        echo "  User: $DB_USER"
        echo ""
        echo -e "${YELLOW}NOTE: If this is your FIRST TIME, run this once:${NC}"
        echo "  python3 database.py"
        echo ""
        echo "This creates the database tables and partitions."
        echo "After tables exist, you never need to run it again."
    else
        echo -e "${YELLOW}⚠️  Database configuration incomplete in .env${NC}"
        echo "Please update .env with DB_HOST, DB_USER, DB_PASSWORD"
    fi
else
    echo -e "${RED}✗ .env file not found${NC}"
    exit 1
fi

echo ""
echo "=========================================="
echo "✓ All prerequisites ready!"
echo "=========================================="
echo ""
echo "You can now start the pipeline:"
echo ""
echo "Terminal 1 - Start workers:"
echo "  python pipeline_worker.py --num-workers 4"
echo ""
echo "Terminal 2 - Start file watcher:"
echo "  python pipeline_watch.py --scan-existing"
echo ""
echo "Or use direct loading:"
echo "  python loader.py"
echo ""
echo "=========================================="
