"""Quick test script for ETL pipeline"""
import asyncio
import sys
import os

# Add the backend directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def test_etl():
    try:
        from app.etl.pipeline import run_etl
        
        # Test with a simple CSV
        csv_content = b"name,age,city\nJohn,30,NYC\nJane,25,LA"
        result = await run_etl(csv_content, "test.csv")
        
        print("✅ ETL Pipeline Test Results:")
        print(f"   Records extracted: {len(result.get('structured_data', []))}")
        print(f"   Schema type: {result.get('schema', {}).get('type')}")
        print(f"   Fields: {list(result.get('schema', {}).get('fields', {}).keys())}")
        
        if len(result.get('structured_data', [])) > 0:
            print("✅ ETL is working correctly!")
            return True
        else:
            print("❌ ETL returned no data")
            return False
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Make sure pandas is installed: pip install pandas")
        return False
    except Exception as e:
        print(f"❌ ETL Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_etl())
    sys.exit(0 if success else 1)

