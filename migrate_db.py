import asyncpg
from asyncpg import Connection
from datetime import datetime
import asyncio
import sys


async def create_connection():
    """Create a connection to PostgreSQL database"""
    try:
        conn = await asyncpg.connect(
            user='postgres',
            password='*****',
            database='postgres',
            host='127.0.0.1',
            port=5432
        )
        print("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"Connection error: {e}")
        sys.exit(1)


async def create_database(conn: Connection):
    """Create database if not exists"""
    try:
        db_exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = 'starwars'"
        )

        if not db_exists:
            await conn.execute("CREATE DATABASE starwars")
            print("Database 'starwars' created successfully")
        else:
            print("Database 'starwars' already exists")
    except Exception as e:
        print(f"Error creating database: {e}")
        raise


async def create_table(conn: Connection):
    """Create characters table if it doesn't exist"""
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS characters (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                birth_year TEXT,
                eye_color TEXT,
                gender TEXT,
                hair_color TEXT,
                height TEXT,
                homeworld TEXT,
                mass TEXT,
                skin_color TEXT,
                films TEXT,
                species TEXT,
                starships TEXT,
                vehicles TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        print("Created characters table")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise


async def create_update_trigger(conn: Connection):
    """Create trigger to update updated_at timestamp"""
    try:
        await conn.execute('''
            CREATE OR REPLACE FUNCTION update_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        ''')

        await conn.execute('''
            CREATE OR REPLACE TRIGGER update_characters_timestamp
            BEFORE UPDATE ON characters
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp()
        ''')
        print("Created update timestamp trigger")
    except Exception as e:
        print(f"Error creating trigger: {e}")
        raise


async def main():
    start_time = datetime.now()
    print("Starting migration at", start_time)

    try:
        admin_conn = await create_connection()
        await create_database(admin_conn)
        await admin_conn.close()

        conn = await asyncpg.connect(
            user='postgres',
            password='********',
            database='starwars',
            host='127.0.0.1',
            port=5432
        )

        await create_table(conn)
        await create_update_trigger(conn)

        print("Migration completed successfully")
    except Exception as e:
        print(f"Migration failed: {e}")
    finally:
        if 'conn' in locals():
            await conn.close()

    end_time = datetime.now()
    print("Finished migration at", end_time)
    print("Total time:", end_time - start_time)


if __name__ == '__main__':
    asyncio.run(main())