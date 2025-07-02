import aiohttp
import asyncio
import asyncpg
from datetime import datetime
import sys
import ssl

# Конфигурация подключения к PostgreSQL
DB_CONFIG = {
    'user': 'postgres',
    'password': '******',
    'database': 'starwars',
    'host': '127.0.0.1',
    'port': 5432,
    'ssl': False  # Отключаем SSL для локального PostgreSQL
}

BASE_URL = "https://swapi.dev/api/people/"


async def test_connection():
    """Проверка подключения к БД"""
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        print("Успешное подключение к PostgreSQL")
        await conn.close()
        return True
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return False


async def get_db_connection():
    """Установка подключения к БД с повторными попытками"""
    for attempt in range(3):
        try:
            return await asyncpg.connect(**DB_CONFIG)
        except Exception as e:
            if attempt == 2:
                raise
            print(f"Попытка {attempt + 1} не удалась, повтор...")
            await asyncio.sleep(1)


async def fetch_character(session, url):
    """Получение данных персонажа"""
    try:
        async with session.get(url) as response:
            return await response.json() if response.status == 200 else None
    except Exception as e:
        print(f"Ошибка при получении {url}: {e}")
        return None


async def fetch_all_characters(session):
    """Получение всех персонажей"""
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)

        async with aiohttp.ClientSession(connector=connector) as secure_session:
            async with secure_session.get(BASE_URL) as response:
                data = await response.json()
                total = data['count']
                print(f"Найдено {total} персонажей")

                tasks = [fetch_character(secure_session, f"{BASE_URL}{i}/") for i in range(1, total + 1)]
                results = await asyncio.gather(*tasks)
                return [char for char in results if char]
    except Exception as e:
        print(f"Ошибка при получении списка персонажей: {e}")
        return []


async def process_character(session, character):
    """Обработка данных персонажа"""
    try:
        char_id = int(character['url'].rstrip('/').split('/')[-1])

        async def fetch_names(urls):
            if not urls:
                return ""
            tasks = [fetch_character(session, url) for url in urls]
            results = await asyncio.gather(*tasks)
            return ", ".join([res['name'] for res in results if res and 'name' in res])

        films, species, starships, vehicles, homeworld = await asyncio.gather(
            fetch_names(character['films']),
            fetch_names(character['species']),
            fetch_names(character['starships']),
            fetch_names(character['vehicles']),
            fetch_names([character['homeworld']]) if character.get('homeworld') else ""
        )

        return {
            'id': char_id,
            'name': character['name'],
            'birth_year': character.get('birth_year', ''),
            'eye_color': character.get('eye_color', ''),
            'gender': character.get('gender', ''),
            'hair_color': character.get('hair_color', ''),
            'height': character.get('height', ''),
            'homeworld': homeworld,
            'mass': character.get('mass', ''),
            'skin_color': character.get('skin_color', ''),
            'films': films,
            'species': species,
            'starships': starships,
            'vehicles': vehicles
        }
    except Exception as e:
        print(f"Ошибка обработки персонажа: {e}")
        return None


async def save_characters(conn, characters):
    """Сохранение персонажей в БД"""
    try:
        for char in characters:
            if char:
                await conn.execute('''
                    INSERT INTO characters VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
                    ) ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        birth_year = EXCLUDED.birth_year,
                        eye_color = EXCLUDED.eye_color,
                        gender = EXCLUDED.gender,
                        hair_color = EXCLUDED.hair_color,
                        height = EXCLUDED.height,
                        homeworld = EXCLUDED.homeworld,
                        mass = EXCLUDED.mass,
                        skin_color = EXCLUDED.skin_color,
                        films = EXCLUDED.films,
                        species = EXCLUDED.species,
                        starships = EXCLUDED.starships,
                        vehicles = EXCLUDED.vehicles
                ''', *[char[k] for k in [
                    'id', 'name', 'birth_year', 'eye_color', 'gender',
                    'hair_color', 'height', 'homeworld', 'mass',
                    'skin_color', 'films', 'species', 'starships', 'vehicles'
                ]])
        return True
    except Exception as e:
        print(f"Ошибка сохранения: {e}")
        return False


async def main():
    print("Запуск загрузки данных Star Wars")
    start_time = datetime.now()

    if not await test_connection():
        sys.exit(1)

    try:
        conn = await get_db_connection()

        # Создаем сессию с отключенной проверкой SSL
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        connector = aiohttp.TCPConnector(ssl=ssl_context)

        async with aiohttp.ClientSession(connector=connector) as session:
            characters = await fetch_all_characters(session)
            if not characters:
                print("Персонажи не найдены")
                return

            print(f"Обработка {len(characters)} персонажей...")
            processed = [await process_character(session, char) for char in characters]
            valid_chars = [char for char in processed if char]

            if await save_characters(conn, valid_chars):
                print(f"Успешно сохранено {len(valid_chars)} персонажей")
            else:
                print("Ошибка при сохранении данных")

    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        if 'conn' in locals():
            await conn.close()
        print(f"Общее время: {datetime.now() - start_time}")


if __name__ == '__main__':
    asyncio.run(main())