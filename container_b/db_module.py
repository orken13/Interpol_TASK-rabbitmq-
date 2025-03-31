import psycopg2
import psycopg2.extras
import json
from datetime import datetime

# Database configuration
DB_CONFIG = {
    "dbname": "ınterpol3",
    "user": "postgres",
    "password": "emel123",  # Change this to your actual password
    "host": "localhost",
    "port": "5432"
}


def get_connection():
    """Create and return a PostgreSQL database connection"""
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    """Initialize the database by creating the necessary tables if they don't exist"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS persons (
            entity_id TEXT PRIMARY KEY,
            name TEXT,
            sex TEXT,
            nationalities TEXT,
            date_of_birth TEXT,
            crime TEXT,
            status TEXT,
            photo_url TEXT,
            additional_data JSONB,
            first_seen TIMESTAMP,
            last_updated TIMESTAMP
        )
        ''')

        conn.commit()
        print("Veritabanı başarıyla başlatıldı.")
    except Exception as e:
        print(f"Veritabanı başlatma hatası: {e}")
    finally:
        if conn:
            conn.close()


def kontrol_db(data):
    """Check if a person with the given entity_id exists in the database"""
    conn = None
    try:
        entity_id = data.get('Entity_id')
        if not entity_id:
            return False

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM persons WHERE entity_id = %s", (entity_id,))
        count = cursor.fetchone()[0]

        return count > 0
    except Exception as e:
        print(f"Kayıt kontrolü hatası: {e}")
        return False
    finally:
        if conn:
            conn.close()


def save_to_db(data):
    """Save a new person record to the database"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        entity_id = data.get('Entity_id', '')
        name = data.get('Name', 'Belirtilmemiş')
        sex = data.get('Sex', 'Belirtilmemiş')

        nationalities = data.get('Nationalities', 'Belirtilmemiş')
        if isinstance(nationalities, list):
            nationalities = ', '.join(nationalities)

        date_of_birth = data.get('Date_of_birth', 'Belirtilmemiş')
        crime = data.get('Crime', 'Belirtilmemiş')
        status = data.get('Status', 'Belirtilmemiş')
        photo_url = data.get('Photo_url', '')

        # Extract additional data
        additional_data = {}
        for key, value in data.items():
            if key not in ['Entity_id', 'Name', 'Sex', 'Nationalities', 'Date_of_birth', 'Crime', 'Status',
                           'Photo_url']:
                additional_data[key] = value

        additional_data_json = json.dumps(additional_data)
        current_time = datetime.now()

        cursor.execute('''
        INSERT INTO persons 
        (entity_id, name, sex, nationalities, date_of_birth, crime, status, photo_url, additional_data, first_seen, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            entity_id, name, sex, nationalities, date_of_birth, crime, status, photo_url,
            additional_data_json, current_time, current_time
        ))

        conn.commit()
        print(f"Yeni kayıt eklendi: {name} - {entity_id}")
    except Exception as e:
        print(f"Veritabanına kayıt hatası: {e}")
    finally:
        if conn:
            conn.close()


def search_persons(name, surname, country):
    """Search for persons matching the given criteria"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Build query
        query = "SELECT * FROM persons WHERE 1=1"
        params = []

        # Add search criteria
        if name:
            query += " AND name ILIKE %s"
            params.append(f"%{name}%")

        if surname:
            query += " AND name ILIKE %s"
            params.append(f"%{surname}%")

        if country:
            query += " AND nationalities ILIKE %s"
            params.append(f"%{country}%")

        cursor.execute(query, params)

        results = []
        for row in cursor.fetchall():
            # Convert DictRow to regular dict
            person = dict(row)

            # Format the results as expected by the JavaScript client
            formatted_person = {
                "Entity_id": person.get("entity_id", ""),
                "Name": person.get("name", "Belirtilmemiş"),
                "Sex": person.get("sex", "Belirtilmemiş"),
                "Nationalities": person.get("nationalities", "Belirtilmemiş"),
                "Date_of_birth": person.get("date_of_birth", "Belirtilmemiş"),
                "Crime": person.get("crime", "Belirtilmemiş"),
                "Status": person.get("status", "Belirtilmemiş"),
                "Photo_url": person.get("photo_url", "")
            }

            # Add additional data
            if "additional_data" in person and person["additional_data"]:
                try:
                    if isinstance(person["additional_data"], str):
                        additional_data = json.loads(person["additional_data"])
                    else:
                        additional_data = person["additional_data"]

                    for key, value in additional_data.items():
                        formatted_person[key] = value
                except Exception as e:
                    print(f"Additional data parsing error: {e}")
                    pass

            results.append(formatted_person)

        return results
    except Exception as e:
        import logging
        logging.error(f"Arama hatası: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return []
    finally:
        if conn:
            conn.close()


def update_in_db(data):
    """Update an existing person record in the database"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        entity_id = data.get('Entity_id', '')
        name = data.get('Name', 'Belirtilmemiş')
        sex = data.get('Sex', 'Belirtilmemiş')

        nationalities = data.get('Nationalities', 'Belirtilmemiş')
        if isinstance(nationalities, list):
            nationalities = ', '.join(nationalities)

        date_of_birth = data.get('Date_of_birth', 'Belirtilmemiş')
        crime = data.get('Crime', 'Belirtilmemiş')
        status = data.get('Status', 'Belirtilmemiş')
        photo_url = data.get('Photo_url', '')

        # Extract additional data
        additional_data = {}
        for key, value in data.items():
            if key not in ['Entity_id', 'Name', 'Sex', 'Nationalities', 'Date_of_birth', 'Crime', 'Status',
                           'Photo_url']:
                additional_data[key] = value

        additional_data_json = json.dumps(additional_data)
        current_time = datetime.now()

        cursor.execute('''
        UPDATE persons 
        SET name = %s, sex = %s, nationalities = %s, date_of_birth = %s, crime = %s, status = %s, 
            photo_url = %s, additional_data = %s, last_updated = %s
        WHERE entity_id = %s
        ''', (
            name, sex, nationalities, date_of_birth, crime, status, photo_url,
            additional_data_json, current_time, entity_id
        ))

        conn.commit()
        print(f"Mevcut kayıt güncellendi: {name} - {entity_id}")
    except Exception as e:
        print(f"Veritabanı güncelleme hatası: {e}")
    finally:
        if conn:
            conn.close()


def get_all_persons():
    """Retrieve all persons from the database, ordered by last updated timestamp"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cursor.execute("SELECT * FROM persons ORDER BY last_updated DESC")
        rows = cursor.fetchall()

        result = []
        for row in rows:
            person = dict(row)

            # Handle additional_data which is now JSONB
            if isinstance(person['additional_data'], str):
                person['additional_data'] = json.loads(person['additional_data'])

            result.append(person)

        return result
    except Exception as e:
        print(f"Tüm kişileri getirme hatası: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_person_by_id(entity_id):
    """Retrieve a specific person by their entity_id"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cursor.execute("SELECT * FROM persons WHERE entity_id = %s", (entity_id,))
        row = cursor.fetchone()

        if row:
            person = dict(row)

            # Handle additional_data which is now JSONB
            if isinstance(person['additional_data'], str):
                person['additional_data'] = json.loads(person['additional_data'])

            return person
        else:
            return None
    except Exception as e:
        print(f"Kişi getirme hatası: {e}")
        return None
    finally:
        if conn:
            conn.close()