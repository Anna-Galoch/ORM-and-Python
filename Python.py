import psycopg2
from psycopg2 import sql

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones (
                phone_id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
                phone_number VARCHAR(20) UNIQUE
            );
        """)
        conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name, email)
            VALUES (%s, %s, %s)
            RETURNING client_id;
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        
        if phones:
            for phone in phones:
                cur.execute("""
                    INSERT INTO phones (client_id, phone_number)
                    VALUES (%s, %s);
                """, (client_id, phone))
        conn.commit()

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phones (client_id, phone_number)
            VALUES (%s, %s);
        """, (client_id, phone))
        conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        update_fields = []
        params = []
        
        if first_name:
            update_fields.append("first_name = %s")
            params.append(first_name)
        if last_name:
            update_fields.append("last_name = %s")
            params.append(last_name)
        if email:
            update_fields.append("email = %s")
            params.append(email)
            
        if update_fields:
            query = sql.SQL("UPDATE clients SET {} WHERE client_id = %s").format(
                sql.SQL(', ').join(map(sql.SQL, update_fields))
            )
            params.append(client_id)
            cur.execute(query, params)
        
        if phones is not None:
            cur.execute("DELETE FROM phones WHERE client_id = %s", (client_id,))
            for phone in phones:
                cur.execute("""
                    INSERT INTO phones (client_id, phone_number)
                    VALUES (%s, %s);
                """, (client_id, phone))
        conn.commit()

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phones
            WHERE client_id = %s AND phone_number = %s;
        """, (client_id, phone))
        conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM clients
            WHERE client_id = %s;
        """, (client_id,))
        conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        conditions = []
        params = []
        
        if first_name:
            conditions.append("c.first_name = %s")
            params.append(first_name)
        if last_name:
            conditions.append("c.last_name = %s")
            params.append(last_name)
        if email:
            conditions.append("c.email = %s")
            params.append(email)
        if phone:
            conditions.append("p.phone_number = %s")
            params.append(phone)
        
        if not conditions:
            return None
            
        query = sql.SQL("""
            SELECT DISTINCT c.client_id, c.first_name, c.last_name, c.email
            FROM clients c
            LEFT JOIN phones p ON c.client_id = p.client_id
            WHERE {}
        """).format(sql.SQL(' OR ').join(map(sql.SQL, conditions)))
        
        cur.execute(query, params)
        return cur.fetchall()


with psycopg2.connect(database="Anna", user="postgres", password="Mingwei11") as conn:
    create_db(conn)
    
    add_client(conn, "Анна", "Г", "anna-g@mail.ru", ["1234567890"])
    add_client(conn, "Гарри", "Поттер", "HP@hogwarts.ru")
    
    add_phone(conn, 1, "0987654321")
    add_phone(conn, 2, "5555555555")
    
    change_client(conn, 1, first_name="Рон", phones=["1111111111", "2222222222"])
    
    delete_phone(conn, 2, "5555555555")
    
    print(find_client(conn, first_name="Рон"))
    print(find_client(conn, phone="1111111111"))
    
    delete_client(conn, 2)

conn.close()
