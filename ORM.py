import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Numeric, DateTime, ForeignKey, text
import datetime
import os
import getpass
import sys

Base = declarative_base()

class Publisher(Base):
    __tablename__ = 'publisher'
    id = Column(Integer, primary_key=True)
    name = Column(String(length=60), unique=True, nullable=False)

class Book(Base):
    __tablename__ = 'book'
    id = Column(Integer, primary_key=True)
    title = Column(String(length=100), nullable=False)
    id_publisher = Column(Integer, ForeignKey('publisher.id'), nullable=False)
    
    publisher = relationship('Publisher', backref='books')

class Shop(Base):
    __tablename__ = 'shop'
    id = Column(Integer, primary_key=True)
    name = Column(String(length=60), unique=True, nullable=False)

class Stock(Base):
    __tablename__ = 'stock'
    id = Column(Integer, primary_key=True)
    id_book = Column(Integer, ForeignKey('book.id'), nullable=False)
    id_shop = Column(Integer, ForeignKey('shop.id'), nullable=False)
    count = Column(Integer, nullable=False)
    
    book = relationship('Book', backref='stocks')
    shop = relationship('Shop', backref='stocks')

class Sale(Base):
    __tablename__ = 'sale'
    id = Column(Integer, primary_key=True)
    price = Column(Numeric(10, 2), nullable=False)
    date_sale = Column(DateTime, default=datetime.datetime.now)
    id_stock = Column(Integer, ForeignKey('stock.id'), nullable=False)
    count = Column(Integer, nullable=False)
    
    stock = relationship('Stock', backref='sales')

def create_tables(engine):
    try:
        Base.metadata.create_all(engine)
        print("Таблицы успешно созданы")
        return True
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        return False

def create_database(dbname, user, password, host='localhost', port=5432):
    try:
        # Подключаемся к системной базе данных
        sys_engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/postgres")
        
        with sys_engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            # Проверяем существование базы данных
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{dbname}'"))
            if not result.scalar():
                conn.execute(text(f"CREATE DATABASE {dbname}"))
                print(f"База данных {dbname} создана")
            else:
                print(f"База данных {dbname} уже существует")
        return True
    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
        return False

def get_connection_params():
    print("\nНастройка подключения к PostgreSQL")
    host = input("Хост [localhost]: ") or "localhost"
    port = input("Порт [5432]: ") or "5432"
    dbname = input("Имя базы данных [netology_db]: ") or "netology_db"
    user = input("Пользователь [postgres]: ") or "postgres"
    password = getpass.getpass("Пароль: ")
    
    return {
        'host': host,
        'port': port,
        'dbname': dbname,
        'user': user,
        'password': password
    }

def insert_test_data(session):
    try:
        pub1 = Publisher(name="Эксмо")
        pub2 = Publisher(name="Дрофа")
        
        book1 = Book(title="Python для начинающих", publisher=pub1)
        book2 = Book(title="Продвинутый SQL", publisher=pub1)
        book3 = Book(title="Алгоритмы и структуры данных", publisher=pub2)
        
        shop1 = Shop(name="Букмаркет")
        shop2 = Shop(name="Книголюб")
        
        stock1 = Stock(book=book1, shop=shop1, count=10)
        stock2 = Stock(book=book2, shop=shop1, count=5)
        stock3 = Stock(book=book3, shop=shop2, count=7)
        
        sale1 = Sale(price=599.99, count=2, stock=stock1, date_sale=datetime.datetime(2023, 5, 12))
        sale2 = Sale(price=899.50, count=1, stock=stock2, date_sale=datetime.datetime(2023, 6, 3))
        sale3 = Sale(price=750.00, count=3, stock=stock3, date_sale=datetime.datetime(2023, 7, 8))
        sale4 = Sale(price=599.99, count=1, stock=stock1, date_sale=datetime.datetime(2023, 7, 10))
        
        session.add_all([pub1, pub2, book1, book2, book3, shop1, shop2, stock1, stock2, stock3, sale1, sale2, sale3, sale4])
        session.commit()
        print("Тестовые данные успешно добавлены")
    except Exception as e:
        print(f"Ошибка при добавлении тестовых данных: {e}")
        session.rollback()

def get_sales_by_publisher(session):
    publisher_input = input("\nВведите имя или ID издателя: ")
    
    try:
        # Пытаемся преобразовать ввод в число (для ID)
        publisher_id = int(publisher_input)
        condition = Publisher.id == publisher_id
    except ValueError:
        # Если не число - используем поиск по имени
        condition = Publisher.name.ilike(f"%{publisher_input}%")
    
    try:
        # Выполняем запрос
        query = session.query(
                Book.title,
                Shop.name.label("shop_name"),
                (Sale.price * Sale.count).label("total_price"),
                Sale.date_sale
            ).select_from(Publisher
            ).join(Book, Publisher.books
            ).join(Stock, Book.stocks
            ).join(Shop, Stock.shop
            ).join(Sale, Stock.sales
            ).filter(condition)
        
        results = query.all()
        
        if not results:
            print("Данные не найдены")
            return
        
        # Выводим результаты
        print("\nРезультаты:")
        print(f"{'Название книги':<30} | {'Магазин':<10} | {'Сумма':<8} | {'Дата'}")
        print("-" * 65)
        for title, shop_name, total_price, date_sale in results:
            date_formatted = date_sale.strftime("%d.%m.%Y")
            print(f"{title:<30} | {shop_name:<10} | {total_price:>8.2f} | {date_formatted}")
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")

def main():
    # Получаем параметры подключения
    params = get_connection_params()
    
    # Пытаемся создать базу данных, если её нет
    create_database(
        dbname=params['dbname'],
        user=params['user'],
        password=params['password'],
        host=params['host'],
        port=params['port']
    )
    
    # Формируем DSN
    DSN = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['dbname']}"
    
    try:
        engine = create_engine(DSN)
        print(f"\nПодключаемся к базе данных: {params['dbname']}...")
        
        # Проверяем соединение
        with engine.connect() as conn:
            print("Соединение с PostgreSQL установлено успешно!")
        
        # Создаём таблицы
        if not create_tables(engine):
            print("Не удалось создать таблицы. Проверьте настройки подключения.")
            return
        
        # Создаём сессию
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Предлагаем добавить тестовые данные
        if input("\nДобавить тестовые данные? (y/n): ").lower() == 'y':
            insert_test_data(session)
        
        # Основной цикл запросов
        while True:
            get_sales_by_publisher(session)
            if input("\nВыполнить ещё один запрос? (y/n): ").lower() != 'y':
                break
                
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()