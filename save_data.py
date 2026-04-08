import functools
import requests
import sqlite3


LINK = 'https://jsonplaceholder.typicode.com/'
TABLES = ('users', 'posts', 'comments')


def transform_user_data(user):
    """Преобразует вложенные данные пользователя из API в словарь."""
    return {
        'id': user['id'],
        'name': user['name'],
        'username': user['username'],
        'email': user['email'],
        'phone': user['phone'],
        'website': user['website'],
        'street': user['address']['street'],
        'suite': user['address']['suite'],
        'city': user['address']['city'],
        'zipcode': user['address']['zipcode'],
        'geo_lat': user['address']['geo']['lat'],
        'geo_lng': user['address']['geo']['lng'],
        'company_name': user['company']['name'],
        'company_catchphrase': user['company']['catchPhrase'],
        'company_bs': user['company']['bs'],
    }


def validate_table_name(func):
    """Валидирует имя таблицы через белый список."""
    @functools.wraps(func)
    def wrapper(table_name: str, *args, **kwargs):
        if table_name not in TABLES:
            raise ValueError(f'Неправильное имя таблицы {table_name}!')
        return func(table_name, *args, **kwargs)
    return wrapper


@validate_table_name
def fetch_data(table_name: str) -> list[dict]:
    """Загружает данные таблицы по ссылке."""
    response = requests.get(LINK + table_name)
    return response.json()


@validate_table_name
def get_columns_from_db(table_name, cursor):
    """Получить список колонок из таблицы БД."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


@validate_table_name
def load_data(table_name: str, cursor, data, transform=None):
    """Находит новые данные, трансформирует и загружает в БД."""
    cursor.execute(f'SELECT id FROM {table_name}')
    existing_ids = {row[0] for row in cursor.fetchall()}
    new_items = [item for item in data if item['id'] not in existing_ids]

    columns = get_columns_from_db(table_name, cursor)

    if new_items and transform:
        new_items = [transform(item) for item in new_items]

    if new_items:
        placeholders = ','.join('?' for _ in columns)
        for item in new_items:
            values = [item[col] for col in columns]
            cursor.execute(
                (
                    f'INSERT INTO {table_name} '
                    f'({",".join(columns)}) '
                    f'VALUES ({placeholders})'
                ),
                values
            )

    print(f'{table_name}: добавлено {len(new_items)} записей')


if __name__ == '__main__':
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()

    cursor.executescript('''
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY,
        name TEXT,
        username TEXT,
        email TEXT,
        phone TEXT,
        website TEXT,
        street TEXT,
        suite TEXT,
        city TEXT,
        zipcode TEXT,
        geo_lat TEXT,
        geo_lng TEXT,
        company_name TEXT,
        company_catchphrase TEXT,
        company_bs TEXT
    );

    CREATE TABLE IF NOT EXISTS posts(
        id INTEGER PRIMARY KEY,
        userId INTEGER,
        title TEXT,
        body TEXT,
        FOREIGN KEY(userId) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS comments(
        id INTEGER PRIMARY KEY,
        postId INTEGER,
        name TEXT,
        email TEXT,
        body TEXT,
        FOREIGN KEY(postId) REFERENCES posts(id)
    )
    ''')

    for table_name in TABLES:
        data = fetch_data(table_name)
        load_data(
            table_name, cursor, data,
            transform=transform_user_data if table_name == 'users' else None
        )

    conn.commit()
    conn.close()
