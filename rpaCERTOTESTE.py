import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# ===================== Funções auxiliares =====================
def transform_data(row):
    """Converte registros em dicionário e limpa strings"""
    if isinstance(row, dict):
        return {k: v.strip() if isinstance(v, str) else v for k, v in row.items()}
    else:
        return {f"col_{i}": v for i, v in enumerate(row)}

def filter_columns_exist(dest_cursor, table, values):
    """Remove colunas que não existem no destino"""
    dest_cursor.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name=%s
    """, (table,))
    cols_destino = [row['column_name'] for row in dest_cursor.fetchall()]
    return {k: v for k, v in values.items() if k in cols_destino}

# ===================== Carrega .env =====================
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if load_dotenv(dotenv_path=dotenv_path, verbose=True):
    print(".env carregado com sucesso!")
else:
    print(".env NÃO foi carregado!")
    exit(1)

def get_env_var(key):
    value = os.getenv(key)
    if value:
        value = value.strip().strip('"').strip("'")
    return value

DATABASE_URL_PRIMEIRO = get_env_var("DATABASE_URL_PRIMEIRO")
DATABASE_URL_SEGUNDO = get_env_var("DATABASE_URL_SEGUNDO")

if not DATABASE_URL_PRIMEIRO or not DATABASE_URL_SEGUNDO:
    print(" Erro: variáveis de ambiente não carregadas.")
    exit(1)

# ===================== ETL sem updated_at =====================
def sync_tables(origem_url, destino_url, tabelas):
    conn_src = psycopg2.connect(origem_url, cursor_factory=RealDictCursor, sslmode="require")
    cur_src = conn_src.cursor()

    conn_dest = psycopg2.connect(destino_url, cursor_factory=RealDictCursor, sslmode="require")
    cur_dest = conn_dest.cursor()

    print(f" Sincronizando tabela(s): {tabelas}")

    for table in tabelas:
        # Buscar dados da origem
        try:
            cur_src.execute(f"SELECT * FROM {table}")
            origem_data = [transform_data(r) for r in cur_src.fetchall()]
        except psycopg2.Error as e:
            print(f" Erro ao buscar dados da tabela {table}: {e}")
            continue

        # Buscar dados do destino
        try:
            cur_dest.execute(f"SELECT * FROM {table}")
            destino_data = {r['id']: r for r in cur_dest.fetchall()}
        except psycopg2.Error as e:
            print(f" Erro ao buscar dados da tabela {table} no destino: {e}")
            continue

        for registro in origem_data:
            registro_id = registro.get("id")
            destino_registro = destino_data.get(registro_id)

            # Remove id_motorista se for None
            if registro.get("id_motorista") is None:
                registro.pop("id_motorista", None)

            # Filtra colunas que existem no destino
            registro = filter_columns_exist(cur_dest, table, registro)

            # ===================== INSERT =====================
            if not destino_registro:
                try:
                    cols = ', '.join(registro.keys())
                    vals = ', '.join(['%s'] * len(registro))
                    sql = f"INSERT INTO {table} ({cols}) VALUES ({vals})"
                    cur_dest.execute(sql, list(registro.values()))
                    conn_dest.commit()
                    print(f" INSERT aplicado em {table}: {registro}")
                except psycopg2.Error as e:
                    conn_dest.rollback()
                    print(f" Erro ao processar INSERT em {table}: {e}")
                continue

            # ===================== UPDATE =====================
            # Só atualiza se houver diferença real
            dif = {k: v for k, v in registro.items() if destino_registro.get(k) != v}
            if dif:
                try:
                    set_clause = ', '.join([f"{k}=%s" for k in dif.keys()])
                    sql = f"UPDATE {table} SET {set_clause} WHERE id=%s"
                    cur_dest.execute(sql, list(dif.values()) + [registro_id])
                    conn_dest.commit()
                    print(f" UPDATE aplicado em {table}: {dif}")
                except psycopg2.Error as e:
                    conn_dest.rollback()
                    print(f" Erro ao processar UPDATE em {table}: {e}")

        # DELETE 
        origem_ids = set(r['id'] for r in origem_data)
        for id_destino in destino_data.keys():
            if id_destino not in origem_ids:
                try:
                    cur_dest.execute(f"DELETE FROM {table} WHERE id=%s", (id_destino,))
                    conn_dest.commit()
                    print(f" DELETE aplicado em {table}: id={id_destino}")
                except psycopg2.Error as e:
                    conn_dest.rollback()
                    print(f" Erro ao processar DELETE em {table}: {e}")

#  Executa sincronização (mudar o nome da tabela, para a tabela desejada)
sync_tables(DATABASE_URL_PRIMEIRO, DATABASE_URL_SEGUNDO, ["empresa"])
