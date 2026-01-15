from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import pandas as pd
import sqlite3, psycopg2, os, io, pytz, json
from dateutil.relativedelta import relativedelta
try:
    import psycopg2
except ImportError:
    psycopg2 = None

app = Flask(__name__)
app.secret_key = "consigtech_secret_2025"

DATABASE_URL = os.environ.get("DATABASE_URL", "").replace("postgres://", "postgresql://")
LOCAL_DB = "local.db"

@app.template_filter('brl')
def format_brl(value):
    try:
        valor = float(value or 0)
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

def get_conn():
    if DATABASE_URL and psycopg2:
        return psycopg2.connect(DATABASE_URL, sslmode="require")
    else:
        return sqlite3.connect(LOCAL_DB, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    if isinstance(conn, sqlite3.Connection):
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE NOT NULL,
            senha TEXT NOT NULL,
            role TEXT DEFAULT 'user'
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS propostas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            consultor TEXT,
            fonte TEXT,
            banco TEXT,
            senha_digitada TEXT,
            tabela TEXT,
            nome_cliente TEXT,
            cpf TEXT,
            valor_equivalente REAL,
            valor_original REAL,
            observacao TEXT,
            telefone TEXT
        )""")
    else:
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            nome TEXT UNIQUE NOT NULL,
            senha TEXT NOT NULL,
            role TEXT DEFAULT 'user'
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS propostas (
            id SERIAL PRIMARY KEY,
            data TIMESTAMP,
            consultor TEXT,
            fonte TEXT,
            banco TEXT,
            senha_digitada TEXT,
            tabela TEXT,
            nome_cliente TEXT,
            cpf TEXT,
            valor_equivalente NUMERIC(12,2),
            valor_original NUMERIC(12,2),
            observacao TEXT,
            telefone TEXT
        )""")

    cur.execute("SELECT * FROM users WHERE nome = ?" if isinstance(conn, sqlite3.Connection)
                else "SELECT * FROM users WHERE nome = %s", ("admin",))
    if not cur.fetchone():
        senha_hash = generate_password_hash("Tech@2025")
        cur.execute("INSERT INTO users (nome, senha, role) VALUES (?, ?, ?)" if isinstance(conn, sqlite3.Connection)
                    else "INSERT INTO users (nome, senha, role) VALUES (%s, %s, %s)",
                    ("admin", senha_hash, "admin"))
    conn.commit()
    conn.close()

init_db()

def ensure_banco_column():
    conn = get_conn()
    cur = conn.cursor()

    try:
        if isinstance(conn, sqlite3.Connection):
            cur.execute("PRAGMA table_info(propostas);")
            colunas = [col[1] for col in cur.fetchall()]

            if "banco" not in colunas:
                print("🛠️ Adicionando coluna 'banco' no SQLite...")
                cur.execute("ALTER TABLE propostas ADD COLUMN banco TEXT;")

            if "produto" not in colunas:
                print("🛠️ Adicionando coluna 'produto' no SQLite...")
                cur.execute("ALTER TABLE propostas ADD COLUMN produto TEXT;")

            if "valor_parcela" not in colunas:
                print("🛠️ Adicionando coluna 'valor_parcela' no SQLite...")
                cur.execute("ALTER TABLE propostas ADD COLUMN valor_parcela REAL;")

            if "quantidade_parcelas" not in colunas:
                print("🛠️ Adicionando coluna 'quantidade_parcelas' no SQLite...")
                cur.execute("ALTER TABLE propostas ADD COLUMN quantidade_parcelas INTEGER;")

            if "data_pagamento_prevista" not in colunas:
                print("🛠️ Adicionando coluna 'data_pagamento_prevista' no SQLite...")
                cur.execute("ALTER TABLE propostas ADD COLUMN data_pagamento_prevista TEXT;")

            conn.commit()
            print("✅ Colunas garantidas no SQLite.")

        else:
            def add_col_pg(col, col_type):
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'propostas' AND column_name = %s;
                """, (col,))
                if not cur.fetchone():
                    print(f"🛠️ Adicionando coluna '{col}' no PostgreSQL...")
                    cur.execute(f"ALTER TABLE propostas ADD COLUMN {col} {col_type};")

            add_col_pg("banco", "TEXT")
            add_col_pg("produto", "TEXT")
            add_col_pg("valor_parcela", "NUMERIC(12,2)")
            add_col_pg("quantidade_parcelas", "INTEGER")
            add_col_pg("data_pagamento_prevista", "TEXT")

            conn.commit()
            print("✅ Colunas garantidas no PostgreSQL.")

    except Exception as e:
        print("⚠️ Erro ao garantir colunas em 'propostas':", e)
    finally:
        conn.close()

def ensure_meta_table():
    conn = get_conn()
    cur = conn.cursor()
    if isinstance(conn, sqlite3.Connection):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS metas_globais (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                valor REAL
            )
        """)
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS metas_globais (
                id SERIAL PRIMARY KEY,
                valor NUMERIC(12,2)
            )
        """)
    conn.commit()
    conn.close()

ensure_meta_table()
ensure_banco_column()

@app.route("/")
def home():
    if "user" in session:
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))

@app.route("/indice_dia")
def indice_dia():
    if "user" not in session:
        return redirect(url_for("login"))

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metas_individuais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            consultor TEXT UNIQUE,
            meta REAL
        )
    """ if isinstance(conn, sqlite3.Connection) else """
        CREATE TABLE IF NOT EXISTS metas_individuais (
            id SERIAL PRIMARY KEY,
            consultor TEXT UNIQUE,
            meta NUMERIC(12,2)
        )
    """)

    cur.execute("SELECT consultor, meta FROM metas_individuais;")
    metas_dict = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT nome FROM users WHERE role != 'admin' ORDER BY nome;")
    todos_usuarios = [r[0] for r in cur.fetchall()]

    tz = pytz.timezone("America/Sao_Paulo")
    hoje = datetime.now(tz).strftime("%Y-%m-%d")

    if isinstance(conn, sqlite3.Connection):
        cur.execute("""
            SELECT consultor,
                   COALESCE(SUM(valor_equivalente), 0) AS total_eq,
                   COALESCE(SUM(valor_original), 0) AS total_or
            FROM propostas
            WHERE DATE(data, 'localtime') = ?
            GROUP BY consultor;
        """, (hoje,))
    else:
        cur.execute("""
            SELECT consultor,
                   COALESCE(SUM(valor_equivalente), 0) AS total_eq,
                   COALESCE(SUM(valor_original), 0) AS total_or
            FROM propostas
            WHERE DATE(data AT TIME ZONE 'America/Sao_Paulo') = %s
            GROUP BY consultor;
        """, (hoje,))

    resultados = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    cur.execute("""
        SELECT consultor,
               COALESCE(SUM(valor_equivalente), 0) AS eq_total
        FROM propostas
        GROUP BY consultor;
    """)
    totais = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta_dia (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            valor REAL
        )
    """ if isinstance(conn, sqlite3.Connection) else """
        CREATE TABLE IF NOT EXISTS meta_dia (
            id SERIAL PRIMARY KEY,
            valor NUMERIC(12,2)
        )
    """)
    cur.execute("SELECT valor FROM meta_dia ORDER BY id DESC LIMIT 1;")
    meta_dia_row = cur.fetchone()
    meta_dia = meta_dia_row[0] if meta_dia_row else 0

    conn.close()

    ranking = []
    for nome in todos_usuarios:
        eq_dia, or_dia = resultados.get(nome, (0, 0))
        meta = metas_dict.get(nome, 0)
        eq_total = totais.get(nome, 0)
        falta = max(meta - eq_total, 0)
        ranking.append([nome, eq_dia, or_dia, meta, falta])

    ranking.sort(key=lambda x: x[1], reverse=True)

    total_eq = sum(r[1] for r in ranking)
    total_or = sum(r[2] for r in ranking)

    falta_meta_dia = max(meta_dia - total_eq, 0)

    return render_template(
        "indice_dia.html",
        ranking=ranking,
        total_eq=total_eq,
        total_or=total_or,
        meta_dia=meta_dia,
        falta_meta_dia=falta_meta_dia,
        data_atual=hoje
    )

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        nome = request.form["nome"]
        senha = request.form["senha"]
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT nome, senha, role FROM users WHERE nome = ?" if isinstance(conn, sqlite3.Connection)
                    else "SELECT nome, senha, role FROM users WHERE nome = %s", (nome,))
        user = cur.fetchone()
        conn.close()
        if user and check_password_hash(user[1], senha):
            session["user"], session["role"] = user[0], user[2]
            return redirect(url_for("dashboard"))
        return render_template("login.html", erro="Usuário ou senha incorretos.")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/nova_proposta", methods=["GET", "POST"])
def nova_proposta():
    if "user" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        tz_br = pytz.timezone("America/Sao_Paulo")
        data_input = request.form.get("data_manual")

        if data_input:
            try:
                data_obj = datetime.strptime(data_input, "%Y-%m-%dT%H:%M")
                data_obj = tz_br.localize(data_obj)
                data_formatada = data_obj.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                print("Erro ao processar data_manual:", e)
                data_formatada = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")
        else:
            data_formatada = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

        # DADOS NA MESMA ORDEM DO INSERT
        dados = (
            data_formatada,
            session["user"],
            request.form.get("fonte"),
            request.form.get("banco"),
            request.form.get("senha_digitada"),
            request.form.get("tabela"),
            request.form.get("nome_cliente"),
            request.form.get("cpf"),
            request.form.get("valor_equivalente") or 0,
            request.form.get("valor_original") or 0,
            request.form.get("observacao"),
            request.form.get("telefone"),
            request.form.get("produto"),
            request.form.get("valor_parcela"),
            request.form.get("quantidade_parcelas"),
            request.form.get("data_pagamento_prevista")
        )

        conn = get_conn()
        cur = conn.cursor()
        ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

        cur.execute(f"""
            INSERT INTO propostas 
            (
                data, consultor, fonte, banco, senha_digitada, tabela,
                nome_cliente, cpf, valor_equivalente, valor_original,
                observacao, telefone, produto, valor_parcela,
                quantidade_parcelas, data_pagamento_prevista
            )
            VALUES ({','.join([ph]*16)})
        """, dados)

        conn.commit()
        conn.close()
        return render_template("nova_proposta.html", sucesso="Proposta enviada com sucesso!")

    return render_template("nova_proposta.html")

@app.route("/relatorios", methods=["GET", "POST"])
def relatorios():
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))   

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT consultor
        FROM propostas
        WHERE consultor IS NOT NULL
        AND consultor NOT IN (SELECT nome FROM users WHERE role = 'admin')
        ORDER BY consultor;
    """)
    usuarios = [u[0] for u in cur.fetchall()]

    user = request.form.get("usuario") or request.args.get("usuario")
    data_ini = request.form.get("data_ini") or request.args.get("data_ini")
    data_fim = request.form.get("data_fim") or request.args.get("data_fim")
    observacao = (request.form.get("observacao") or request.args.get("observacao") or "").strip()
    senha_digitada = (request.form.get("senha_digitada") or request.args.get("senha_digitada") or "").strip()
    fonte = (request.form.get("fonte") or request.args.get("fonte") or "").strip()
    tabela = (request.form.get("tabela") or request.args.get("tabela") or "").strip()
    banco = (request.form.get("banco") or request.args.get("banco") or "").strip()
    cpf = (request.form.get("cpf") or request.args.get("cpf") or "").strip()
    mes = request.form.get("mes") or request.args.get("mes")
    ano = request.form.get("ano") or request.args.get("ano")

    acao = request.form.get("acao")

    if acao == "limpar":
        return redirect(url_for("relatorios"))

    if acao == "filtrar":
        return redirect(url_for(
            "relatorios",
            usuario=None if not user else user,
            data_ini=data_ini or "",
            data_fim=data_fim or "",
            mes=mes or "",
            ano=ano or "",
            observacao=observacao or "",
            senha_digitada=senha_digitada or "",
            fonte=fonte or "",
            tabela=tabela or "",
            banco=banco or "",
            cpf=cpf or "",
            pagina=1
        ))

    def normalizar_data(data_str):
        if not data_str:
            return None
        try:
            return datetime.strptime(data_str, "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return data_str

    data_ini = normalizar_data(data_ini)
    data_fim = normalizar_data(data_fim)

    ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    query_base = f"""
        SELECT id, data, consultor, fonte, banco, senha_digitada, tabela, nome_cliente, cpf,
               valor_equivalente, valor_original, observacao, telefone, valor_parcela, quantidade_parcelas, data_pagamento_prevista
        FROM propostas
    """

    condicoes, params = [], []

    condicoes.append("consultor NOT IN (SELECT nome FROM users WHERE role = 'admin')")

    def filtro_lower(campo, valor):
        if isinstance(conn, sqlite3.Connection):
            return f"LOWER({campo}) LIKE LOWER({ph})", f"%{valor.lower()}%"
        else:
            return f"LOWER({campo}) LIKE LOWER({ph})", f"%{valor}%"

    if user and user.strip() and user != "-":
        condicoes.append(f"LOWER(consultor) = LOWER({ph})")
        params.append(user)

    if data_ini and data_fim:
        condicoes.append(f"data BETWEEN {ph} AND {ph}")
        params += [data_ini, data_fim]
        mes_atual = "Filtro por período"

    elif cpf:
        filtro, valor = filtro_lower("cpf", cpf)
        condicoes.append(filtro)
        params.append(valor)
        mes_atual = "Filtro por CPF"

    elif mes or ano:
        if not ano:
            ano = datetime.now().year

        if not mes:
            inicio = f"{ano}-01-01 00:00:00"
            fim = f"{ano}-12-31 23:59:59"
            mes_atual = f"Ano {ano}"
        else:
            inicio = f"{ano}-{mes}-01 00:00:00"
            inicio_dt = datetime.strptime(inicio, "%Y-%m-%d %H:%M:%S")
            fim_dt = inicio_dt + relativedelta(months=1) - timedelta(seconds=1)
            fim = fim_dt.strftime("%Y-%m-%d %H:%M:%S")
            mes_atual = f"{mes}/{ano}"

        condicoes.append(f"data BETWEEN {ph} AND {ph}")
        params += [inicio, fim]

    else:
        agora = datetime.now()
        inicio_mes = agora.replace(day=1, hour=0, minute=0, second=0)
        proximo_mes = inicio_mes + relativedelta(months=1)
        fim_mes = proximo_mes - timedelta(seconds=1)

        condicoes.append(f"data BETWEEN {ph} AND {ph}")
        params += [
            inicio_mes.strftime("%Y-%m-%d %H:%M:%S"),
            fim_mes.strftime("%Y-%m-%d %H:%M:%S")
        ]

        meses_pt = {
            "January": "Janeiro", "February": "Fevereiro", "March": "Março",
            "April": "Abril", "May": "Maio", "June": "Junho", "July": "Julho",
            "August": "Agosto", "September": "Setembro", "October": "Outubro",
            "November": "Novembro", "December": "Dezembro"
        }
        mes_nome = inicio_mes.strftime("%B")
        mes_atual = f"{meses_pt[mes_nome]}/{inicio_mes.year}"

    if observacao:
        filtro, valor = filtro_lower("observacao", observacao)
        condicoes.append(filtro)
        params.append(valor)

    if senha_digitada:
        filtro, valor = filtro_lower("senha_digitada", senha_digitada)
        condicoes.append(filtro)
        params.append(valor)

    if fonte:
        filtro, valor = filtro_lower("fonte", fonte)
        condicoes.append(filtro)
        params.append(valor)

    if banco:
        filtro, valor = filtro_lower("banco", banco)
        condicoes.append(filtro)
        params.append(valor)

    if tabela:
        filtro, valor = filtro_lower("tabela", tabela)
        condicoes.append(filtro)
        params.append(valor)

    if condicoes:
        query_base += " WHERE " + " AND ".join(condicoes)

    order_clause = "ORDER BY datetime(data) DESC" if isinstance(conn, sqlite3.Connection) else "ORDER BY data DESC"

    pagina = int(request.args.get("pagina", 1))
    por_pagina = 50
    offset = (pagina - 1) * por_pagina

    cur.execute(f"SELECT COUNT(*) FROM ({query_base})", tuple(params))
    total_registros = cur.fetchone()[0]
    total_paginas = (total_registros + por_pagina - 1) // por_pagina

    query_paginada = f"{query_base} {order_clause} LIMIT {ph} OFFSET {ph}"
    params_paginada = params + [por_pagina, offset]

    cur.execute(
        query_paginada.replace("?", "%s") if not isinstance(conn, sqlite3.Connection) else query_paginada,
        tuple(params_paginada)
    )
    dados = cur.fetchall()

    cur.execute(
        f"SELECT COALESCE(SUM(valor_equivalente),0), COALESCE(SUM(valor_original),0) FROM ({query_base})",
        tuple(params)
    )
    total_equivalente, total_original = cur.fetchone()
    total_propostas = total_registros

    cur.execute("SELECT valor FROM metas_globais ORDER BY id DESC LIMIT 1;")
    meta_row = cur.fetchone()
    meta_global = float(meta_row[0]) if meta_row else 0.0

    if user:
        cur.execute(
            "SELECT meta FROM metas_individuais WHERE consultor = %s;"
            if not isinstance(conn, sqlite3.Connection)
            else "SELECT meta FROM metas_individuais WHERE consultor = ?;",
            (user,),
        )
        meta_individual_row = cur.fetchone()
        meta_individual = float(meta_individual_row[0]) if meta_individual_row else meta_global
        falta_para_meta = max(meta_individual - float(total_equivalente or 0), 0)
    else:
        falta_para_meta = max(meta_global - float(total_equivalente or 0), 0)

    if acao == "baixar":
        colunas = ["ID", "Data", "Consultor", "Fonte", "Banco", "Senha Digitada", "Tabela", "Nome", "CPF",
                   "Valor Equivalente", "Valor Original", "Observação", "Telefone"]
        df = pd.DataFrame(dados, columns=colunas)
        output = io.BytesIO()
        df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)
        filename = f"Relatorio_{user or 'Todos'}_{datetime.now().strftime('%d-%m_%Hh%M')}.xlsx"
        conn.close()
        return send_file(output, as_attachment=True, download_name=filename)

    conn.close()

    return render_template(
        "relatorios.html",
        usuarios=usuarios,
        dados=dados,
        user=user,
        data_ini=data_ini,
        data_fim=data_fim,
        observacao=observacao,
        senha_digitada=senha_digitada,
        fonte=fonte,
        tabela=tabela,
        banco=banco,
        cpf=cpf,
        total_equivalente=total_equivalente,
        total_original=total_original,
        total_propostas=total_propostas,
        falta_para_meta=falta_para_meta,
        pagina=pagina,
        total_paginas=total_paginas,
        mes_atual=mes_atual
    )

from datetime import datetime, timedelta

@app.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect(url_for("login"))

    periodo = request.args.get("periodo")
    inicio = request.args.get("inicio")
    fim = request.args.get("fim")

    agora = datetime.now()

    if inicio and fim:
        try:
            inicio = datetime.strptime(inicio, "%Y-%m-%d").strftime("%Y-%m-%d")
            fim = datetime.strptime(fim, "%Y-%m-%d").strftime("%Y-%m-%d")
        except:
            inicio = agora.replace(day=1).strftime("%Y-%m-%d")
            fim = agora.strftime("%Y-%m-%d")
    else:
        if periodo == "hoje":
            inicio = fim = agora.strftime("%Y-%m-%d")
        elif periodo == "ultima_semana":
            inicio = (agora - timedelta(days=7)).strftime("%Y-%m-%d")
            fim = agora.strftime("%Y-%m-%d")
        elif periodo == "ultimo_mes":
            mes_passado = (agora.replace(day=1) - timedelta(days=1))
            inicio = mes_passado.replace(day=1).strftime("%Y-%m-%d")
            fim = mes_passado.strftime("%Y-%m-%d")
        elif periodo == "tudo":
            inicio, fim = "1900-01-01", "2100-01-01"
        else:
            inicio = agora.replace(day=1).strftime("%Y-%m-%d")
            fim = agora.strftime("%Y-%m-%d")

    conn = get_conn()
    cur = conn.cursor()
    ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    if isinstance(conn, sqlite3.Connection):
        filtro_data = f"date(data) BETWEEN {ph} AND {ph}"
    else:
        filtro_data = f"DATE(data AT TIME ZONE 'America/Sao_Paulo') BETWEEN {ph} AND {ph}"

    cur.execute(f"""
        SELECT SUM(valor_equivalente), SUM(valor_original), COUNT(*)
        FROM propostas
        WHERE {filtro_data}
    """, (inicio, fim))
    total_eq, total_or, total_propostas = cur.fetchone() or (0, 0, 0)

    cur.execute("SELECT valor FROM metas_globais ORDER BY id DESC LIMIT 1;")
    meta_row = cur.fetchone()
    meta_global = meta_row[0] if meta_row else 0
    falta_meta = max(meta_global - (total_eq or 0), 0)

    cur.execute(f"""
        SELECT consultor, SUM(valor_equivalente) AS total
        FROM propostas
        WHERE {filtro_data}
        GROUP BY consultor
        ORDER BY total DESC
        LIMIT 3;
    """, (inicio, fim))
    ranking = cur.fetchall()

    cur.execute(f"""
        SELECT banco, COUNT(*) AS total_propostas, COALESCE(SUM(valor_equivalente), 0) AS total_valor
        FROM propostas
        WHERE {filtro_data}
        GROUP BY banco
        HAVING banco IS NOT NULL AND banco <> ''
        ORDER BY total_propostas ASC;
    """, (inicio, fim))
    bancos_dados = cur.fetchall()
    
    cur.execute(f"""
        SELECT 
            fonte,
            observacao AS status,
            COUNT(*) AS qtd,
            COALESCE(SUM(valor_equivalente), 0) AS total_eq,
            COALESCE(SUM(valor_original), 0) AS total_or
        FROM propostas
        WHERE fonte IN ('URA', 'Disparo/Whatsapp', 'Disparo/SMS', 
                        'Indicação', 'Discadora', 'Tráfego')
          AND {filtro_data}
        GROUP BY fonte, observacao
        ORDER BY fonte, observacao;
    """, (inicio, fim))

    dados_fontes = cur.fetchall()

    fontes_lista = [
        "URA",
        "Disparo/Whatsapp",
        "Disparo/SMS",
        "Indicação",
        "Discadora",
        "Tráfego"
    ]

    fontes = {fonte: {} for fonte in fontes_lista}
    for fonte, status, qtd, eq, or_ in dados_fontes:
        status = (status or "Andamento").strip().title()
        fontes[fonte][status] = {
            "qtd": qtd,
            "valor_eq": float(eq or 0),
            "valor_or": float(or_ or 0)
        }

    import calendar

    hoje_str = agora.strftime("%Y-%m-%d")

    cur.execute(f"""
        SELECT COALESCE(SUM(valor_equivalente), 0)
        FROM propostas
        WHERE DATE(data) = {ph}
    """, (hoje_str,))
    total_hoje = cur.fetchone()[0] or 0

    primeiro_dia = agora.replace(day=1)
    dias_passados = (agora - primeiro_dia).days + 1
    dias_mes_real = calendar.monthrange(agora.year, agora.month)[1]

    from datetime import date
    dias_uteis_restantes = sum(
        1 for i in range(agora.day + 1, dias_mes_real + 1)
        if date(agora.year, agora.month, i).weekday() < 5
    )

    media_diaria_contratos = (total_or / total_propostas) if total_propostas > 0 else 0

    ticket_meta_diaria = 0
    if meta_global and total_eq is not None:
        falta_dias = max(dias_uteis_restantes, 1)
        ticket_meta_diaria = (falta_meta / falta_dias) if falta_dias > 0 else 0

    conn.close()

    return render_template(
        "dashboard.html",
        total_eq=float(total_eq or 0),
        total_or=float(total_or or 0),
        total_propostas=int(total_propostas or 0),
        falta_meta=float(falta_meta or 0),
        meta_global=float(meta_global or 0),
        ranking=ranking or [],
        inicio=inicio,
        fim=fim,
        periodo=periodo,
        bancos_dados=bancos_dados,
        fontes=fontes,
        ticket_meta_diaria=float(ticket_meta_diaria or 0),
        media_diaria_contratos=float(media_diaria_contratos or 0)
    )

from datetime import timedelta

@app.route("/painel_admin", methods=["GET", "POST"])
def painel_admin():
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))

    data_ini = request.args.get("data_ini")
    data_fim = request.args.get("data_fim")

    agora = datetime.now()
    if not data_ini or not data_fim:
        data_ini = agora.replace(day=1).strftime("%Y-%m-%d")
        ultimo_dia = (agora.replace(day=1) + relativedelta(months=1) - timedelta(days=1)).strftime("%Y-%m-%d")
        data_fim = ultimo_dia

    conn = get_conn()
    cur = conn.cursor()
    ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metas_globais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            valor REAL
        )
    """ if isinstance(conn, sqlite3.Connection) else """
        CREATE TABLE IF NOT EXISTS metas_globais (
            id SERIAL PRIMARY KEY,
            valor NUMERIC(12,2)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metas_individuais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            consultor TEXT UNIQUE,
            meta REAL
        )
    """ if isinstance(conn, sqlite3.Connection) else """
        CREATE TABLE IF NOT EXISTS metas_individuais (
            id SERIAL PRIMARY KEY,
            consultor TEXT UNIQUE,
            meta NUMERIC(12,2)
        )
    """)

    if isinstance(conn, sqlite3.Connection):
        query = f"""
            SELECT u.nome AS consultor,
                   COALESCE(SUM(p.valor_equivalente), 0) AS total_eq,
                   COALESCE(SUM(p.valor_original), 0) AS total_or,
                   COALESCE(m.meta, 0) AS meta,
                   (COALESCE(m.meta, 0) - COALESCE(SUM(p.valor_equivalente), 0)) AS falta
            FROM users u
            LEFT JOIN propostas p
                ON u.nome = p.consultor
               AND DATE(p.data) BETWEEN {ph} AND {ph}
            LEFT JOIN metas_individuais m
                ON u.nome = m.consultor
            WHERE u.role != 'admin'
            GROUP BY u.nome, m.meta
            ORDER BY total_eq DESC;
        """
    else:
        query = f"""
            SELECT u.nome AS consultor,
                   COALESCE(SUM(p.valor_equivalente), 0) AS total_eq,
                   COALESCE(SUM(p.valor_original), 0) AS total_or,
                   COALESCE(m.meta, 0) AS meta,
                   (COALESCE(m.meta, 0) - COALESCE(SUM(p.valor_equivalente), 0)) AS falta
            FROM users u
            LEFT JOIN propostas p
                ON u.nome = p.consultor
               AND DATE(p.data AT TIME ZONE 'America/Sao_Paulo') BETWEEN {ph} AND {ph}
            LEFT JOIN metas_individuais m
                ON u.nome = m.consultor
            WHERE u.role != 'admin'
            GROUP BY u.nome, m.meta
            ORDER BY total_eq DESC;
        """

    cur.execute(query, (data_ini, data_fim))
    ranking = cur.fetchall()

    cur.execute("SELECT valor FROM metas_globais ORDER BY id DESC LIMIT 1;")
    meta_global_row = cur.fetchone()
    meta_global = meta_global_row[0] if meta_global_row else 0

    media_usuarios = (sum([r[3] or 0 for r in ranking]) / len(ranking)) if ranking else 0

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta_dia (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            valor REAL
        )
    """ if isinstance(conn, sqlite3.Connection) else """
        CREATE TABLE IF NOT EXISTS meta_dia (
            id SERIAL PRIMARY KEY,
            valor NUMERIC(12,2)
        )
    """)
    cur.execute("SELECT valor FROM meta_dia ORDER BY id DESC LIMIT 1;")
    meta_dia_row = cur.fetchone()
    meta_dia = meta_dia_row[0] if meta_dia_row else 0

    conn.close()

    return render_template(
        "painel_admin.html",
        ranking=ranking,
        meta_global=meta_global,
        media_usuarios=media_usuarios,
        data_ini=data_ini,
        data_fim=data_fim,
        meta_dia=meta_dia
    )

@app.route("/editar_meta", methods=["POST"])
def editar_meta():
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))

    try:
        nova_meta = float(request.form.get("nova_meta", 0))
    except:
        nova_meta = 0

    conn = get_conn()
    cur = conn.cursor()
    ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    cur.execute("""
        CREATE TABLE IF NOT EXISTS metas_globais (
            id SERIAL PRIMARY KEY,
            valor NUMERIC(12,2)
        )
    """ if not isinstance(conn, sqlite3.Connection) else """
        CREATE TABLE IF NOT EXISTS metas_globais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            valor REAL
        )
    """)

    cur.execute("TRUNCATE metas_globais RESTART IDENTITY" if not isinstance(conn, sqlite3.Connection) else "DELETE FROM metas_globais;")
    cur.execute(f"INSERT INTO metas_globais (valor) VALUES ({ph})", (nova_meta,))
    conn.commit()

    cur.execute("SELECT valor FROM metas_globais ORDER BY id DESC LIMIT 1;")
    ultimo = cur.fetchone()
    print("Meta salva com sucesso:", ultimo[0] if ultimo else "(nenhuma)")

    conn.close()
    flash("Meta global atualizada com sucesso!", "success")
    return redirect(url_for("painel_admin"))

from dateutil.relativedelta import relativedelta

@app.route("/painel_usuario", methods=["GET"])
def painel_usuario():
    if "user" not in session:
        return redirect(url_for("login"))

    usuario_logado = session["user"]
    role = session["role"]

    conn = get_conn()
    cur = conn.cursor()

    if role == "admin":
        cur.execute(
            "SELECT DISTINCT consultor FROM propostas WHERE consultor IS NOT NULL ORDER BY consultor;"
        )
        consultores = [r[0] for r in cur.fetchall()]
    else:
        consultores = [usuario_logado]

    consultor_filtro = request.args.get("consultor") if role == "admin" else usuario_logado

    data_ini = request.args.get("data_ini")
    data_fim = request.args.get("data_fim")
    periodo = request.args.get("periodo")
    mes = request.args.get("mes")

    agora = datetime.now()
    hoje = agora.strftime("%Y-%m-%d")

    if data_ini and data_fim:
        inicio, fim = data_ini, data_fim
    else:
        if periodo == "hoje":
            inicio = fim = hoje
        elif periodo == "ultima_semana":
            inicio = (agora - timedelta(days=7)).strftime("%Y-%m-%d")
            fim = hoje
        elif periodo == "ultimo_mes":
            mes_passado = (agora.replace(day=1) - timedelta(days=1))
            inicio = mes_passado.replace(day=1).strftime("%Y-%m-%d")
            fim = mes_passado.strftime("%Y-%m-%d")
        elif periodo == "tudo":
            inicio, fim = "1900-01-01", "2100-01-01"
        else:
            if not mes:
                mes = agora.strftime("%Y-%m")
            inicio = f"{mes}-01"
            fim = (
                datetime.strptime(inicio, "%Y-%m-%d")
                + relativedelta(months=1)
                - timedelta(days=1)
            ).strftime("%Y-%m-%d")

    ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    if isinstance(conn, sqlite3.Connection):
        query = f"""
            SELECT
                id, data, fonte, banco, senha_digitada, tabela, nome_cliente, cpf,
                valor_equivalente, valor_original, observacao, telefone,
                valor_parcela, quantidade_parcelas, data_pagamento_prevista
            FROM propostas
            WHERE consultor = {ph}
              AND date(data) BETWEEN {ph} AND {ph}
            ORDER BY datetime(data) DESC;
        """
    else:
        query = f"""
            SELECT
                id, data, fonte, banco, senha_digitada, tabela, nome_cliente, cpf,
                valor_equivalente, valor_original, observacao, telefone,
                valor_parcela, quantidade_parcelas, data_pagamento_prevista
            FROM propostas
            WHERE consultor = {ph}
              AND DATE(data AT TIME ZONE 'America/Sao_Paulo') BETWEEN {ph} AND {ph}
            ORDER BY data DESC;
        """

    cur.execute(query, (consultor_filtro, inicio, fim))
    propostas_raw = cur.fetchall()

    propostas = []

    for p in propostas_raw:
        try:
            data_val = p[1]
            if isinstance(data_val, str):
                try:
                    data_val = datetime.strptime(
                        data_val.split(".")[0], "%Y-%m-%d %H:%M:%S"
                    )
                except Exception:
                    data_val = datetime.strptime(data_val, "%Y-%m-%d")
            propostas.append((p[0], data_val, *p[2:]))
        except Exception:
            propostas.append(p)

    total_eq = sum(float(p[8] or 0) for p in propostas)
    total_or = sum(float(p[9] or 0) for p in propostas)

    try:
        cur.execute(
            "SELECT meta FROM metas_individuais WHERE consultor = ?"
            if isinstance(conn, sqlite3.Connection)
            else "SELECT meta FROM metas_individuais WHERE consultor = %s",
            (consultor_filtro,),
        )
        meta_row = cur.fetchone()
        meta_individual = float(meta_row[0]) if meta_row else 0
    except Exception as e:
        print("⚠️ Erro ao buscar meta individual:", e)
        meta_individual = 0

    falta_meta = max(meta_individual - total_eq, 0)

    conn.close()

    try:
        mes_titulo = datetime.strptime(inicio, "%Y-%m-%d").strftime("%B/%Y")
    except Exception:
        mes_titulo = agora.strftime("%B/%Y")

    return render_template(
        "painel_usuario.html",
        usuario_logado=usuario_logado,
        propostas=propostas,
        total_eq=total_eq,
        total_or=total_or,
        consultores=consultores,
        consultor_filtro=consultor_filtro,
        role=role,
        inicio=inicio,
        fim=fim,
        mes=mes,
        mes_titulo=mes_titulo,
        hoje=hoje,
        meta_individual=meta_individual,
        falta_meta=falta_meta,
    )

@app.route("/editar_meta_individual", methods=["POST"])
def editar_meta_individual():
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))
    consultor = request.form["consultor"]
    nova_meta = float(request.form["nova_meta"])
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metas_individuais (
            id SERIAL PRIMARY KEY,
            consultor TEXT UNIQUE,
            meta NUMERIC(12,2)
        );
    """)
    cur.execute("INSERT INTO metas_individuais (consultor, meta) VALUES (%s, %s) "
                "ON CONFLICT (consultor) DO UPDATE SET meta = EXCLUDED.meta;" if not isinstance(conn, sqlite3.Connection)
                else "INSERT OR REPLACE INTO metas_individuais (consultor, meta) VALUES (?, ?);",
                (consultor, nova_meta))
    conn.commit()
    conn.close()
    return redirect(url_for("painel_admin"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))

    if request.method == "POST":
        nome = request.form["nome"].strip()
        senha = request.form["senha"]
        role = request.form["role"]

        conn = get_conn()
        cur = conn.cursor()

        ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

        cur.execute(f"SELECT * FROM users WHERE nome = {ph}", (nome,))
        if cur.fetchone():
            conn.close()
            return render_template("register.html", erro="Usuário já existe!")

        senha_hash = generate_password_hash(senha)
        cur.execute(f"INSERT INTO users (nome, senha, role) VALUES ({ph}, {ph}, {ph})", (nome, senha_hash, role))
        conn.commit()
        conn.close()
        return render_template("register.html", sucesso="Usuário criado com sucesso!")

    return render_template("register.html")


@app.route("/editar_usuario/<int:id>", methods=["GET", "POST"])
def editar_usuario(id):
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, nome, role FROM users WHERE id = ?" if isinstance(conn, sqlite3.Connection)
                else "SELECT id, nome, role FROM users WHERE id = %s", (id,))
    user = cur.fetchone()

    if not user:
        conn.close()
        return redirect(url_for("usuarios"))

    if request.method == "POST":
        nome = request.form["nome"]
        senha = request.form["senha"]
        role = request.form.get("role")

        ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

        if senha.strip():
            senha_hash = generate_password_hash(senha)
            query = f"UPDATE users SET nome = {ph}, senha = {ph}, role = {ph} WHERE id = {ph}"
            params = (nome, senha_hash, role, id)
        else:
            query = f"UPDATE users SET nome = {ph}, role = {ph} WHERE id = {ph}"
            params = (nome, role, id)

        cur.execute(query, params)
        conn.commit()
        conn.close()
        return redirect(url_for("usuarios"))

    conn.close()
    return render_template("editar.html", user=user)

@app.route("/usuarios", endpoint="usuarios")
def usuarios():
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, nome, role FROM users ORDER BY id ASC;")
    usuarios = cur.fetchall()
    conn.close()
    return render_template("usuarios.html", usuarios=usuarios)


@app.route("/excluir/<int:id>", methods=["POST"])
def excluir_usuario(id):
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = ?" if isinstance(conn, sqlite3.Connection)
                else "DELETE FROM users WHERE id = %s", (id,))
    conn.commit()
    conn.close()
    flash("Usuário excluído com sucesso!")
    return redirect(url_for("usuarios"))

def carregar_meta():
    if os.path.exists("meta.json"):
        with open("meta.json", "r", encoding="utf-8") as f:
            return json.load(f).get("meta", 0)
    return 0

@app.route("/atualizar_meta", methods=["POST"])
def atualizar_meta():
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))
    nova_meta = float(request.form["nova_meta"])
    with open("meta.json", "w", encoding="utf-8") as f:
        json.dump({"meta": nova_meta}, f)
    return redirect(url_for("dashboard"))

@app.route("/excluir_proposta/<int:id>")
def excluir_proposta(id):
    if "user" not in session:
        return redirect(url_for("login"))

    conn = get_conn()
    cur = conn.cursor()
    ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    cur.execute(f"DELETE FROM propostas WHERE id = {ph}", (id,))
    conn.commit()
    conn.close()

    origem = request.args.get("origem")

    if origem == "relatorios" and session.get("role") == "admin":
        flash("Proposta excluída com sucesso!", "success")
        return redirect(url_for("relatorios"))
    else:
        flash("Proposta excluída com sucesso!", "success")
        return redirect(url_for("painel_usuario"))

@app.route("/editar_proposta/<int:id>", methods=["GET", "POST"])
def editar_proposta(id):
    if "user" not in session:
        return redirect(url_for("login"))

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

        cur.execute(f"""
            SELECT 
                id,
                data,
                fonte,
                banco,
                senha_digitada,
                tabela,
                nome_cliente,
                cpf,
                valor_equivalente,
                valor_original,
                observacao,
                telefone,
                valor_parcela,
                quantidade_parcelas,
                produto,
                data_pagamento_prevista
            FROM propostas
            WHERE id = {ph}
        """, (id,))
        proposta = cur.fetchone()

        if not proposta:
            conn.close()
            return "Proposta não encontrada", 404

        # 🔹 SALVAR EDIÇÃO
        if request.method == "POST":
            fonte = request.form.get("fonte")
            banco = request.form.get("banco")
            senha_digitada = request.form.get("senha_digitada")
            produto = request.form.get("produto")
            tabela = request.form.get("tabela")
            nome_cliente = request.form.get("nome_cliente")
            cpf = request.form.get("cpf")
            telefone = request.form.get("telefone")

            valor_equivalente = request.form.get("valor_equivalente") or 0
            valor_original = request.form.get("valor_original") or 0
            valor_parcela = request.form.get("valor_parcela")
            quantidade_parcelas = request.form.get("quantidade_parcelas")

            observacao = request.form.get("observacao")
            data_pagamento_prevista = request.form.get("data_pagamento_prevista")

            # 🔹 DATA MANUAL (SE EXISTIR)
            data_manual = request.form.get("data_manual")
            if data_manual:
                try:
                    tz_br = pytz.timezone("America/Sao_Paulo")
                    data_obj = datetime.strptime(data_manual, "%Y-%m-%dT%H:%M")
                    data_obj = tz_br.localize(data_obj)
                    nova_data = data_obj.strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print("Erro ao converter data_manual:", e)
                    nova_data = proposta[1]
            else:
                nova_data = proposta[1]

            # 🔹 UPDATE FINAL
            cur.execute(f"""
                UPDATE propostas SET
                    data = {ph},
                    fonte = {ph},
                    banco = {ph},
                    senha_digitada = {ph},
                    produto = {ph},
                    tabela = {ph},
                    nome_cliente = {ph},
                    cpf = {ph},
                    valor_equivalente = {ph},
                    valor_original = {ph},
                    valor_parcela = {ph},
                    quantidade_parcelas = {ph},
                    observacao = {ph},
                    telefone = {ph},
                    data_pagamento_prevista = {ph}
                WHERE id = {ph}
            """, (
                nova_data,
                fonte,
                banco,
                senha_digitada,
                produto,
                tabela,
                nome_cliente,
                cpf,
                valor_equivalente,
                valor_original,
                valor_parcela,
                quantidade_parcelas,
                observacao,
                telefone,
                data_pagamento_prevista,
                id
            ))

            conn.commit()
            conn.close()
            flash("Proposta atualizada com sucesso!", "success")

            origem = request.args.get("origem")
            if origem == "relatorios" and session.get("role") == "admin":
                return redirect(url_for("relatorios"))
            return redirect(url_for("painel_usuario"))

        conn.close()
        return render_template("editar_proposta.html", proposta=proposta)

    except Exception as e:
        print("Erro ao editar proposta:", e)
        if conn:
            conn.close()
        return f"Ocorreu um erro ao editar a proposta: {e}", 500

@app.route("/visao_fontes")
def visao_fontes():
    fontes_lista = [
        "URA",
        "Disparo/Whatsapp",
        "Disparo/SMS",
        "Indicação",
        "Discadora",
        "Tráfego"
    ]

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            fonte,
            observacao,  -- usamos observacao como status (Pagas, Andamento, etc)
            COUNT(*) AS qtd,
            COALESCE(SUM(valor_equivalente), 0) AS total_eq,
            COALESCE(SUM(valor_original), 0) AS total_or
        FROM propostas
        WHERE fonte IN ('URA', 'Disparo/Whatsapp', 'Disparo/SMS', 
                        'Indicação', 'Discadora', 'Tráfego')
        GROUP BY fonte, observacao
        ORDER BY fonte;
    """)

    dados = cur.fetchall()
    conn.close()

    fontes = {fonte: {} for fonte in fontes_lista}

    for fonte, status, qtd, eq, or_ in dados:
        if fonte not in fontes:
            continue

        status = (status or "Andamento").strip().title()
        fontes[fonte][status] = {
            "qtd": qtd,
            "valor_eq": float(eq or 0),
            "valor_or": float(or_ or 0)
        }

    return render_template("visao_fontes.html", fontes=fontes)

@app.route("/editar_meta_dia", methods=["POST"])
def editar_meta_dia():
    if "user" not in session or session["role"] != "admin":
        return redirect(url_for("login"))

    try:
        nova_meta_dia = float(request.form.get("nova_meta_dia", 0))
    except:
        nova_meta_dia = 0

    conn = get_conn()
    cur = conn.cursor()

    if isinstance(conn, sqlite3.Connection):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta_dia (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                valor REAL
            )
        """)
        cur.execute("DELETE FROM meta_dia;")
        cur.execute("INSERT INTO meta_dia (valor) VALUES (?)", (nova_meta_dia,))
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta_dia (
                id SERIAL PRIMARY KEY,
                valor NUMERIC(12,2)
            )
        """)
        cur.execute("TRUNCATE meta_dia RESTART IDENTITY;")
        cur.execute("INSERT INTO meta_dia (valor) VALUES (%s);", (nova_meta_dia,))

    conn.commit()
    conn.close()
    return redirect(url_for("painel_admin"))

import random
import string

@app.route("/recuperar_senha", methods=["POST"])
def recuperar_senha():
    data = request.get_json()
    nome = data.get("nome", "").strip()

    if not nome:
        return jsonify({"erro": "Usuário inválido"}), 400

    conn = get_conn()
    cur = conn.cursor()

    ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    cur.execute(
        f"SELECT id FROM users WHERE nome = {ph}",
        (nome,)
    )
    user = cur.fetchone()

    if not user:
        conn.close()
        return jsonify({"erro": "Usuário não encontrado"}), 404

    chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789"
    senha_temp = "".join(random.choice(chars) for _ in range(8))

    senha_hash = generate_password_hash(senha_temp)

    cur.execute(
        f"UPDATE users SET senha = {ph} WHERE nome = {ph}",
        (senha_hash, nome)
    )

    conn.commit()
    conn.close()

    return jsonify({"senha": senha_temp})

@app.route("/ranking", methods=["GET"])
def ranking():
    if "user" not in session:
        return redirect(url_for("login"))

    data_ini = request.args.get("data_ini")
    data_fim = request.args.get("data_fim")

    agora = datetime.now()
    if not data_ini or not data_fim:
        data_ini = agora.replace(day=1).strftime("%Y-%m-%d")
        data_fim = (agora.replace(day=1) + relativedelta(months=1) - timedelta(days=1)).strftime("%Y-%m-%d")

    conn = get_conn()
    cur = conn.cursor()
    ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    if isinstance(conn, sqlite3.Connection):
        query = f"""
            SELECT u.nome AS consultor,
                   COALESCE(SUM(p.valor_equivalente), 0) AS total_eq,
                   COALESCE(SUM(p.valor_original), 0) AS total_or,
                   COALESCE(m.meta, 0) AS meta,
                   (COALESCE(m.meta, 0) - COALESCE(SUM(p.valor_equivalente), 0)) AS falta
            FROM users u
            LEFT JOIN propostas p
                ON u.nome = p.consultor
               AND DATE(p.data) BETWEEN {ph} AND {ph}
            LEFT JOIN metas_individuais m
                ON u.nome = m.consultor
            WHERE u.role != 'admin'
            GROUP BY u.nome, m.meta
            ORDER BY total_eq DESC;
        """
    else:
        query = f"""
            SELECT u.nome AS consultor,
                   COALESCE(SUM(p.valor_equivalente), 0) AS total_eq,
                   COALESCE(SUM(p.valor_original), 0) AS total_or,
                   COALESCE(m.meta, 0) AS meta,
                   (COALESCE(m.meta, 0) - COALESCE(SUM(p.valor_equivalente), 0)) AS falta
            FROM users u
            LEFT JOIN propostas p
                ON u.nome = p.consultor
               AND DATE(p.data AT TIME ZONE 'America/Sao_Paulo') BETWEEN {ph} AND {ph}
            LEFT JOIN metas_individuais m
                ON u.nome = m.consultor
            WHERE u.role != 'admin'
            GROUP BY u.nome, m.meta
            ORDER BY total_eq DESC;
        """

    cur.execute(query, (data_ini, data_fim))
    ranking = cur.fetchall()
    conn.close()

    return render_template(
        "ranking.html",
        ranking=ranking,
        data_ini=data_ini,
        data_fim=data_fim
    )

if __name__ == "__main__":
    app.run(debug=True)