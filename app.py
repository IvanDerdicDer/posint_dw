from flask import Flask, redirect, render_template, session, request
import pyodbc
import secrets
from itertools import groupby

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)


def get_connection(
        conn_string: str
) -> pyodbc.Connection:
    conn = pyodbc.connect(conn_string)

    return conn


@app.route('/')
def home():  # put application's code here
    return render_template('home.html')


@app.route('/connection_string')
def connection_string():
    return render_template(
        'connection_string.html',
        connection_string=session.get('connection_string')
    )


@app.post('/save_connection')
def save_connection():
    session['connection_string'] = request.form.get('connection_string')
    return redirect('/')


@app.get('/tables')
def tables():
    conn = get_connection(session.get('connection_string'))
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tablica where sifTipTablica = 1")
    tables = cursor.fetchall()
    cursor.close()
    conn.close()

    tables = [{
        'sifTablica': table.sifTablica,
        'nazTablica': table.nazTablica.strip(),
        'nazSQLTablica': table.nazSQLTablica.strip()
    } for table in tables]

    session['fact_tables'] = tables

    return render_template(
        'tables.html',
        fact_tables=tables
    )


@app.post('/tables')
def tables_post():
    session['fact_table_id'] = request.form.get('fact_table')
    conn = get_connection(session.get('connection_string'))
    cursor = conn.cursor()
    cursor.execute(
f"""
select tablica.sifTablica,
       tA.rbrAtrib,
       imeSQLAtrib,
       tAAF.imeAtrib,
       nazAgrFun
from tablica
join tabAtribut tA
    on tablica.sifTablica = tA.sifTablica
join tabAtributAgrFun tAAF
    on tA.sifTablica = tAAF.sifTablica and tA.rbrAtrib = tAAF.rbrAtrib
join agrFun aF on aF.sifAgrFun = tAAF.sifAgrFun
where tA.sifTipAtrib = 1 and tA.sifTablica = {session.get('fact_table_id')}
""")

    fact_table_mesures = cursor.fetchall()

    cursor.execute(
f"""
select sifDimTablica
    , rbrAtrib
    , imeSQLAtrib
    , imeAtrib
    , nazTablica
    , nazSQLTablica
from dimCinj
join tabAtribut tA
    on tA.sifTablica = dimCinj.sifDimTablica
join tablica t
    on tA.sifTablica = t.sifTablica
where sifTipAtrib = 2 and dimCinj.sifCinjTablica = {session.get('fact_table_id')}
order by sifDimTablica
""")

    dimensions_and_columns = cursor.fetchall()

    cursor.close()
    conn.close()

    fact_table_mesures = [{
        'sifTablica': fact_table_mesure.sifTablica,
        'rbrAtrib': fact_table_mesure.rbrAtrib,
        'imeSQLAtrib': fact_table_mesure.imeSQLAtrib.strip(),
        'imeAtrib': fact_table_mesure.imeAtrib.strip(),
        'nazAgrFun': fact_table_mesure.nazAgrFun.strip()
    } for fact_table_mesure in fact_table_mesures]
    session['fact_table_mesures'] = fact_table_mesures

    dimensions_and_columns = [{
        'sifDimTablica': dimension_and_column.sifDimTablica,
        'rbrAtrib': dimension_and_column.rbrAtrib,
        'imeSQLAtrib': dimension_and_column.imeSQLAtrib.strip(),
        'imeAtrib': dimension_and_column.imeAtrib.strip(),
        'nazTablica': dimension_and_column.nazTablica.strip(),
        'nazSQLTablica': dimension_and_column.nazSQLTablica.strip(),
        'group_key': f"{dimension_and_column.sifDimTablica}|{dimension_and_column.nazSQLTablica.strip()}|{dimension_and_column.nazTablica.strip()}"
    } for dimension_and_column in dimensions_and_columns]

    dimensions_and_columns_grouped = {i: list(j) for i, j in groupby(dimensions_and_columns, lambda x: x['group_key'])}

    session['dimensions_and_columns_grouped'] = dimensions_and_columns_grouped

    return render_template(
        'tables.html',
        fact_tables=session.get('fact_tables'),
        fact_table_mesures=fact_table_mesures,
        fact_table=[i for i in session.get('fact_tables') if i['sifTablica'] == int(session.get('fact_table_id'))][0]['nazTablica'],
        dimensions_and_columns_grouped=dimensions_and_columns_grouped
    )


if __name__ == '__main__':
    app.run(debug=True)
