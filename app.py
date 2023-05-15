from functools import reduce

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
        fact_table=[i for i in session.get('fact_tables') if i['sifTablica'] == int(session.get('fact_table_id'))][0][
            'nazTablica'],
        dimensions_and_columns_grouped=dimensions_and_columns_grouped
    )


# ['mes|100|9', 'mes|100|12', 'mes|100|14']
# ['dim|102|2', 'dim|102|3', 'dim|111|4', 'dim|111|8', 'dim|111|13']

@app.post('/generate_sql')
def generate_sql():
    measures = request.form.getlist('fact_table_mesures')
    dim_columns = request.form.getlist('dimension_columns')

    measures = [{
        'description': i.split('|')[0],
        'table_id': int(i.split('|')[1]),
        'column_id': int(i.split('|')[2]),
        'aggregation': i.split('|')[3]
    } for i in measures]

    dim_columns = [{
        'description': i.split('|')[0],
        'table_id': int(i.split('|')[1]),
        'column_id': int(i.split('|')[2])
    } for i in dim_columns]

    fact_table = [i for i in session.get('fact_tables') if i['sifTablica'] == int(session.get('fact_table_id'))][0]

    measures = [i for i in session.get('fact_table_mesures') if
                (i['rbrAtrib'], i['nazAgrFun']) in map(lambda x: (x['column_id'], x['aggregation']), measures)]
    for i in measures:
        i.update({
            'nazSQLTablica': fact_table['nazSQLTablica'],
            'nazTablica': fact_table['nazTablica']
        })

    dimensions_and_columns_grouped = {i: [x
                                          for x in j if x['sifDimTablica'] in map(lambda x: x['table_id'], dim_columns)\
                                          and x['rbrAtrib'] in map(lambda x: x['column_id'], dim_columns)]
                                      for i, j in session.get('dimensions_and_columns_grouped').items() if
                                      int(i.split('|')[0]) in map(lambda x: x['table_id'], dim_columns)}

    conn = get_connection(session.get('connection_string'))
    cursor = conn.cursor()

    cursor.execute(
        f"""
select imeSQLAtrib,
       imeAtrib
from tabAtribut
where sifTablica = {session.get('fact_table_id')}
""")
    fact_table_columns = cursor.fetchall()
    fact_table_columns = [{
        'imeSQLAtrib': fact_table_column.imeSQLAtrib.strip(),
        'imeAtrib': fact_table_column.imeAtrib.strip()
    } for fact_table_column in fact_table_columns]

    # [BEGIN] SQL GENERATION NO MEASURES AND NO DIMENSIONS
    if not measures and not dimensions_and_columns_grouped:
        sql = \
            f"""
select {', '.join(f'[{fact_table["nazSQLTablica"]}].[{fact_table_column["imeSQLAtrib"]}] as [{fact_table_column["imeAtrib"]}]'
                  for fact_table_column in fact_table_columns)}
from {fact_table['nazSQLTablica']} 
"""
        cursor.execute(sql)

        data = cursor.fetchall()
        data_column_names = [i[0] for i in cursor.description]
        data_rows = [i for i in data]

        return render_template(
            'tables.html',
            fact_tables=session.get('fact_tables'),
            fact_table_mesures=session.get('fact_table_mesures'),
            fact_table=fact_table['nazTablica'],
            dimensions_and_columns_grouped=session.get('dimensions_and_columns_grouped'),
            sql=sql.replace(', ', '\n\t, ').strip(),
            data_column_names=data_column_names,
            data_rows=data_rows
        )
    # [END] SQL GENERATION NO MEASURES AND NO DIMENSIONS

    # [BEGIN] SQL GENERATION MEASURES AND NO DIMENSIONS
    if measures and not dimensions_and_columns_grouped:
        sql = \
            f"""
select {', '.join(f'{i["nazAgrFun"]}([{fact_table["nazSQLTablica"]}].[{i["imeSQLAtrib"]}]) as [{i["imeAtrib"]}]'
                  for i in measures)}
from {fact_table['nazSQLTablica']}
"""
        cursor.execute(sql)

        data = cursor.fetchall()
        data_column_names = [i[0] for i in cursor.description]
        data_rows = [i for i in data]

        return render_template(
            'tables.html',
            fact_tables=session.get('fact_tables'),
            fact_table_mesures=session.get('fact_table_mesures'),
            fact_table=fact_table['nazTablica'],
            dimensions_and_columns_grouped=session.get('dimensions_and_columns_grouped'),
            sql=sql.replace(', ', '\n\t, ').strip(),
            data_column_names=data_column_names,
            data_rows=data_rows
        )
    # [END] SQL GENERATION MEASURES AND NO DIMENSIONS

    cursor.execute(
        f"""
select sifCinjTablica
     , sifDimTablica
     , t.nazSQLTablica
     , tA.imeSQLAtrib  as dimImeSQLAtrib
     , tA.imeAtrib     as dimImeAtrib
     , tA2.imeSQLAtrib as factImeSQLAtrib
     , tA2.imeAtrib    as factImeAtrib
from dimCinj
    join tabAtribut tA
        on tA.sifTablica = dimCinj.sifDimTablica
            and tA.rbrAtrib = dimCinj.rbrDim
    join tabAtribut tA2
        on tA2.sifTablica = dimCinj.sifCinjTablica
            and tA2.rbrAtrib = dimCinj.rbrCinj
    join tablica t
        on tA.sifTablica = t.sifTablica
where sifCinjTablica = {session.get('fact_table_id')}
and sifDimTablica in ({', '.join(map(lambda x: x.split('|')[0], dimensions_and_columns_grouped))})
""")

    join_conditions = cursor.fetchall()
    join_conditions = [{
        'sifCinjTablica': join_condition.sifCinjTablica,
        'sifDimTablica': join_condition.sifDimTablica,
        'nazSQLTablica': join_condition.nazSQLTablica.strip(),
        'dimImeSQLAtrib': join_condition.dimImeSQLAtrib.strip(),
        'dimImeAtrib': join_condition.dimImeAtrib.strip(),
        'factImeSQLAtrib': join_condition.factImeSQLAtrib.strip(),
        'factImeAtrib': join_condition.factImeAtrib.strip()
    } for join_condition in join_conditions]

    # [BEGIN] SQL GENERATION NO MEASURES AND DIMENSIONS
    if not measures and dimensions_and_columns_grouped:
        sql = \
f"""
select {', '.join(f'[{i["nazSQLTablica"]}].[{i["imeSQLAtrib"]}] as [{i["nazTablica"]}.{i["imeAtrib"]}]' 
                  for i in reduce(lambda x, y: x + y, dimensions_and_columns_grouped.values()))}
from {fact_table['nazSQLTablica']}
{'--R--'.join(f'join {join_condition["nazSQLTablica"]} on {fact_table["nazSQLTablica"]}.{join_condition["factImeSQLAtrib"]} = {join_condition["nazSQLTablica"]}.{join_condition["dimImeSQLAtrib"]}' for join_condition in join_conditions)}
""".replace('--R--', '\n').replace(', ', '\n\t, ').strip()
        cursor.execute(sql)

        data = cursor.fetchall()
        data_column_names = [i[0] for i in cursor.description]
        data_rows = [i for i in data]

        return render_template(
            'tables.html',
            fact_tables=session.get('fact_tables'),
            fact_table_mesures=session.get('fact_table_mesures'),
            fact_table=fact_table['nazTablica'],
            dimensions_and_columns_grouped=session.get('dimensions_and_columns_grouped'),
            sql=sql,
            data_column_names=data_column_names,
            data_rows=data_rows
        )
    # [END] SQL GENERATION NO MEASURES AND DIMENSIONS

    # [BEGIN] SQL GENERATION MEASURES AND DIMENSIONS
    if measures and dimensions_and_columns_grouped:
        sql = \
f"""
select {', '.join(f'[{i["nazSQLTablica"]}].[{i["imeSQLAtrib"]}] as [{i["nazTablica"]}.{i["imeAtrib"]}]' 
                  for i in reduce(lambda x, y: x + y, dimensions_and_columns_grouped.values()))}
    , {', '.join(f'{i["nazAgrFun"]}([{fact_table["nazSQLTablica"]}].[{i["imeSQLAtrib"]}]) as [{i["imeAtrib"]}]'
                  for i in measures)}
from {fact_table['nazSQLTablica']}
{'--R--'.join(f'join {join_condition["nazSQLTablica"]} on {fact_table["nazSQLTablica"]}.{join_condition["factImeSQLAtrib"]} = {join_condition["nazSQLTablica"]}.{join_condition["dimImeSQLAtrib"]}' for join_condition in join_conditions)}
group by {', '.join(f'[{i["nazSQLTablica"]}].[{i["imeSQLAtrib"]}]' for i in reduce(lambda x, y: x + y, dimensions_and_columns_grouped.values()))}
""".replace('--R--', '\n').replace(', ', '\n\t, ').strip()
        cursor.execute(sql)

        data = cursor.fetchall()
        data_column_names = [i[0] for i in cursor.description]
        data_rows = [i for i in data]

        return render_template(
            'tables.html',
            fact_tables=session.get('fact_tables'),
            fact_table_mesures=session.get('fact_table_mesures'),
            fact_table=fact_table['nazTablica'],
            dimensions_and_columns_grouped=session.get('dimensions_and_columns_grouped'),
            sql=sql,
            data_column_names=data_column_names,
            data_rows=data_rows
        )


if __name__ == '__main__':
    app.run(debug=True)
