from flask import Flask, request
import base64
import json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
import dash
from dash import dash_table
import dash_bootstrap_components as dbc
from dash import dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from datetime import datetime, time

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://luca.figari@uc.cl:18640133@langosta.ing.puc.cl:5432/luca.figari@uc.cl'
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 3600}

db = SQLAlchemy(app)

class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    operation_type = db.Column(db.String(4), nullable=False)
    message_id = db.Column(db.String(10), nullable=False)
    source_bank = db.Column(db.String(7), nullable=False)
    source_account = db.Column(db.String(10), nullable=False)
    destination_bank = db.Column(db.String(7), nullable=False)
    destination_account = db.Column(db.String(10), nullable=False)
    amount = db.Column(db.Numeric(16), nullable=False)
    publish_time = db.Column(db.DateTime, nullable=False)



@app.route('/recibir-transferencia', methods=['POST'])
def recibir_transferencia():
    content = request.get_json()  # Extrae el JSON del body del POST
    encoded_data = content['message']['data']
    decoded_data = base64.b64decode(encoded_data).decode()
    if (len(decoded_data) != 64):
        return 'Largo de mensaje erroneo', 204
    tipo_op = decoded_data[0:4]
    tipo_operacion = decoded_data[0:4]
    id_mensaje = decoded_data[4:14]
    banco_origen = decoded_data[14:21]
    cuenta_origen = decoded_data[21:31]
    banco_destino = decoded_data[31:38]
    cuenta_destino = decoded_data[38:48]
    monto = decoded_data[48:64]
    publish_time = content['message']['publishTime']
    transaction = Transaction(operation_type=tipo_operacion, message_id=id_mensaje, source_bank=banco_origen, source_account=cuenta_origen, destination_bank=banco_destino, destination_account=cuenta_destino, amount=monto, publish_time=publish_time)
    db.session.add(transaction)
    db.session.commit()
    return '', 204  

dash_app = dash.Dash(__name__, server=app, url_base_pathname='/dashboard/', external_stylesheets=[dbc.themes.BOOTSTRAP])

def serve_layout():
    min_date = db.session.query(Transaction.publish_time).order_by(Transaction.publish_time.asc()).first()
    if min_date:
        min_date = min_date[0]
    max_date = db.session.query(Transaction.publish_time).order_by(Transaction.publish_time.desc()).first()
    if max_date:
        max_date = max_date[0]

    source_bank_options = [{'label': 'Todos', 'value': 'all'}] + [{'label': bank[0], 'value': bank[0]} for bank in db.session.query(Transaction.source_bank).distinct().all()]
    destination_bank_options = [{'label': 'Todos', 'value': 'all'}] + [{'label': bank[0], 'value': bank[0]} for bank in db.session.query(Transaction.destination_bank).distinct().all()]

    return html.Div([
        dcc.Location(id='url', refresh=False),
        html.H1('Dashboard'),
        html.H3(id='num_operations'),
        html.H2('Tabla de operaciones'),
        dash_table.DataTable(id='operation_table'),
        html.H2('Conciliacion entre bancos'),
        html.P('El monto es neto para el banco 1, si el monto neto es positivo, el banco 1 le debe al banco 2, si es negativo, el banco 2 le debe al banco 1.'),
        dash_table.DataTable(id='bank_conciliation'),
        html.Br(),
        html.H2('Operaciones'),
        html.P('Aquí se muestran las últimas 100 operaciones recibidas'),
        dash_table.DataTable(id='last_100_transactions'),
        html.Br(),
        html.H2('Filters'),
        dcc.Dropdown(
            id = 'source_bank',
            options = source_bank_options,
            placeholder = 'Banco origen'
        ),
        dcc.Dropdown(
            id = 'destination_bank',
            options = destination_bank_options,
            placeholder = 'Banco destino'
        ),
        html.P('Fecha:'),
        dcc.DatePickerSingle(
            id='date_picker',
            max_date_allowed=max_date,
            initial_visible_month=max_date,
            date=max_date.date()
        ),
        html.H2('Histograma de montos'),
        dcc.Graph(id='histogram'),
    ])

with app.app_context():
    db.create_all()
    dash_app.layout = serve_layout

@dash_app.callback(
    Output('num_operations', 'children'),
    Input('url', 'pathname')
)
def update_num_operations(pathname):
    num_operations = db.session.query(Transaction).count()
    return f'Número de operaciones recibidas: {num_operations}'

@dash_app.callback(
    Output('operation_table', 'data'),
    Output('operation_table', 'columns'),
    [Input('url', 'pathname')]
)
def update_operation_table(pathname):
    operation_breakdown = db.session.query(
        Transaction.operation_type,
        db.func.count(Transaction.id),
        db.func.sum(Transaction.amount)
    ).group_by(Transaction.operation_type).all()
    df = pd.DataFrame(operation_breakdown, columns=['Tipo de operación', 'Cantidad', 'Monto total'])
    columns = [{"name": i, "id": i} for i in df.columns]
    data = df.to_dict('records')
    return data, columns


@dash_app.callback(
    Output('bank_conciliation', 'data'),
    Output('bank_conciliation', 'columns'),
    [Input('url', 'pathname')]
)
def banks_conciliation(pathname):
    query = text("""
            SELECT operation_type, source_bank, destination_bank, SUM(amount) as total_amount FROM transaction GROUP BY operation_type, source_bank, destination_bank;
            """)
    df = db.session.execute(query).fetchall()
    banks = set()
    for row in df:
        banks.add(row[1])
        banks.add(row[2])
    banks = list(banks)
    results = []
    # calculate the net amount for every par of banks
    bancos = []
    for bank1 in banks:
        for bank2 in banks:
            if bank1 != bank2 and (bank1, bank2) not in bancos and (bank2, bank1) not in bancos:
                bancos.append((bank1, bank2))
                amount = 0
                for row in df:
                    if row[1] == '2200':
                        if row[1] == bank1 and row[2] == bank2:
                            amount += row[3]
                        elif row[1] == bank2 and row[2] == bank1:
                            amount -= row[3]
                    else:
                        if row[1] == bank1 and row[2] == bank2:
                            amount -= row[3]
                        elif row[1] == bank2 and row[2] == bank1:
                            amount += row[3]
                if amount != 0:
                    results.append({'Banco 1': bank1, 'Banco 2': bank2,  'Monto neto': amount})
    
    df = pd.DataFrame(results)
    columns = [{"name": i, "id": i} for i in df.columns]
    data = df.to_dict('records')
    return data, columns

@dash_app.callback(
    Output('last_100_transactions', 'data'),
    Output('last_100_transactions', 'columns'),
    [Input('url', 'pathname')]
)
def last_100_transactions(pathname):
    query = text("""
            SELECT * FROM transaction ORDER BY id DESC LIMIT 100;
            """)
    df = db.session.execute(query).fetchall()
    df = pd.DataFrame(df, columns=['id', 'operation_type', 'message_id', 'source_bank', 'source_account', 'destination_bank', 'destination_account', 'amount', 'publish_time'])
    data = df.to_dict('records')
    columns = [{"name": i, "id": i} for i in df.columns]
    return data, columns

@dash_app.callback(
    Output('histogram', 'figure'),
    [Input('url', 'pathname'),
    Input('source_bank', 'value'),
    Input('destination_bank', 'value'),
    Input('date_picker', 'date')]
)
def update_histogram(pathname, source_bank, destination_bank, date):
    query = db.session.query(Transaction)
    if type(date) == str:
        date = datetime.strptime(date, '%Y-%m-%d')
    start_of_day = datetime.combine(date, time.min)
    end_of_day = datetime.combine(date, time.max)
    if date:
        query = query.filter(Transaction.publish_time >= start_of_day)
        query = query.filter(Transaction.publish_time <= end_of_day)
    
    if source_bank != 'all' and source_bank:
        query = query.filter(Transaction.source_bank == source_bank)
    if destination_bank != 'all' and destination_bank:
        query = query.filter(Transaction.destination_bank == destination_bank)
    transactions = query.all()
    amounts = [transaction.amount for transaction in transactions]
    bins = [0, 10000, 50000, 100000, 500000, 1000000, 10000000, float('inf')]
    labels = ['0-10k', '10k-50k', '50k-100k', '100k-500k', '500k-1M', '1M-10M', '10M+']
    df = pd.DataFrame({'Monto': amounts})
    df['Rango'] = pd.cut(df['Monto'], bins=bins, labels=labels)
    fig = px.histogram(df, x='Rango')
    return fig


@app.route('/')
def hello_world():
    return 'Hello, World!'


if __name__ == "__main__":    
    app.run(port=5000)