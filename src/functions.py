import datetime
import psycopg2
import uuid


def get_time_range():
    today = datetime.date.today()
    last_monday = today - datetime.timedelta(days=today.weekday())
    previous_monday = today - datetime.timedelta(days=today.weekday(), weeks=1)

    return [previous_monday, last_monday]


def get_locals(connection):
    cursor = connection.cursor()
    cursor.execute('SELECT loc.id, loc.company_id, com.fee FROM companies_local loc JOIN companies_company com ON loc.company_id = com.id')
    results = cursor.fetchall()

    return results


def get_order_data(from_date, to_date, local_id, connection):

    cursor = connection.cursor()

    sql_query = 'SELECT id, igv, total, sub_total FROM orders_order WHERE created > %s AND created < %s AND local_id = %s'

    cursor.execute(sql_query, (
        from_date,
        to_date,
        local_id,
    ))

    results = cursor.fetchall()

    igv_total = 0
    total = 0
    sub_total = 0

    order_ids = []

    for result in results:
        order_ids.append(result[0])
        igv_total += result[1]
        total += result[2]
        sub_total += result[3]

    return {
        'sub_total': sub_total,
        'igv_total': igv_total,
        'total': total,
        'order_ids': order_ids
    }


def process_billing(local, from_date, to_date, company_id, company_fee, connection):
    order_data = get_order_data(from_date, to_date, local, connection)

    cursor = connection.cursor()

    sql_query = 'INSERT INTO billings_billing (uuid, sub_total, fee, total, igv, start_date, end_date, company_id, local_id, status, created, modified) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id'

    cursor.execute(sql_query, (
        str(uuid.uuid4()),
        order_data['sub_total'],
        0 if company_fee == None else company_fee,
        order_data['total'],
        order_data['igv_total'],
        from_date,
        to_date,
        company_id,
        local,
        'INVOICED',
        to_date,
        to_date
    ))

    new_billing_id = cursor.fetchone()[0]

    for order_id in order_data['order_ids']:

        sql_query_billing_orders = 'INSERT INTO billings_billing_orders (billing_id, order_id) VALUES ( %s, %s)'

        cursor.execute(sql_query_billing_orders, (
            new_billing_id,
            order_id
        ))

    connection.commit()

    # aca llamar a funcion que crea el reporte
    # guardarlo en S3
    # enviarlo por correo

    print(new_billing_id)


def local_lambda():
    try:
        connection = psycopg2.connect(
            host='127.0.0.1', database='local', user='postgres', password='postgres')

        time_range = get_time_range()

        locals_data = get_locals(connection)

        for local_data in locals_data:
            process_billing(local_data[0], time_range[0], time_range[1], local_data[1], local_data[2], connection)

    except psycopg2.DatabaseError as e:
        print(f'Error {e}')


local_lambda()
