import datetime
import psycopg2
import uuid
import sendgrid
import jinja2
from weasyprint import HTML, CSS
from weasyprint.fonts import FontConfiguration
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Attachment, FileContent, FileName, FileType, Disposition)
import base64


def get_time_range():
    today = datetime.date.today()
    last_monday = today - datetime.timedelta(days=today.weekday())
    previous_monday = today - datetime.timedelta(days=today.weekday(), weeks=1)

    return [previous_monday, last_monday]


def get_locals(connection):
    cursor = connection.cursor()
    cursor.execute(
        'SELECT loc.id, loc.company_id, com.fee FROM companies_local loc JOIN companies_company com ON loc.company_id = com.id')
    results = cursor.fetchall()

    return results


def get_order_data(from_date, to_date, local_id, connection):

    cursor = connection.cursor()

    sql_query = 'SELECT id, igv, total, sub_total, number, created FROM orders_order WHERE created > %s AND created < %s AND local_id = %s'

    cursor.execute(sql_query, (
        from_date,
        to_date,
        local_id,
    ))

    results = cursor.fetchall()

    igv_total = 0
    total = 0
    sub_total = 0

    for result in results:
        igv_total += result[1]
        total += result[2]
        sub_total += result[3]

    return {
        'sub_total': sub_total,
        'igv_total': igv_total,
        'total': total,
        'data': results
    }


def send_mail_with_attachment(recipients_array, email_body, pdf):
    encoded_file = base64.b64encode(pdf).decode()

    attachedFile = Attachment(
        FileContent(encoded_file),
        FileName('prueba.pdf'),
        FileType('application/pdf'),
        Disposition('attachment')
    )

    message = Mail(
        from_email='info@weeare.pe',
        to_emails=recipients_array,
        subject='Resumen Semanal',
        html_content=email_body
    )

    message.attachment = attachedFile

    sg = SendGridAPIClient(
        api_key='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')

    response = sg.send(message)

    print(response.status_code)


def generate_pdf(billing_context, order_context):
    font_config = FontConfiguration()

    templateLoader = jinja2.FileSystemLoader(searchpath="./")
    templateEnv = jinja2.Environment(loader=templateLoader)
    TEMPLATE_FILE = "billing_pdf.html"
    template = templateEnv.get_template(TEMPLATE_FILE)

    outputText = template.render(billing=billing_context, orders=order_context)

    html = HTML(string=outputText)
    pdf = html.write_pdf(font_config=font_config)

    return pdf


def process_billing(local, from_date, to_date, company_id, company_fee, connection):
    order_data = get_order_data(from_date, to_date, local, connection)

    cursor = connection.cursor()

    sql_query = 'INSERT INTO billings_billing (uuid, sub_total, fee, total, igv, start_date, end_date, company_id, local_id, status, created, modified) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id'

    cursor.execute(sql_query, (
        str(uuid.uuid4()),
        order_data['sub_total'],
        0 if company_fee == None else company_fee,
        order_data['total'] if company_fee == None else company_fee +
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

    for order in order_data['data']:
        sql_query_billing_orders = 'INSERT INTO billings_billing_orders (billing_id, order_id) VALUES ( %s, %s)'

        cursor.execute(sql_query_billing_orders, (
            new_billing_id,
            order[0]
        ))

    suppliers_mails_query = 'SELECT email FROM companies_supplier WHERE company_id = %s'

    cursor.execute(suppliers_mails_query, (
        str(company_id)
    ))

    suppliers_mails = cursor.fetchall()

    local_data_sql = 'SELECT name FROM companies_local WHERE id = %s'

    cursor.execute(local_data_sql, (
        str(local)
    ))

    local_name = cursor.fetchone()[0]

    connection.commit()

    recipients = ''

    for supplier_mail in suppliers_mails:
        recipients += supplier_mail[0] + ','

    if len(recipients) > 0:
        recipients = recipients[:-1]

    recipients_array = recipients.split(',')

    if len(recipients_array) == 0:
        raise Exception('No recipients')

    order_context = []

    for order in order_data['data']:
        fmt_datetime = str(order[5].strftime("%I:%M %p %d/%m/%Y"))
        order_context.append({
            'created': fmt_datetime,
            'number': order[4],
            'igv': order[1],
            'sub_total': order[3],
            'total': order[2],
            'company': {
                'fee': 0 if company_fee == None else company_fee
            }
        })

    billing_context = {'start_date': str(from_date.strftime("%d/%m/%Y")), 'end_date': str(to_date.strftime("%d/%m/%Y")),
                       'sub_total': order_data['sub_total'], 'igv': order_data['igv_total'], 'total': order_data['total'], 'local': {'name': local_name}}

    pdf = generate_pdf(billing_context, order_context)

    send_mail_with_attachment(recipients_array, str(order_data), pdf)


def local_lambda():
    try:
        connection = psycopg2.connect(
            host='127.0.0.1', database='local', user='postgres', password='postgres')

        time_range = get_time_range()

        locals_data = get_locals(connection)

        for local_data in locals_data:
            process_billing(
                local_data[0], time_range[0], time_range[1], local_data[1], local_data[2], connection)

    except psycopg2.DatabaseError as e:
        print(f'Error {e}')


local_lambda()
