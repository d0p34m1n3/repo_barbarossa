
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import smtplib


def send_email_with_attachment(**kwargs):

    if 'send_from' in kwargs.keys():
        send_from = kwargs['send_from']
    else:
        send_from = 'kocatulum@gmail.com'

    if 'send_to' in kwargs.keys():
        send_to = kwargs['send_to']
    else:
        send_to = 'kocatulum@gmail.com'

    if 'username' in kwargs.keys():
        username = kwargs['username']
    else:
        username = 'kocatulum'

    if 'password' in kwargs.keys():
        password = kwargs['password']
    else:
        password = 'H5Vsh7S2vmyz'

    if 'email_text' in kwargs.keys():
        email_text = kwargs['email_text']
    else:
        email_text = ''

    if 'attachment_list' in kwargs.keys():
        attachment_list = kwargs['attachment_list']
    else:
        attachment_list = []

    subject = kwargs['subject']

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    if email_text != '':
        msg.attach(MIMEText(email_text))

    if len(attachment_list)>=0:
        for i in range(len(attachment_list)):
                part = MIMEBase('application', "octet-stream")
                part.set_payload(open(attachment_list[i], "rb").read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename=file-' + str(i+1) + '.xlsx' + '')
                msg.attach(part)

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.login(username,password)

    server.sendmail(send_from, send_to, msg.as_string())
    server.quit()



