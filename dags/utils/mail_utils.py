import base64


def get_mail_body(message):
    body = message['payload']['body'].get('data')
    if body:
        return base64.urlsafe_b64decode(body).decode('utf-8')
    body = ''
    for part in message['payload']['parts']:
        if part['mimeType'] == 'text/html':
            part_body = part['body']['data']
            part_body = base64.urlsafe_b64decode(part_body).decode('utf-8')
            body += " " + part_body
    return body
            