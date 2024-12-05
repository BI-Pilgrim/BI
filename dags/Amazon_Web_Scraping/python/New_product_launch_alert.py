import smtplib
import pandas as pd
from google.cloud import bigquery
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import StringIO
from cryptography.fernet import Fernet
import pandas_gbq as pgbq


key = b"6xxliLxwuePV66P3kVVsxQunIPXrGpftn3wPHtQw-To="
encrypted_password = b"gAAAAABnOsoKL5aIs8AVIQBRfqX-Ba3x5mFGzVS1l8DR7fJQudmNK4qqRy3JBnFZWf7JQVF5iulcA_a-sLTOpEIlRj7rWTzh4A=="

cipher = Fernet(key)
smtp_password = cipher.decrypt(encrypted_password).decode("utf-8")

SENDER_EMAIL = "kajal.ray@discoverpilgrim.com"
# RECIPIENT_EMAILS = ["kajal.ray@discoverpilgrim.com","rwitapa.mitra@discoverpilgrim.com","shafiq@discoverpilgrim.com"]
RECIPIENT_EMAILS = ["konark.gaur@discoverpilgrim.com","pooja.vadia@discoverpilgrim.com","minu@discoverpilgrim.com","swati.sachdeva@discoverpilgrim.com",
 "amrutha.renganathan@discoverpilgrim.com","bi@discoverpilgrim.com"]
EMAIL_PASSWORD = smtp_password
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

query = """
   SELECT 
    latest.Category, latest.Brand_value, latest.Product_Title, latest.Parent_ASIN, latest.ASIN,latest.Product_URL, latest.Selling_Price,latest.MRP_Price,
    latest.Unit_Sold, latest.Revenue,
FROM 
    `Amazon_Market_Sizing.AMZ_Current_month_MS` latest
LEFT JOIN 
    `Amazon_Market_Sizing.AMZ_SKU_level_Historical_MS`  past
ON 
    latest.ASIN = past.Child_ASIN
    AND latest.Parent_ASIN = past.Parent_ASIN
WHERE 
    past.Date_MS < "2024-11-01" and 
    past.Child_ASIN IS NULL 
    AND past.Parent_ASIN IS NULL 
    and latest.Category Not Like "%EDP%"
    and (latest.Brand_value LIKE '%mCaffeine%' 
   OR latest.Brand_value LIKE '%Plum%' 
   OR latest.Brand_value LIKE '%Minimalist%' 
   OR latest.Brand_value LIKE '%Mama Earth%' 
   OR latest.Brand_value LIKE '%Pilgrim%' 
   OR latest.Brand_value LIKE '%Dot & Key%' 
   OR latest.Brand_value LIKE '%Loreal%' 
   OR latest.Brand_value LIKE '%Dove%' 
   OR latest.Brand_value LIKE '%Tresemme%' 
   OR latest.Brand_value LIKE '%Pantene%' 
   OR latest.Brand_value LIKE '%Head & Shoulders%' 
   OR latest.Brand_value LIKE '%Indulekha%' 
   OR latest.Brand_value LIKE '%Love Beauty%' 
   OR latest.Brand_value LIKE '%Traya%' 
   OR latest.Brand_value LIKE '%Wishcare%' 
   OR latest.Brand_value LIKE '%Wow%' 
   OR latest.Brand_value LIKE '%Soulflower%' 
   OR latest.Brand_value LIKE '%Bare Anatomy%' 
   OR latest.Brand_value LIKE '%Derma Co%' 
   OR latest.Brand_value LIKE '%Plix%' 
   OR latest.Brand_value LIKE '%Ponds%' 
   OR latest.Brand_value LIKE '%Nivea%' 
   OR latest.Brand_value LIKE '%Neutrogena%' 
   OR latest.Brand_value LIKE '%Garnier%' 
   OR latest.Brand_value LIKE '%Lakme%' 
   OR latest.Brand_value LIKE '%Glow & Lovely%' 
   OR latest.Brand_value LIKE '%Aqualogica%' 
   OR latest.Brand_value LIKE '%Dr. Sheth%' 
   OR latest.Brand_value LIKE '%Deconstruct%' 
   OR latest.Brand_value LIKE '%La Shield%')
    """

df =  pgbq.read_gbq(query,"shopify-pubsub-project")
date_query = """

select max(Date_MS) from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS`

"""

max_date = pgbq.read_gbq(date_query,"shopify-pubsub-project")

current = max_date['f0_'][0].strftime("%B %Y")
summary = df.groupby('Brand_value')['ASIN'].nunique().reset_index(name='New Product Launch')

html_table = summary.to_html(index=False, border=1)

def send_email_with_attachment(df,current):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENT_EMAILS)
    msg['Subject'] = "Competitor New Product Launch" + " - " +current

    body = f"""

<html>
<body>
    <p>Hi Team,</p>
    <p>Here is the summary of the new product launched by target competitors,
    </p>
    {html_table}
    <br>Also, PFA the detailed report on the competitor new product launched. </p>
    <br><p>Regards,<br>Kajal Ray</p>
</body>
</html>
"""


    empty_data_body = f"""
<html>
<body>
    <p>Hi Team,</p>
    <p>Here is the summary of the new product launched by target competitors
    </p>
    <b> There are no new product launches at Amazon this month </b>
    
    <p>Regards,<br>Kajal Ray</p>
</body>
</html>
"""

    if summary.empty == True:
        msg.attach(MIMEText(empty_data_body, 'html'))
    else:
        msg.attach(MIMEText(body, 'html'))
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(csv_buffer.getvalue())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= new_product_entries.csv")
        msg.attach(part)

    # Attach the CSV file
    

    
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, EMAIL_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())

send_email_with_attachment(df,current)