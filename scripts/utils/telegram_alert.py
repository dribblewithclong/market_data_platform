import re
import requests
import warnings

warnings.filterwarnings("ignore")


def send_message(
    text: str,
    html_mode: bool = True,
):
    bot = '6059763237:AAGRnB6D7F_Oj4hsc6G4lsKZnNQDhl-coV8'
    chat_id = '5621197039'
    telegram_url = f'https://api.telegram.org/bot{bot}/sendMessage'
    if html_mode:
        payload = {
            'chat_id': chat_id,
            'text': f'<blockquote>🥊{text}🥊</blockquote>',
            'parse_mode': 'HTML',
        }
    else:
        payload = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'Markdown',
        }

    response = requests.post(telegram_url, json=payload)
    if response.status_code == 200:
        print('Telegram alert sent successfully!')
    else:
        print('Failed to send Telegram alert!')
        print(response.text)


def alert_failed_job(context):
    message = f"\u26d4 *{context['task_instance']}*\n"
    message += f"DAG ID: *{context['task_instance'].dag_id}*\n"
    message += f"Task ID: *{context['task_instance'].task_id}*\n"
    message += f"Execution Time: *{context['task_instance'].execution_date}*\n"
    message += f"Error: *{context['exception']}*"[:500] + "\n"

    local_log_url = str(context['task_instance'].log_url)
    local_domain = re.findall(
        r'localhost:[0-9]+',
        local_log_url,
    )[0]
    sever_domain = 'https://airflow-ingestion.yes4all.com'
    log_url = sever_domain + local_log_url.split(local_domain)[-1]

    message += f"Log URL: [Link]({log_url})"

    send_message(
        text=message,
        html_mode=False,
    )
