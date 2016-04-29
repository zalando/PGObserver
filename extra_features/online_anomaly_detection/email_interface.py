import logging
import time
import smtplib
from datetime import timedelta, datetime
from email.mime.text import MIMEText
import yaml


EMAIL_CONFIG_FILE = 'email_config.yaml'
email_config = None


def read_email_config_from_file(file_path=EMAIL_CONFIG_FILE):
    global email_config
    with open(file_path) as f:
        email_config = yaml.load(f.read())
    return email_config


def compose_email_subject(host, metric, ident, change_pct, change_pct_limit):
    if not email_config:
        read_email_config_from_file()
    return """[OLAD] Irregularity detected - [{host}][{metric}][{ident}] change pct: {change_pct} % (limit: {change_pct_limit} %)""".format(
        host=host, metric=metric, ident=ident, change_pct=change_pct, change_pct_limit=change_pct_limit)


def compose_email_body(host, metric, ident, change_pct, change_pct_limit, additional_message='', metrics='', from_dt=None, to_dt=None, last_mva=None, historic_avg=None):
    if not email_config:
        read_email_config_from_file()
    from_epoch_ms = int(from_dt.strftime('%s')) * 1000 if from_dt else ''
    to_epoch_ms = int(to_dt.strftime('%s')) * 1000 if to_dt else ''
    datetime1 = str(from_dt) if from_dt else ''
    datetime2 = str(to_dt) if to_dt else ''
    msg = \
        """An irregularity has been registered for host "{host}", metric "{metric}", ident "{ident}".

Change pct: {change_pct} % (limit: {change_pct_limit})
Timerange: ["{datetime1}" .. "{datetime2}"]


Additional message: {additional_message}

Host overview on PgObserver: {pgo_url}/{host}
Host overview on Grafana: {grafana_base_url}{grafana_host_overview_with_placeholders}

{pgo_ident_details}
{grafana_ident_details}

"""
    pgo_ident_details = ''
    grafana_ident_details = ''
    if metric == 'cpu_load':
        grafana_ident_details = 'Load details on Grafana: ' + email_config['GRAFANA_BASE_URL'] + email_config['GRAFANA_CPU_LOAD_URL_WITH_PLACEHOLDERS'].format(host=host, from_epoch_ms=from_epoch_ms, to_epoch_ms=to_epoch_ms)
    if metric == 'seq_scans':
        pgo_ident_details = 'Table details on PgObserver: ' + email_config['PGO_URL'] + email_config['PGO_TABLE_DETAILS_URL_WITH_PLACEHOLDERS'].format(pgo_url=email_config['PGO_URL'], host=host, ident=ident)
    if metric == 'sproc_runtime':
        pgo_ident_details = 'Sproc details on PgObserver: ' + email_config['PGO_URL'] + email_config['PGO_SPROC_DETAILS_URL_WITH_PLACEHOLDERS'].format(pgo_url=email_config['PGO_URL'], host=host, ident=ident)

    return msg.format(host=host, metric=metric, ident=ident, change_pct=change_pct, change_pct_limit=change_pct_limit,
                      pgo_url=email_config['PGO_URL'], grafana_ident_details=grafana_ident_details,
                      additional_message=additional_message, metrics=metrics,
                      from_epoch_ms=from_epoch_ms, to_epoch_ms=to_epoch_ms, datetime1=datetime1, datetime2=datetime2,
                      grafana_base_url=email_config['GRAFANA_BASE_URL'],
                      grafana_host_overview_with_placeholders=email_config['GRAFANA_HOST_OVERVIEW_WITH_PLACEHOLDERS'].format(host=host), pgo_ident_details=pgo_ident_details)


def try_send_mail(to_email_addresses, subject, message=''):
    if not to_email_addresses:
        raise Exception('to_email_addresses empty!')

    try:
        # Create a text/plain message
        msg = MIMEText(message)
        msg['To'] = to_email_addresses
        msg['From'] = email_config['SENDER']
        msg['Subject'] = subject

        s = smtplib.SMTP(email_config['MAILHOST'])
        s.sendmail(email_config['SENDER'], to_email_addresses.split(','), msg.as_string())
        s.quit()
        return True
    except:
        logging.exception('failed to send email to [%s]', to_email_addresses)
        return False


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    read_email_config_from_file('email_config_zalando.yaml')
    to = 'test@zalando.de'
    host = 'bmdb'
    metric = 'cpu_load'
    metric = 'seq_scans'
    ident = 'load_5'
    change_pct = 99
    change_pct_limit = 50
    logging.info('sending email to %s about change in %s.%s by %s %%', to, host, metric, change_pct)
    sub = compose_email_subject(host, metric, ident, change_pct, change_pct_limit)
    print ('subject:', sub)
    msg = compose_email_body(host, metric, ident, change_pct, change_pct_limit,
                             from_dt=datetime.now() - timedelta(minutes=30), to_dt=datetime.now())
    print ('message:', msg)
    print (try_send_mail(to, sub, msg))
