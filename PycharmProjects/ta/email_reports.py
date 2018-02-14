
import shared.email as se
import contract_utilities.expiration as exp
import shared.directory_names as dn
import ta.strategy as ts
import ta.expiration_followup as ef


def send_hrsn_report(**kwargs):

    daily_dir = dn.get_directory_name(ext='daily')

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    ibo_dir = ts.create_strategy_output_dir(strategy_class='os', report_date=report_date)
    cov_data_integrity = ''

    try:
        with open(ibo_dir + '/' + 'covDataIntegrity.txt','r') as text_file:
            cov_data_integrity = text_file.read()
    except Exception:
        pass

    try:
        expiration_report = ef.get_expiration_report(report_date=report_date, con=kwargs['con'])
        expiration_report = expiration_report[expiration_report['tr_days_2roll'] < 5]

        if expiration_report.empty:
            expiration_text = 'No near expirations.'
        else:
            expiration_text = 'Check for approaching expirations!'
    except Exception:
        expiration_text = 'Check expiration report for errors!'

    se.send_email_with_attachment(subject='hrsn_' + str(report_date),
                                  email_text='cov_data_integrity: ' + cov_data_integrity + "\r\n" + expiration_text,
                                  attachment_list = [daily_dir + '/' + 'pnl_' + str(report_date) + '.xlsx', daily_dir +
                                                     '/' + 'followup_' + str(report_date) + '.xlsx'])

