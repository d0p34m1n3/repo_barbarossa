
import shared.email as se
import contract_utilities.expiration as exp
import shared.directory_names as dn


def send_hrsn_report(**kwargs):

    daily_dir = dn.get_directory_name(ext='daily')

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    se.send_email_with_attachment(subject='hrsn_' + str(report_date),
                                  attachment_list = [daily_dir + '/' + 'pnl_' + str(report_date) + '.xlsx', daily_dir + '/' + 'followup_' + str(report_date) + '.xlsx'])

