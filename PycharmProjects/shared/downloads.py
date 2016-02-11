
import csv
import io
import urllib.request
import pandas as pd


def download_csv_from_web(**kwargs):

    web_address = kwargs['web_address']

    if 'data_frame_outQ' in kwargs.keys():
        data_frame_outQ = kwargs['data_frame_outQ']
    else:
        data_frame_outQ = True

    if 'first_row_columnsQ' in kwargs.keys():
        first_row_columnsQ = kwargs['first_row_columnsQ']
    else:
        first_row_columnsQ = True

    webpage = urllib.request.urlopen(web_address)
    datareader = csv.reader(io.TextIOWrapper(webpage))
    data_out = list(datareader)

    if data_frame_outQ:
        if first_row_columnsQ:
            data_frame_out = pd.DataFrame(data_out[1:], columns=data_out[0])
        else:
            data_frame_out = pd.DataFrame(data_out)

        return data_frame_out
    else:
        return data_out


def download_txt_from_web(**kwargs):

     web_address = kwargs['web_address']
     webpage = urllib.request.urlopen(web_address)
     text_out = webpage.read()
     return text_out.splitlines()

