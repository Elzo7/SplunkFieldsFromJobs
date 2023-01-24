from time import strftime, sleep
import socket
from urllib3.connection import HTTPSConnection
import requests
import xmltodict
import xml.etree.ElementTree as E
import pandas
from os import path

def getAllSearchesWholeMonth():
    data = {
        'search': 'search index=_audit action=search "search"=* earliest=-mon@ latest=now | fields search'
    }
    response = requests.post('https://192.168.231.160:8089/services/search/jobs', data=data, verify=False,auth=('admin', 'qaz123456'))
    job_tree = E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot()
    sid = job_tree.find('./sid').text
    searches_from_month=[]
    fields_from_month={}
    while(True):
        response = requests.get('https://192.168.231.160:8089/services/search/jobs/' + sid,data={'output_mode':'json'},verify=False,auth=('admin','qaz123456'))
        if(response.json()['entry'][0]['content']['dispatchState']=='DONE'):
            response = requests.get('https://192.168.231.160:8089/services/search/jobs/' + sid + '/results?count=10',verify=False, auth=('admin', 'qaz123456'))
            break
    job_tree=E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot()
    for result in job_tree:
        if(result.tag=='result'):
            searches_from_month.append(result.find("./field[@k='search']/value/text").text)
    print(searches_from_month)
    for search in searches_from_month:
        data={
            "search":search.strip('\'')
        }
        print(data)
        response = requests.post('https://192.168.231.160:8089/services/search/jobs',data=data,verify=False,auth=('admin','qaz123456'))
        if(response.status_code>=200 and response.status_code<300):
            search_sid=E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot().find('./sid').text
            command={
                'action':'finalize'
            }
            sleep(2)
            search_results=response.content.decode('utf-8')
            requests.post('https://192.168.231.160:8089/services/search/jobs/'+search_sid+"/control",data=command,verify=False,auth=('admin','qaz123456'))
            response=requests.get('https://192.168.231.160:8089/services/search/jobs/'+search_sid+'/results_preview',verify=False,auth=('admin','qaz123456'))
            used_fields_months = {}
            if (response.content.decode('utf-8') != ''):
                job_tree = E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot()
                print(response.content.decode('utf-8'))
                for result in job_tree:
                    t_index = result.find("./field[@k='index']")
                    if t_index is not None:
                        t_index = t_index.find('./value/text')
                        print(t_index.text)
                    t_source = result.find("./field[@k='source']")
                    if t_source is not None:
                        t_source = t_source.find('./value/text')
                    t_sourcetype = result.find("./field[@k='sourcetype']")
                    if t_sourcetype is not None:
                        t_sourcetype = t_sourcetype.find('./value/text')
                    for field in result:
                        if (field.tag == 'field'):
                            if t_index is not None and t_sourcetype is not None and t_source is not None:
                                used_fields_months[t_index.text] = used_fields_months.get(t_index.text, {})
                                if (used_fields_months[t_index.text].get(field.attrib['k'], False) == False and field.attrib['k'] != 'source' and field.attrib['k'] != 'sourcetype' and field.attrib['k'] != 'index'):

                                    fields_from_month[t_index.text] = fields_from_month.get(t_index.text, {})
                                    fields_from_month[t_index.text][t_source.text] = fields_from_month.get(t_index.text, {}).get(t_source.text, {})
                                    fields_from_month[t_index.text][t_source.text][t_sourcetype.text] = fields_from_month.get(t_index.text, {}).get(t_source.text, {}).get(t_sourcetype.text, {})
                                    fields_from_month[t_index.text][t_source.text][t_sourcetype.text][field.attrib['k']] = fields_from_month.get(t_index.text, {}).get(t_source.text,{}).get(t_sourcetype.text, {}).get(field.attrib['k'], 0) + 1
                                    used_fields_months[t_index.text][field.attrib['k']] = used_fields_months.get(t_index.text).get(field.attrib['k'], True)

    if (fields_from_month != {}):
        data=[]
        for index in fields_from_month:
            pdata={'index':index}
            for source in fields_from_month[index]:
                pdata['source']=source
                for sourcetype in fields_from_month[index][source]:
                        pdata['sourcetype']=sourcetype
                        print(pdata)
                        for pair in fields_from_month[index][source][sourcetype]:
                            pdata[pair]=fields_from_month[index][source][sourcetype][pair]
            data.append(pdata)
        dfm = pandas.DataFrame(data=data)
        dfm = dfm.fillna(0)
        dfm.to_csv('report_month.csv', index=False)
        print(dfm)

def getFieldsFromJobs():
    response = requests.get('https://192.168.231.160:8089/services/search/jobs', verify=False,
                            auth=('admin', 'qaz123456'))
    if (path.exists('report.csv')):
        test_fields = {}
        pomoc = pandas.read_csv('report.csv').to_dict('records')
        for record in pomoc:
            test_fields[record['index']] = test_fields.get(record['index'], {})
            test_fields[record['index']][record['source']] = test_fields[record['index']].get(record['source'], {})
            test_fields[record['index']][record['source']][record['sourcetype']] = test_fields[record['index']][
                record['source']].get(record['sourcetype'], {})
            for de in record:
                if (de != 'index' and de != 'source' and de != 'sourcetype'):
                    test_fields[record['index']][record['source']][record['sourcetype']][de] = \
                    test_fields[record['index']][record['source']][record['sourcetype']].get(de, record[de])
    else:
        test_fields = {}
    index_initialized = {}
    dict_saved_search = xmltodict.parse(response.content)
    saved_searches = dict_saved_search.get('feed').get('entry')
    search_ids = []
    if (len(saved_searches) > 1):
        for search in saved_searches:
            search_ids.append(search.get('id'))
    else:
        saved_searches.get('id')
    for id in search_ids:
        url = id + '/results_preview?count=0'
        response = requests.get(url, verify=False, auth=('admin', 'qaz123456'))
        used_fields_for_index = {}
        used_fields_for_source = {}
        used_fields_for_sourcetype = {}
        used_fields = {}
        if (response.content.decode('utf-8') != ''):
            job_tree = E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot()
            for result in job_tree:
                t_index = result.find("./field[@k='index']")
                if t_index is not None:
                    t_index = t_index.find('./value/text')
                t_source = result.find("./field[@k='source']")
                if t_source is not None:
                    t_source = t_source.find('./value/text')
                t_sourcetype = result.find("./field[@k='sourcetype']")
                if t_sourcetype is not None:
                    t_sourcetype = t_sourcetype.find('./value/text')
                for field in result:
                    if (field.tag == 'field'):
                        if t_index is not None and t_sourcetype is not None and t_source is not None:
                            used_fields[t_index.text] = used_fields.get(t_index.text, {})
                            if (used_fields[t_index.text].get(field.attrib['k'], False) == False and field.attrib[
                                'k'] != 'source' and field.attrib['k'] != 'sourcetype' and field.attrib[
                                'k'] != 'index'):
                                test_fields[t_index.text] = test_fields.get(t_index.text, {})
                                test_fields[t_index.text][t_source.text] = test_fields.get(t_index.text, {}).get(
                                    t_source.text, {})
                                test_fields[t_index.text][t_source.text][t_sourcetype.text] = test_fields.get(
                                    t_index.text, {}).get(t_source.text, {}).get(t_sourcetype.text, {})
                                test_fields[t_index.text][t_source.text][t_sourcetype.text][
                                    field.attrib['k']] = test_fields.get(t_index.text, {}).get(t_source.text, {}).get(
                                    t_sourcetype.text, {}).get(field.attrib['k'], 0) + 1
                                used_fields[t_index.text][field.attrib['k']] = used_fields.get(t_index.text).get(
                                    field.attrib['k'], True)
    if (test_fields != {}):
        data = []
        for index in test_fields:
            pdata = {'index': index}
            print(pdata)
            for source in test_fields[index]:
                pdata['source'] = source
                for sourcetype in test_fields[index][source]:
                    pdata['sourcetype'] = sourcetype
                    for pair in test_fields[index][source][sourcetype]:
                        pdata[pair] = test_fields[index][source][sourcetype][pair]
            data.append(pdata)
        df = pandas.DataFrame(data=data)
        df = df.fillna(0)
        df.to_csv('report.csv', index=False)
        print(df)


if __name__ == '__main__':
    HTTPSConnection.default_socket_options = (
        HTTPSConnection.default_socket_options+[
        (socket.SOL_SOCKET,socket.SO_KEEPALIVE,1),
        (socket.SOL_TCP,socket.TCP_KEEPIDLE,45),
        (socket.SOL_TCP,socket.TCP_KEEPINTVL,10),
        (socket.SOL_TCP,socket.TCP_KEEPCNT,6)
    ]
    )
    #Pobranie danych z całego miesiaca
    getAllSearchesWholeMonth()
    #getFieldsFromJobs()

