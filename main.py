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
    df_t = pandas.DataFrame(columns=['index','source','sourcetype'])
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
        print(df_t)

def getFieldsFromJobs():
    response = requests.get('https://192.168.231.160:8089/services/search/jobs', verify=False,
                            auth=('admin', 'qaz123456'))
    if (path.exists('report.csv')):
        test_df=pandas.read_csv('report.csv')
    else:
        test_df =pandas.DataFrame(columns=['index','source','sourcetype'])
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
        used_fields = {}
        if (response.content.decode('utf-8') != ''):
            job_tree = E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot()
            for result in job_tree:
                t_index = result.find("./field[@k='index']")
                if t_index is not None:
                    t_index = t_index.find('./value/text')
                    filter_index= test_df['index'] == t_index.text
                t_source = result.find("./field[@k='source']")
                if t_source is not None:
                    t_source = t_source.find('./value/text')
                    filter_source = test_df['source'] == t_source.text
                t_sourcetype = result.find("./field[@k='sourcetype']")
                if t_sourcetype is not None:
                    t_sourcetype = t_sourcetype.find('./value/text')
                    filter_sourcetype = test_df['sourcetype'] ==t_sourcetype.text
                if t_index is not None and t_sourcetype is not None and t_source is not None:
                    if not ((filter_index) & (filter_sourcetype) & (filter_source)).any():
                        pomoc = {'index': t_index.text, 'source': t_source.text, 'sourcetype': t_sourcetype.text}
                        test_df2 = pandas.DataFrame(data=pomoc, index=[0])
                        test_df = pandas.concat([test_df, test_df2], ignore_index=True)
                        test_df = test_df.fillna(0)
                for field in result:
                    if (field.tag == 'field'):
                        if t_index is not None and t_sourcetype is not None and t_source is not None:
                            used_fields[t_index.text] = used_fields.get(t_index.text, {})
                            used_fields[t_index.text][t_source.text] = used_fields[t_index.text].get(t_source.text, {})
                            used_fields[t_index.text][t_source.text][t_sourcetype.text] = used_fields[t_index.text][t_source.text].get(t_sourcetype.text, {})
                            if not field.attrib['k'] in test_df.columns:
                                test_df[field.attrib['k']] = 0
                            if (used_fields[t_index.text][t_source.text][t_sourcetype.text].get(field.attrib['k'], False) == False and field.attrib[
                                'k'] != 'source' and field.attrib['k'] != 'sourcetype' and field.attrib[
                                'k'] != 'index'):
                                test_df[field.attrib['k']]=test_df[field.attrib['k']].mask(filter_source&filter_sourcetype&filter_index,test_df[field.attrib['k']]+1)
                                used_fields[t_index.text][t_source.text][t_sourcetype.text][field.attrib['k']] = used_fields[t_index.text][t_source.text][t_sourcetype.text].get(
                                    field.attrib['k'], True)
        test_df.to_csv('report.csv', index=False)
        print(test_df)



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
    #getAllSearchesWholeMonth()
    getFieldsFromJobs()

