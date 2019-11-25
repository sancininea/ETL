#####################################################################################
#                                                                                   #
# Function get_xml                                                                  #
# This function get all the data from a list of xml files of CFDI type.             #
#                                                                                   #
# Parameters: filesList - An array of strings that contains the names of the files  #
# retrieve. Use filesList = glob.glob(".\<folder name>\*.xml") to set this list.    #
#                                                                                   #
# Returns: xmls - List of dictionaries with the data to store. The lenght varies,   #
# the files can have different data.                                                #
#                                                                                   #
#####################################################################################
def get_xml(filesList):
    
    import xml.etree.ElementTree as et

    xmls = []
    for files in filesList:

        xtree = et.parse(files)
        xroot = xtree.getroot()
        #print(files)
        version = xroot.attrib['Version']
        try:
            folio = xroot.attrib['Folio']
        except:
            folio=""
        fecha = xroot.attrib['Fecha']
        try:
            f_pago = xroot.attrib['FormaPago']
        except:
            f_pago = "NA"
        subtot = xroot.attrib['SubTotal']
        moneda = xroot.attrib['Moneda']
        try:
            t_cambio = xroot.attrib['TipoCambio']
        except:
            t_cambio = "1"
        total = xroot.attrib['Total']
        tipo_comp = xroot.attrib['TipoDeComprobante']
        try:
            met_pago = xroot.attrib['MetodoPago']
        except:
            met_pago = "NA"

        for node in xroot: 
            if node.tag == '{http://www.sat.gob.mx/cfd/3}Emisor':
                rfc_emi = node.attrib['Rfc']
                try:
                    nom_emi = node.attrib['Nombre']
                except:
                    nom_emi = ""
            elif node.tag == '{http://www.sat.gob.mx/cfd/3}Receptor':
                rfc_rec = node.attrib['Rfc']
                try:
                    nom_rec = node.attrib['Nombre']
                except:
                    nom_rec = ""
                try:
                    uso_cfdi = node.attrib['UsoCFDI']
                except:
                    uso_cfdi = ""
            elif node.tag == '{http://www.sat.gob.mx/cfd/3}Conceptos':
                conceptos = []
                for subnode in node:
                    clave_prod = subnode.attrib['ClaveProdServ'] 
                    cantidad = subnode.attrib['Cantidad']
                    clave_unidad = subnode.attrib['ClaveUnidad'] 
                    try:
                        unidad = subnode.attrib['Unidad'] 
                    except:
                        unidad = "Unidad de Servicio"
                    descripcion= subnode.attrib['Descripcion']
                    valor_unitario = subnode.attrib['ValorUnitario']
                    importe = subnode.attrib['Importe'] 
                    impuestos = []
                    for imp in subnode:
                        traslados = []
                        for tras in imp:
                            for tt in tras:
                                try:
                                    impuesto = tt.attrib['Impuesto']
                                except:
                                    impuesto = "000"
                                try:
                                    tasa = tt.attrib['TasaOCuota']
                                except:
                                    tasa="0.0"
                                try:
                                    importe_imp = tt.attrib['Importe']
                                except:
                                    importe_imp = "0"
                            traslados.append({'impuesto': impuesto, 'tasa':tasa, 'importe_imp':importe_imp})
                        impuestos.append(traslados)
                    conceptos.append({'clave_prod':clave_prod, 'cantidad':cantidad, 'clave_unidad':clave_unidad, 'unidad':unidad, 'descripcion':descripcion, 'valor_unitario':valor_unitario, 'importe':importe, 'impuestos':impuestos})
            elif node.tag == '{http://www.sat.gob.mx/cfd/3}Complemento':
                uuid = ""
                for subnode in node:
                    #print(subnode.tag)
                    if subnode.tag == '{http://www.sat.gob.mx/TimbreFiscalDigital}TimbreFiscalDigital':
                        uuid = subnode.attrib['UUID']
        row = {
            'version':version ,
            'folio':folio ,
            'fecha':fecha ,
            'f_pago':f_pago ,
            'subtot':subtot ,
            'moneda':moneda ,
            't_cambio':t_cambio ,
            'total':total ,
            'tipo_comp':tipo_comp ,
            'met_pago':met_pago ,
            'rfc_emi':rfc_emi ,
            'nom_emi':nom_emi ,
            'rfc_rec':rfc_rec ,
            'nom_rec': nom_rec,
            'uso_cfdi':uso_cfdi ,
            'conceptos':conceptos,
            'uuid': uuid
        }
        xmls.append(row)
    return xmls

#####################################################################################
#                                                                                   #
# Function insert_xml                                                               #
# This function insert the data in a list of get_xml format inside PstgreSQL tables #
#                                                                                   #
# Parameters: A List with the CFDI-XML data  in the get_xml format                  #
#                                                                                   #
# Returns: None. Make the data insertion                                            #
#                                                                                   #
#####################################################################################

def insert_xml(cfdiXML):
    import pandas as pd
    from sqlalchemy import create_engine
    from config import usrPW
    from datetime import datetime
    
    engine = create_engine(f'postgresql://{usrPW}localhost:5432/etl_db')
        
    for row in cfdiXML:
        main_xml = {
            'uuid': row['uuid'],
            'version' : row['version'],
            'folio': row['folio'],
            'fecha': datetime.strptime(row['fecha'], "%Y-%m-%dT%H:%M:%S"),
            'f_pago': row['f_pago'],
            'subtot': float(row['subtot']),
            'moneda': row['moneda'],
            't_cambio': row['t_cambio'],
            'total': float(row['total']),
            'tipo_comp': row['tipo_comp'],
            'met_pago': row['met_pago'],
            'rfc_emi': row['rfc_emi'],
            'nom_emi': row['nom_emi'],
            'rfc_rec': row['rfc_rec'],
            'nom_rec': row['nom_rec'],
            'uso_cfdi': row['uso_cfdi']
        }
        
        main_xml_pd = (
            pd.DataFrame(main_xml , index=[0])
            .set_index("uuid")
        )
        
        main_xml_pd.to_sql(name="main_xml", con=engine, if_exists="append", index=True)


#####################################################################################
#                                                                                   #
# Function scrap_sat                                                                #
# This function scraps the files of the Tax Authority black list from their page    #
#                                                                                   #
# Parameters:                                                                       #
#             folder - Folder where the files are gonna be stored                   #
#             driver - Chrome driver file#
#                                                                                   #
# Returns: Files saved.                                                             #
#                                                                                   #
#####################################################################################
def scrap_sat(folder, driver):
    # Dependencies
    from bs4 import BeautifulSoup
    import requests
    from splinter import Browser
    import os
    import urllib

    # URL of page to be scraped
    url = 'http://omawww.sat.gob.mx/cifras_sat/Paginas/datos/vinculo.html?page=ListCompleta69.html'

    response = requests.get(url)

    # response.text
    soup = BeautifulSoup(response.text, 'html.parser')

    executable_path = {'executable_path': driver}
    browser = Browser('chrome', **executable_path, headless=False)
    browser.visit(url)

    html = browser.html
    soup = BeautifulSoup(html, 'html.parser')

    base_url = "http://omawww.sat.gob.mx/cifras_sat/Paginas/datos/"

    iframes = soup.find_all('iframe')

    for iframe in iframes:
    
        response = urllib.request.urlopen(base_url + iframe.attrs['src'])
        iframe_soup = BeautifulSoup(response)
    
        links = iframe_soup.find_all("a")
    
        for link in links:
            testfile = urllib.request.URLopener()
            fname = link["href"].split("/")[-1]
            testfile.retrieve(link["href"], f"{folder}/{fname}")
    browser.quit()

#####################################################################################
#                                                                                   #
# Function query_xml                                                                #
# This function creates query's and return tables in html format to put in a page   #
#                                                                                   #
# Parameters:                                                                       #
#             None                                                                  #
#                                                                                   #
# Returns: html tables with format                                                  #
#                                                                                   #
#####################################################################################
def query_xml():
    from sqlalchemy import create_engine
    from config import usrPW
    import pandas as pd

    engine = create_engine(f'postgresql://{usrPW}localhost:5432/etl_db')
    conn = engine.connect()

    query = '''
    select TO_CHAR(count(uuid), '9,999,999,999') as "Cantidad de comprobantes", TO_CHAR(sum(total)/1000000, '$9,999,999,999D99') as "Total (Millones de pesos)"
    from main_xml 
    where tipo_comp = 'I'
    order by sum(total)/1000000 desc
    '''
    result_qry = pd.read_sql(query, conn)
    html_table = result_qry.to_html(index=False)
    html_table = html_table.replace('\n', '')
    total_table = html_table.replace('<table border="1" class="dataframe">', '<table border="1" class="table table-striped table-sm table-condensed">')
    total_table = total_table.replace('<tr style="text-align: right;">', '<tr style="text-align: left;">')
    
    query = '''
    select lista_negra.supuesto as "Supuesto", TO_CHAR(count(uuid), '9,999,999,999') as "Cantidad de comprobantes", TO_CHAR(sum(total)/1000000, '$9,999,999,999D99') as "Total (Millones de pesos)" 
    from main_xml inner join lista_negra on main_xml.rfc_emi = lista_negra.rfc
    where tipo_comp = 'I'
    group by lista_negra.supuesto
    order by sum(total) desc
    '''
    result_qry = pd.read_sql(query, conn)
    html_table = result_qry.to_html(index=False)
    html_table = html_table.replace('\n', '')
    bad_a_table = html_table.replace('<table border="1" class="dataframe">', '<table border="1" class="table table-striped table-sm table-condensed">')
    bad_a_table = bad_a_table.replace('<tr style="text-align: right;">', '<tr style="text-align: left;">')

    query = '''
    select main_xml.rfc_emi as "Proveedor", lista_negra.supuesto as "Supuesto", TO_CHAR(count(uuid), '9,999,999,999') as "Cantidad de comprobantes", TO_CHAR(sum(total), '$9,999,999,999D99') as "Total" 
    from main_xml inner join lista_negra on main_xml.rfc_emi = lista_negra.rfc
    where tipo_comp = 'I'
    group by main_xml.rfc_emi, lista_negra.supuesto
    order by sum(total) desc
    '''
    result_qry = pd.read_sql(query, conn)
    html_table = result_qry.to_html(index=False)
    html_table = html_table.replace('\n', '')
    bad_detail_table = html_table.replace('<table border="1" class="dataframe">', '<table border="1" class="table table-striped table-sm table-condensed">')
    bad_detail_table = bad_detail_table.replace('<tr style="text-align: right;">', '<tr style="text-align: left;">')

    return total_table, bad_a_table, bad_detail_table


#####################################################################################
#                                                                                   #
# Function load_xml                                                                 #
# This function loads all the files in one folder to a database.                    #
#                                                                                   #
# Parameters:                                                                       #
#             folder - Name of the folder where the resources are.                  #
#                                                                                   #
# Returns: files inserted in the database                                           #
#                                                                                   #
#####################################################################################
def load_xml(folder, usr):
    import glob
    import pandas as pd                          
    from sqlalchemy import create_engine

    # List of files
    filesList = glob.glob(folder)
    list_data = []
    # Loop thru files
    for files in filesList:
        data = pd.read_csv(files, encoding="latin-1")
        list_data.append(data)
    list_dt= pd.DataFrame(columns=('RFC', 'RAZÓN SOCIAL','SUPUESTO'))
    #cortar la lista
    for x in list_data:
        try:
            nuevo = x.loc[:,['RFC', 'RAZÓN SOCIAL','SUPUESTO']]
            list_dt = pd.concat([list_dt,nuevo])
        except:
            print(x.head())
    list_dt.rename({'RFC': 'rfc', 'RAZÓN SOCIAL': 'nombre', 'SUPUESTO' : 'supuesto'}, axis=1, inplace=True)
    connection_string=f'postgresql://{usr}localhost:5432/etl_db'
    engine = create_engine(connection_string)
    list_dt.to_sql(name="lista_negra", con=engine, if_exists="append",index=False)






