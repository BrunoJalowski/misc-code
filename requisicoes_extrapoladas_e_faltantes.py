#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  2 10:06:33 2025

@author: brunojalowski
"""
# Importando pacotes
import requests
import regex as re
import pandas as pd
from decouple import config

#%% Baixando nomes dos arquivos do arquivo json

# Site de acesso aos logs
API_REST_ADRESS = config('API_REST_ADRESS')

# Lendo o arquivo json para uma lista de dicionários python
dados_json = requests.get(API_REST_ADRESS).json()

# Filtrando objetivos no formato 'controller.log.bkp*'
arquivos_bkp = [dado['name'] 
                for dado in dados_json
                if 'controller.log' in dado['name']] 

#%% Baixando dados 
FILE_SERVER = config('FILE_SERVER')

lista_url = [FILE_SERVER + str(nome) 
             for nome in arquivos_bkp]

#%% 
# Listando todas as linhas de todos os arquivos
dados = []
for url in lista_url:
    # Pegando dados pela url
    response = requests.get(url)
    dados.append(response.text.split('\n'))

# Transformando a lista de listas em uma lista unica
lista_txt = [x
             for xs in dados 
             for x in xs]

# Iterar sobre as linhas do arquivo e filtrar as linhas-alvo
linhas = [lista_txt[linha] 
          for linha in range(len(lista_txt)) 
          if '[INFO] [root] Process' in lista_txt[linha]]

# Selecionando zoom e a data-hora das linhas filtradas no formato 'YYYY-MM-DD HH'
reqs_obtidas = [re.findall(r'(\s[\d]{2}\s)', linha) +
                 re.findall(r'([\d]{4}\-[\d]{2}\-[\d]{2} [\d]{2})', linha)
                 for linha in linhas]

reqs_obtidas = sorted(reqs_obtidas)


#%% REQUISIÇÕES QUE EXTRAPOLARAM
# Filtrando datas em que a hora de termino extrapola a hora representada
reqs_extrapoladas = ["zoom" + data[0] + data[1] + ':00:00' 
                      for data in reqs_obtidas
                      if data[1][-2:] != data[-1][-2:]]

extrapoladas_zooms = [data.split(' ')[1]
                      for data in reqs_extrapoladas]

extrapoladas_datahora = [pd.to_datetime(data.split(' ')[2] +
                                        ' ' +
                                        data.split(' ')[-1])
                         for data in reqs_extrapoladas]

#%% REQUISIÇÕES FALTANTES
reqs_obtidas = ["zoom" + data[0] + data[1] + ':00:00'
                 for data in reqs_obtidas]

# Definindo limites do periodo registrado
reqs_unicas_obtidas = sorted(list(set(pd.to_datetime(data[7:])
                                       .timestamp()
                                       for data in reqs_obtidas)))

# Criando lista de data-hora de referência
datetime_ref = pd.date_range(
    start = pd.to_datetime(min(reqs_unicas_obtidas),
                           unit = 's'),
    end = pd.to_datetime(max(reqs_unicas_obtidas),
                         unit = 's'),
    freq = 'h')

reqs_unicas_obtidas = pd.to_datetime(reqs_unicas_obtidas, unit='s') 

datas_faltantes = [data
                   for data in datetime_ref
                   if data not in reqs_unicas_obtidas]

    
"""PRODUTOS FINAIS:
        extrapoladas_data-hora -> lista com valores de data-hora das requisições
    extrapoladas, em ordem cronológica
    
        extrapoladas_zooms -> lista de zooms das requisições extrapoladas, com
    índice condizente com a lista de data-hora extrapolada

        datas_faltantes -> lista com valores de data-hora das requisições não
    realizadas
"""

#%% Definição de processo de verificação das requisições faltantes e extrapoladas

# Pacotes necessários
import requests
import regex as re
import pandas as pd
from decouple import config

# Url de acesso aos logs
API_REST_ADRESS = config('API_REST_ADRESS')
    
# Url de acesso aos dados de cada arquivo log
FILE_SERVER = config('FILE_SERVER')







def requisicoes_extrapoladas_e_faltantes(API_REST_ADRESS, FILE_SERVER): 
    """
    Identifica e retorna listas com os valores de zooms e data-hora das 
    requisições que extrapolaram a hora que elas deveriam representar, além
    dos valores de data-hora das requisições não realizadas.

    Parameters
    ----------
    API_REST_ADRESS: constante
        Url de acesso aos nomes dos arquivos de registro
    
    FILE_SERVER: constante
        Url de acesso direto aos arquivos de registro
    
    Returns
    -------
    extrapoladas_data-hora : lista de valores datetime
        Lista com valores de data-hora das requisições extrapoladas, em ordem 
        cronológica.
        
    extrapoladas_zooms : lista de strings
        Lista de zooms das requisições extrapoladas, com índice condizente com
        a lista de data-hora extrapolada.
    
    datas_faltantes : lista de valores datetime
        Lista com valores de data-hora das requisições não realizadas.

    """    
    
    # Lendo o arquivo json para uma lista de dicionários python
    dados_json = requests.get(API_REST_ADRESS).json()

    # Filtrando objetos no formato 'controller.log.bkp*'
    arquivos_bkp = [dado['name'] 
                    for dado in dados_json
                    if 'controller.log' in dado['name']] 
    
    # Criando lista das urls de acesso para cada arquivo a ser baixado
    lista_url = [FILE_SERVER + str(nome) 
                 for nome in arquivos_bkp]
    
    # Listando todas as linhas de todos os arquivos de texto
    dados = []
    for url in lista_url:
        # Pegando dados pela url
        response = requests.get(url)
        dados.append(response.text.split('\n'))

    # Transformando a lista de listas em uma lista unica
    lista_txt = [x
                 for xs in dados 
                 for x in xs]

    # Iterar sobre as linhas do arquivo e filtrar linhas com
    # '[INFO] [root] Process'
    linhas = [lista_txt[linha] 
              for linha in range(len(lista_txt)) 
              if '[INFO] [root] Process' in lista_txt[linha]]

    # Selecionando zoom e a data-hora das linhas filtradas 
    # no formato 'YYYY-MM-DD HH'
    reqs_obtidas = [re.findall(r'(\s[\d]{2}\s)', linha) +
                     re.findall(r'([\d]{4}\-[\d]{2}\-[\d]{2} [\d]{2})', linha)
                     for linha in linhas]
    
    # Ordenar a lista de requisições
    reqs_obtidas = sorted(reqs_obtidas)
    
    # ----------------- REQUISIÇÕES EXTRAPOLADAS----------------------------
    
    # Filtrando datas em que a hora de término extrapola a hora representada
    reqs_extrapoladas = ["zoom" + data[0] + data[1] + ':00:00' 
                          for data in reqs_obtidas
                          if data[1][-2:] != data[-1][-2:]]

    extrapoladas_zooms = [data.split(' ')[1]
                          for data in reqs_extrapoladas]

    extrapoladas_datahora = [pd.to_datetime(data.split(' ')[2] +
                                            ' ' +
                                            data.split(' ')[-1])
                             for data in reqs_extrapoladas]
    
    # ----------------- REQUISIÇÕES FALTANTES ------------------------------

    # Selecionando zoom e data-hora obtidos e formatando a data-hora
    reqs_obtidas = [data[0] + data[1] + ':00:00'
                     for data in reqs_obtidas]

    # Definindo limites do período registrado
    reqs_unicas_obtidas = sorted(list(set(pd.to_datetime(data[3:])
                                           .timestamp()
                                           for data in reqs_obtidas)))

    # Criando lista de data-hora de referência
    datetime_ref = pd.date_range(
        start = pd.to_datetime(min(reqs_unicas_obtidas),
                               unit = 's'),
        end = pd.to_datetime(max(reqs_unicas_obtidas),
                             unit = 's'),
        freq = 'h')

    reqs_unicas_obtidas = pd.to_datetime(reqs_unicas_obtidas, unit='s') 
    
    # Criando lista de data-hora das requisições faltantes
    datas_faltantes = [data
                       for data in datetime_ref
                       if data not in reqs_unicas_obtidas]
    
    return (extrapoladas_datahora,
            extrapoladas_zooms,
            datas_faltantes) 
