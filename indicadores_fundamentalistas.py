import pandas as pd
from sqlalchemy import asc
from conexao_banco import conexao_aws
import os
from datetime import datetime
from datetime import date


class indicadores_fundamentalistas_brasil:

    def __init__(self, primeiro_dia_indicador, ultimo_dia_indicador, tuple_cod_empresas = ()):
        
        usuario_sql = os.getenv('usuario_sql')
        senha_sql = os.getenv('senha_sql')
        self.aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='edu_db')
        self.aws.iniciar_conexao()
        self.primeiro_dia_indicador = datetime.strptime(primeiro_dia_indicador, '%Y-%m-%d').date()
        self.ultimo_dia_indicador = datetime.strptime(ultimo_dia_indicador , '%Y-%m-%d').date()
        self.setores_restritos = ['Holding', 'Ignorados']

        if tuple_cod_empresas != ():
           
            self.todos_os_ativos = False
            self.tuple_cod_empresas = tuple_cod_empresas

        else:
            
            self.todos_os_ativos = True


    def coletando_balancos_cvm(self):

        if self.todos_os_ativos:

            self.dados_fundamentalistas = pd.read_sql(f'''SELECT c.cod, df.data_ref, df.data_reb, df.cod_empresa, df.nome_indicador, df.periodicidade, df.valor
                                    FROM dados_fundamentalistas AS df
                                    LEFT JOIN cadastro_empresas AS c USING(cod_empresa)
                                    WHERE  (df.data_reb >= '{self.primeiro_dia_indicador}' AND df.data_reb < '{self.ultimo_dia_indicador}') '''
                                    , con=self.aws.engine)

            self.cotacoes = pd.read_sql(f'''SELECT c.cod_empresa, p.cod, p.date, p.value_price from price as p 
                                            LEFT JOIN cadastro_empresas as c 
                                            where p.data >= {self.primeiro_dia_indicador} and p.data <= {self.ultimo_dia_indicador}''', con = self.aws.engine)

        else:

            try:

                
                self.dados_fundamentalistas = pd.read_sql(f'''SELECT c.cod, df.data_ref, df.data_reb, df.cod_empresa, df.nome_indicador, df.periodicidade, df.valor
                                        FROM dados_fundamentalistas AS df
                                        LEFT JOIN cadastro_empresas AS c USING(cod_empresa)
                                        WHERE  (df.data_reb >= '{self.primeiro_dia_indicador}' AND df.data_reb < '{self.ultimo_dia_indicador}') and c.cod_empresa = {self.tuple_cod_empresas}  '''
                                        , con=self.aws.engine)

                self.cotacoes = pd.read_sql(f'''SELECT c.cod_empresa, p.cod, p.date, p.value_price from price as p 
                                                LEFT JOIN cadastro_empresas as c 
                                                where p.data >= {self.primeiro_dia_indicador} and p.data <= {self.ultimo_dia_indicador}
                                                and c.cod_empresa = {self.tuple_cod_empresas}  ''', con = self.aws.engine)


            except:

                
                self.dados_fundamentalistas = pd.read_sql(f'''SELECT c.cod, df.data_ref, df.data_reb, df.cod_empresa, df.nome_indicador, df.periodicidade, df.valor
                                        FROM dados_fundamentalistas AS df
                                        LEFT JOIN cadastro_empresas AS c USING(cod_empresa)
                                        WHERE  (df.data_reb >= '{self.primeiro_dia_indicador}' AND df.data_reb < '{self.ultimo_dia_indicador}') 
                                         and c.cod_empresa in {self.tuple_cod_empresas}  '''
                                        , con=self.aws.engine)

                self.cotacoes = pd.read_sql(f'''SELECT c.cod_empresa, p.cod, p.date, p.value_price from price as p 
                                                LEFT JOIN cadastro_empresas as c 
                                                where p.data >= {self.primeiro_dia_indicador} and p.data <= {self.ultimo_dia_indicador}
                                                 and c.cod_empresa in {self.tuple_cod_empresas}  ''', con = self.aws.engine)


        self.empresas = self.dados_fundamentalistas['cod_empresa'].sort_values(ascending=True).unique()
    

    def calculando_indicadores_fundamentalistas(self):

        self.lista_df_indicadores = []

        indicadores = ['p/l', 'ev/ebit', 'p/vpa', 'div_yield', 'p/fcf', 'ev/ebitda', 'lpa', 'margem_bruta', 'margem_ebitda', 'div_liquida/pl', 
                        'div_liquida/ebitda', 'div_liquida/ebit', 'div_bruta/patrimonio_liquido', 'indice_de_cobertura_de_juros']

        for empresa in self.empresas:

            dados_fundamentalistas_empresas = self.dados_fundamentalistas[self.dados_fundamentalistas['cod_empresa'] == empresa]

            datas = self.cotacoes[self.cotacoes['cod_empresa'] == empresa].unique()

            data_demonstracao = date(2000, 1, 1)

            for data in datas:

                demonstracao_mais_atual = max(dados_fundamentalistas_empresas[dados_fundamentalistas_empresas['data_reb'] < data]['data_reb'])

                if data_demonstracao < demonstracao_mais_atual:

                    data_demonstracao = demonstracao_mais_atual

                    dados_fundamentalistas_empresa_na_data = dados_fundamentalistas_empresas[dados_fundamentalistas_empresas['date_reb'] < data]

                    dados_fundamentalistas_empresa_na_data = dados_fundamentalistas_empresa_na_data[dados_fundamentalistas_empresa_na_data['date_ref'] ==  max(dados_fundamentalistas_empresa_na_data['date_ref'])]

                    dados_fundamentalistas_empresa_na_data = dados_fundamentalistas_empresa_na_data[dados_fundamentalistas_empresa_na_data['date_reb'] < max(dados_fundamentalistas_empresa_na_data['date_reb'])]

                    dados_fundamentalistas_empresa_na_data = dados_fundamentalistas_empresa_na_data[dados_fundamentalistas_empresa_na_data['periodiciade'].isin(['12m', 'pontual'])]

                    lucro = dados_fundamentalistas_empresa_na_data[dados_fundamentalistas_empresa_na_data['nome_indicador'] == 'lucro']






















