import pandas as pd
from pandas_datareader import test
from conexao_banco import conexao_aws
import os
from datetime import datetime


class balanco_patrimonial_indicadores:

    def __init__(self, primeiro_dia_indicador, ultimo_dia_indicador, tuple_cod_empresas = ()):
        
        usuario_sql = os.getenv('usuario_sql')
        senha_sql = os.getenv('senha_sql')
        self.aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='edu_db')
        self.aws.iniciar_conexao()
        self.primeiro_dia_indicador = datetime.strptime(primeiro_dia_indicador, '%Y-%m-%d').date()
        self.ultimo_dia_indicador = datetime.strptime(ultimo_dia_indicador , '%Y-%m-%d').date()
        self.setores_restritos = ['Bancos', 'Holding', 'Ignorados']

        if tuple_cod_empresas != ():
           
            self.todos_os_ativos = False
            self.tuple_cod_empresas = tuple_cod_empresas

        else:
            
            self.todos_os_ativos = True


    def coletando_balancos_cvm(self):

        if self.todos_os_ativos:

            self.demonstracoes_sql = pd.read_sql(f'''SELECT c.id_doc, c.data_reb, c.data_ref, d.con_ind, d.numero_conta,
                                    d.nome_conta, d.valor_conta, c.cod_empresa, d.tipo_dem, c.tipo_doc
                                    FROM cadastro_dem AS c
                                    LEFT JOIN dados_financeiros AS d USING(id_doc)
                                    WHERE  d.tipo_dem in (2, 3) AND
                                    (c.data_ref >= '{self.primeiro_dia_indicador}' AND c.data_ref < '{self.ultimo_dia_indicador}') '''
                                    , con=self.aws.engine)


        else:

            self.demonstracoes_sql = pd.read_sql(f'''SELECT c.id_doc, c.data_reb, c.data_ref, d.con_ind, d.numero_conta,
                                    d.nome_conta, d.valor_conta, c.cod_empresa, d.tipo_dem, c.tipo_doc
                                    FROM cadastro_dem AS c
                                    LEFT JOIN dados_financeiros AS d USING(id_doc)
                                    WHERE  d.tipo_dem in (2, 3) AND
                                    (c.data_ref >= '{self.primeiro_dia_indicador}' AND c.data_ref < '{self.ultimo_dia_indicador}') AND c.cod_empresa = {self.tuple_cod_empresas}'''
                                    , con=self.aws.engine)

        self.documentos = self.demonstracoes_sql['id_doc'].unique()

    def coletando_dados_de_cadastro(self):

        self.cadastro_empresas = pd.read_sql('''SELECT cod, cod_empresa, setor from cadastro_empresas''', con= self.aws.engine)

    def pegando_indicadores_ativo(self):

        self.lista_df_indicadores = []

        for documento in self.documentos:

            print(documento, "ativo")

            demonstracao_completa = self.demonstracoes_sql.query(f" tipo_dem == 2 and id_doc ==  {documento}")

            setor_empresa = self.cadastro_empresas.query(f"cod_empresa == {demonstracao_completa.iat[0, 7]}").setor.values[0]

            empresa_nao_financeira = str(setor_empresa) not in self.setores_restritos

            if 2 in demonstracao_completa['con_ind'].to_list():

                if demonstracao_completa[(demonstracao_completa['con_ind'] == 2) & (demonstracao_completa['numero_conta'] == "1")]['valor_conta'].iat[0] != 0:

                    demonstracao_completa = demonstracao_completa.query("con_ind == 2")

                elif len(set(demonstracao_completa['valor_conta'])) == 1:

                    try:

                        demonstracao_completa = demonstracao_completa.query("con_ind == 2")
                    
                    except:

                        demonstracao_completa = demonstracao_completa.query("con_ind == 1")

                else:

                    demonstracao_completa = demonstracao_completa.query("con_ind == 1")

            ativo_total = demonstracao_completa.query("numero_conta == '1'")['valor_conta'].iat[0] 

            id_doc = demonstracao_completa.iat[0, 0]
            data_reb = demonstracao_completa.iat[0, 1]
            data_ref = demonstracao_completa.iat[0, 2]
            cod_empresa = demonstracao_completa.iat[0, 7]
            tipo_doc = demonstracao_completa.iat[0, 9]

            id_dado_ativo = f"{cod_empresa}_{id_doc}_ativo_total" 

            db_ativo = pd.DataFrame(data = {'id_dado_fundamentalista': id_dado_ativo,
                                            'data_reb': data_reb,
                                            'data_ref': data_ref,
                                            'cod_empresa': cod_empresa,
                                            'id_doc': id_doc,
                                            'nome_indicador': 'ativo_total',
                                            'periodicidade': 'pontual',
                                            'valor': ativo_total,
                                            'tipo_doc': tipo_doc}, index= [0])
            
            self.lista_df_indicadores.append(db_ativo)
            
            if empresa_nao_financeira:

                try:

                    caixa_1 = demonstracao_completa.query("numero_conta == '1.01.01'")['valor_conta'].iat[0] 
                    caixa_2 = demonstracao_completa.query("numero_conta == '1.01.02'")['valor_conta'].iat[0]

                    caixa = caixa_1 + caixa_2   

                except:

                    caixa = demonstracao_completa.query("numero_conta == '1.01'")['valor_conta'].iat[0]

        
                
                id_dado_caixa = f"{cod_empresa}_{id_doc}_caixa" 

                db_caixa = pd.DataFrame(data = {'id_dado_fundamentalista': id_dado_caixa,
                                                'data_reb': data_reb,
                                                'data_ref': data_ref,
                                                'cod_empresa': cod_empresa,
                                                'id_doc': id_doc,
                                                'nome_indicador': 'caixa',
                                                'periodicidade': 'pontual',
                                                'valor': caixa,
                                                'tipo_doc': tipo_doc}, index= [0])

                
                self.lista_df_indicadores.append(db_caixa)

    def pegando_indicadores_passivo(self):

        for documento in self.documentos:

            print(documento, "passivo")

            demonstracao_completa = self.demonstracoes_sql.query(f" tipo_dem == 3 and id_doc ==  {documento}")

            if 2 in demonstracao_completa['con_ind'].to_list():

                if demonstracao_completa[(demonstracao_completa['con_ind'] == 2) & (demonstracao_completa['numero_conta'] == "2")]['valor_conta'].iat[0] != 0:

                    demonstracao_completa = demonstracao_completa.query("con_ind == 2")

                elif len(set(demonstracao_completa['valor_conta'])) == 1:

                    try:

                        demonstracao_completa = demonstracao_completa.query("con_ind == 2")
                    
                    except:

                        demonstracao_completa = demonstracao_completa.query("con_ind == 1")

                else:

                    demonstracao_completa = demonstracao_completa.query("con_ind == 1")

            setor_empresa = self.cadastro_empresas.query(f"cod_empresa == {demonstracao_completa.iat[0, 7]}").setor.unique()

            setor_empresa = setor_empresa[0]

            empresa_nao_financeira = str(setor_empresa) not in self.setores_restritos

            id_doc = demonstracao_completa.iat[0, 0]
            data_reb = demonstracao_completa.iat[0, 1]
            data_ref = demonstracao_completa.iat[0, 2]
            cod_empresa = demonstracao_completa.iat[0, 7]
            tipo_doc = demonstracao_completa.iat[0, 9]
            id_divida = f"{cod_empresa}_{id_doc}_divida_bruta" 
            id_passivo = f"{cod_empresa}_{id_doc}_passivo_total"
            id_pl = f"{cod_empresa}_{id_doc}_pl" 

            passivo_total = demonstracao_completa.query("numero_conta == '2'")['valor_conta'].iat[0] 

            if empresa_nao_financeira:

                divida_cp = demonstracao_completa.query('numero_conta == "2.01.04"').valor_conta.values 
                
                divida_lp = demonstracao_completa.query('numero_conta == "2.02.01"').valor_conta.values

                passivo_arrend = 0
            
                linhas_da_demonstracao = range(0, len(demonstracao_completa.iloc[:, 0]))

                for i in linhas_da_demonstracao: 

                    if 'Arrendamento' in demonstracao_completa.iloc[i,5]:

                        if '2.01.04' not in demonstracao_completa.iloc[i,4] and '2.02.01' not in demonstracao_completa.iloc[i,4]:
                        
                            passivo_arrend = passivo_arrend  + demonstracao_completa.iloc[i,6]

                divida_total = divida_cp + divida_lp + passivo_arrend 
                
                patrimonio_liquido = demonstracao_completa.query("numero_conta == '2.03'")['valor_conta'].iat[0]
            
                db_divida = pd.DataFrame(data = {'id_dado_fundamentalista': id_divida,
                                                'data_reb': data_reb,
                                                'data_ref': data_ref,
                                                'cod_empresa': cod_empresa,
                                                'id_doc': id_doc,
                                                'nome_indicador': 'divida_bruta',
                                                'periodicidade': 'pontual',
                                                'valor': divida_total,
                                                'tipo_doc': tipo_doc}, index= [0])
                
                self.lista_df_indicadores.append(db_divida)

            else:
                try:
                    patrimonio_liquido = demonstracao_completa.query("numero_conta == '2.05'")['valor_conta'].iat[0]

                except:

                    patrimonio_liquido = 0

            db_passivo = pd.DataFrame(data = {'id_dado_fundamentalista': id_passivo,
                                            'data_reb': data_reb,
                                            'data_ref': data_ref,
                                            'cod_empresa': cod_empresa,
                                            'id_doc': id_doc,
                                            'nome_indicador': 'passivo_total',
                                            'periodicidade': 'pontual',
                                            'valor': passivo_total,
                                            'tipo_doc': tipo_doc}, index= [0])

            db_pl = pd.DataFrame(data = {'id_dado_fundamentalista': id_pl,
                                            'data_reb': data_reb,
                                            'data_ref': data_ref,
                                            'cod_empresa': cod_empresa,
                                            'id_doc': id_doc,
                                            'nome_indicador': 'patrimonio_liquido',
                                            'periodicidade': 'pontual',
                                            'valor': patrimonio_liquido,
                                            'tipo_doc': tipo_doc}, index= [0])


            
            self.lista_df_indicadores.append(db_passivo)
            self.lista_df_indicadores.append(db_pl)

    def colocando_indicadores_na_base(self):

        self.aws.iniciar_conexao()

        id_dado_fundamentalista = pd.read_sql('''SELECT id_dado_fundamentalista FROM dados_fundamentalistas''', con=self.aws.engine)['id_dado_fundamentalista'].to_list()

        dados_fundamentalistas = pd.concat(self.lista_df_indicadores)

        dados_fundamentalistas['data_reb'] = pd.to_datetime(dados_fundamentalistas['data_reb'])
        dados_fundamentalistas['data_ref'] = pd.to_datetime(dados_fundamentalistas['data_ref'])

        dados_fundamentalistas = dados_fundamentalistas[~dados_fundamentalistas['id_dado_fundamentalista'].isin(id_dado_fundamentalista)]
        dados_fundamentalistas = dados_fundamentalistas.reset_index(drop=True)
        dados_fundamentalistas = dados_fundamentalistas.drop_duplicates(["id_dado_fundamentalista"])

        dados_fundamentalistas.to_sql('dados_fundamentalistas', self.aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')

if __name__ == "__main__":

    lista_anos_iniciais = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
    lista_anos_finais = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]

    for i, item in enumerate(lista_anos_finais):

        data_inicio_do_periodo = f'20{lista_anos_iniciais[i]}-12-30' #digitar nesse formato
        data_final_do_periodo = f'20{lista_anos_finais[i]}-12-30'

        teste_indicador = balanco_patrimonial_indicadores(primeiro_dia_indicador = data_inicio_do_periodo, ultimo_dia_indicador = data_final_do_periodo)

        teste_indicador.coletando_balancos_cvm()
        teste_indicador.coletando_dados_de_cadastro()
        teste_indicador.pegando_indicadores_ativo()
        teste_indicador.pegando_indicadores_passivo()
        teste_indicador.colocando_indicadores_na_base()
