import pandas as pd
from conexao_banco import conexao_aws
import os
from datetime import datetime


class dre_indicadores:

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

            self.demonstracoes_sql = pd.read_sql(f'''SELECT c.id_doc, c.data_reb, c.data_ref, d.con_ind, d.numero_conta,
                                    d.nome_conta, d.valor_conta, c.cod_empresa, d.tipo_dem, c.tipo_doc
                                    FROM cadastro_dem AS c
                                    LEFT JOIN dados_financeiros AS d USING(id_doc)
                                    WHERE  d.tipo_dem = 4 AND
                                    (c.data_ref >= '{self.primeiro_dia_indicador}' AND c.data_ref < '{self.ultimo_dia_indicador}') '''
                                    , con=self.aws.engine)


        else:

            self.demonstracoes_sql = pd.read_sql(f'''SELECT c.id_doc, c.data_reb, c.data_ref, d.con_ind, d.numero_conta,
                                    d.nome_conta, d.valor_conta, c.cod_empresa, d.tipo_dem, c.tipo_doc
                                    FROM cadastro_dem AS c
                                    LEFT JOIN dados_financeiros AS d USING(id_doc)
                                    WHERE  d.tipo_dem = 4 AND
                                    (c.data_ref >= '{self.primeiro_dia_indicador}' AND c.data_ref < '{self.ultimo_dia_indicador}') AND c.cod_empresa = {self.tuple_cod_empresas}'''
                                    , con=self.aws.engine)

        self.documentos = (self.demonstracoes_sql.sort_values(by = 'data_ref', ascending=True))['id_doc'].unique()

    def coletando_dados_de_cadastro(self):

        self.cadastro_empresas = pd.read_sql('''SELECT cod, cod_empresa, setor from cadastro_empresas''', con= self.aws.engine)

    def coletando_dados_fundamentalistas(self):

        self.dados_fundamentalistas_antigos = pd.read_sql('''SELECT * FROM dados_fundamentalistas''', con = self.aws.engine)

        self.dados_fundamentalistas_antigos["data_reb"] = pd.to_datetime(self.dados_fundamentalistas_antigos["data_reb"])
        self.dados_fundamentalistas_antigos["data_ref"] = pd.to_datetime(self.dados_fundamentalistas_antigos["data_ref"]).dt.date

    def pegando_indicadores_dre(self):

        #verificar se é DFP. Caso seja, cria o indicador consolidado de 12m e busca 3 trimestres anteriores para calcular o 3 tri.
        #caso nao exista referencias anteriores, desiste. obs: 3 trimestres com recebimento menor que o recebimento da DFP.
        
        #Caso seja ITR, buscar 3 trimestres anteriores para calcular 12M. Se não existir, deiste. obs: 3 trimestres com recebimento menor que o ITR
         
        self.lista_df_indicadores = []

        self.documentos = list(filter(lambda x: x > 11198, self.documentos))   

        for documento in self.documentos:

            print(documento)

            self.aws.iniciar_conexao()

            self.coletando_dados_fundamentalistas()

            demonstracao_completa = self.demonstracoes_sql.query(f"id_doc ==  {documento}")

            nome_documento = demonstracao_completa.iat[0, 9].upper()

            dfp = nome_documento == "DFP"

            if 2 in demonstracao_completa['con_ind'].to_list():

                if demonstracao_completa[(demonstracao_completa['con_ind'] == 2) & (demonstracao_completa['numero_conta'] == "3.01")]['valor_conta'].iat[0] != 0:

                    demonstracao_completa = demonstracao_completa.query("con_ind == 2")

                elif len(set(demonstracao_completa['valor_conta'])) == 1:

                    try:

                        demonstracao_completa = demonstracao_completa.query("con_ind == 2")
                    
                    except:

                        demonstracao_completa = demonstracao_completa.query("con_ind == 1")

                else:

                    demonstracao_completa = demonstracao_completa.query("con_ind == 1")

            setor_empresa = self.cadastro_empresas.query(f"cod_empresa == {demonstracao_completa.iat[0, 7]}").setor.unique()

            empresa_nao_financeira = (str(setor_empresa[0]) not in self.setores_restritos) and (str(setor_empresa[0]) != "Bancos")

            id_doc = demonstracao_completa.iat[0, 0]
            data_reb = demonstracao_completa.iat[0, 1]
            data_ref = demonstracao_completa.iat[0, 2]
            cod_empresa = str(demonstracao_completa.iat[0, 7])
            tipo_doc = demonstracao_completa.iat[0, 9]

            if empresa_nao_financeira:

                lista_indicadores = ['receita', 'cpv', 'lucro_bruto', 'despesa_operacional', 'ebit', 
                                    'resultado_financeiro', 'resultado_antes_do_ir', 'ir', 'lucro_liquido']

                lista_id_3m = [f"{cod_empresa}_{id_doc}_{indicador}_3m" for indicador in lista_indicadores]
                lista_id_12m = [f"{cod_empresa}_{id_doc}_{indicador}_12m" for indicador in lista_indicadores]

                if dfp:

                    lista_atributos = self.pegando_dado_pontual(df = demonstracao_completa)

                    for i, indicador in enumerate(lista_indicadores):

                        self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_12m[i],
                                            'data_reb': data_reb,
                                            'data_ref': data_ref,
                                            'cod_empresa': cod_empresa,
                                            'id_doc': id_doc,
                                            'nome_indicador': indicador,
                                            'periodicidade': '12m',
                                            'valor': lista_atributos[i],
                                                'tipo_doc': tipo_doc}, index= [0]))
                        
                    possui_refencias = self.verificando_a_existencia_de_3_referencias_anteriores(cod_empresa, data_reb, data_ref)

                    if possui_refencias:

                        lista_atributos_trimestral = self.puxando_dados_9m(self.referencias_para_filtro, self.referencias_antigas, nome_documento, lista_atributos)

                        for i, indicador in enumerate(lista_indicadores):

                            self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_3m[i],
                                'data_reb': data_reb,
                                'data_ref': data_ref,
                                'cod_empresa': cod_empresa,
                                'id_doc': id_doc,
                                'nome_indicador': indicador,
                                'periodicidade': '3m',
                                'valor': lista_atributos_trimestral[i],
                                                'tipo_doc': tipo_doc}, index= [0]))
                

                else:

                    lista_atributos = self.pegando_dado_pontual(df = demonstracao_completa)

                    for i, indicador in enumerate(lista_indicadores):

                        self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_3m[i],
                                            'data_reb': data_reb,
                                            'data_ref': data_ref,
                                            'cod_empresa': cod_empresa,
                                            'id_doc': id_doc,
                                            'nome_indicador': indicador,
                                            'periodicidade': '3m',
                                            'valor': lista_atributos[i],
                                                'tipo_doc': tipo_doc}, index= [0]))
                        
                    possui_refencias = self.verificando_a_existencia_de_3_referencias_anteriores(cod_empresa, data_reb, data_ref)

                    if possui_refencias:

                        lista_atributos = self.puxando_dados_9m(self.referencias_para_filtro, self.referencias_antigas, nome_documento, lista_atributos)

                        for i, indicador in enumerate(lista_indicadores):

                            self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_12m[i],
                                'data_reb': data_reb,
                                'data_ref': data_ref,
                                'cod_empresa': cod_empresa,
                                'id_doc': id_doc,
                                'nome_indicador': indicador,
                                'periodicidade': '12m',
                                'valor': lista_atributos[i],
                                                'tipo_doc': tipo_doc}, index= [0]))
            
            elif str(setor_empresa[0]) == "Bancos":

                lista_indicadores = ['receita_intermediação_financeira', 'despesas_intermediação', 'lucro_bruto', 'outras_despesas', 
                                    'resultado_antes_do_ir', 'ir', 'lucro_liquido']

                lista_id_3m = [f"{cod_empresa}_{id_doc}_{indicador}_3m" for indicador in lista_indicadores]
                lista_id_12m = [f"{cod_empresa}_{id_doc}_{indicador}_12m" for indicador in lista_indicadores]

                if dfp:

                    lista_atributos = self.pegando_dado_pontual_bancario(df = demonstracao_completa)

                    for i, indicador in enumerate(lista_indicadores):

                        self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_12m[i],
                                            'data_reb': data_reb,
                                            'data_ref': data_ref,
                                            'cod_empresa': cod_empresa,
                                            'id_doc': id_doc,
                                            'nome_indicador': indicador,
                                            'periodicidade': '12m',
                                            'valor': lista_atributos[i],
                                                'tipo_doc': tipo_doc}, index= [0]))

                    possui_refencias = self.verificando_a_existencia_de_3_referencias_anteriores(cod_empresa, data_reb, data_ref)

                    if possui_refencias:

                        lista_atributos_trimestral = self.puxando_dados_9m_bancario(self.referencias_para_filtro, self.referencias_antigas, nome_documento, lista_atributos)

                        for i, indicador in enumerate(lista_indicadores):

                            self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_3m[i],
                                'data_reb': data_reb,
                                'data_ref': data_ref,
                                'cod_empresa': cod_empresa,
                                'id_doc': id_doc,
                                'nome_indicador': indicador,
                                'periodicidade': '3m',
                                'valor': lista_atributos_trimestral[i],
                                                'tipo_doc': tipo_doc}, index= [0]))

                else:

                    lista_atributos = self.pegando_dado_pontual_bancario(df = demonstracao_completa)

                    for i, indicador in enumerate(lista_indicadores):

                        self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_3m[i],
                                            'data_reb': data_reb,
                                            'data_ref': data_ref,
                                            'cod_empresa': cod_empresa,
                                            'id_doc': id_doc,
                                            'nome_indicador': indicador,
                                            'periodicidade': '3m',
                                            'valor': lista_atributos[i],
                                                'tipo_doc': tipo_doc}, index= [0]))
                        
                    possui_refencias = self.verificando_a_existencia_de_3_referencias_anteriores(cod_empresa, data_reb, data_ref)

                    if possui_refencias:

                        lista_atributos = self.puxando_dados_9m_bancario(self.referencias_para_filtro, self.referencias_antigas, nome_documento, lista_atributos)

                        for i, indicador in enumerate(lista_indicadores):

                            self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_12m[i],
                                'data_reb': data_reb,
                                'data_ref': data_ref,
                                'cod_empresa': cod_empresa,
                                'id_doc': id_doc,
                                'nome_indicador': indicador,
                                'periodicidade': '12m',
                                'valor': lista_atributos[i],
                                                'tipo_doc': tipo_doc}, index= [0]))
            else:

                pass

        self.colocando_indicadores_na_base()       

    
    def pegando_dado_pontual(self, df):

        receita = df.query("numero_conta == '3.01'")['valor_conta'].iat[0] 

        cpv = df.query("numero_conta == '3.02'")['valor_conta'].iat[0] 

        lucro_bruto = df.query("numero_conta == '3.03'")['valor_conta'].iat[0] 

        despesa_operacional = df.query("numero_conta == '3.04'")['valor_conta'].iat[0] 

        ebit = df.query("numero_conta == '3.05'")['valor_conta'].iat[0] 

        resultado_financeiro = df.query("numero_conta == '3.06'")['valor_conta'].iat[0] 

        resultado_antes_do_ir = df.query("numero_conta == '3.07'")['valor_conta'].iat[0] 

        ir = df.query("numero_conta == '3.08'")['valor_conta'].iat[0] 

        lucro_liquido = df.query("numero_conta == '3.11'")['valor_conta'].iat[0]

        lista_atributos = [receita, cpv, lucro_bruto, despesa_operacional, ebit, resultado_financeiro,
                            resultado_antes_do_ir, ir, lucro_liquido]

        return lista_atributos

    def pegando_dado_pontual_bancario(self, df):

        consolidado = "3.13" not in df['numero_conta'].to_list()

        if consolidado:

            receita_intermediação_financeira = df.query("numero_conta == '3.01'")['valor_conta'].iat[0] 

            despesas_intermediação = df.query("numero_conta == '3.02'")['valor_conta'].iat[0] 

            lucro_bruto = df.query("numero_conta == '3.03'")['valor_conta'].iat[0] 

            outras_despesas = df.query("numero_conta == '3.04'")['valor_conta'].iat[0] 

            resultado_antes_do_ir = df.query("numero_conta == '3.05'")['valor_conta'].iat[0] 

            ir = df.query("numero_conta == '3.06'")['valor_conta'].iat[0] 

            lucro_liquido = df.query("numero_conta == '3.09'")['valor_conta'].iat[0] 

            lista_atributos = [receita_intermediação_financeira, despesas_intermediação, lucro_bruto, outras_despesas,
                                resultado_antes_do_ir, ir, lucro_liquido]

            return lista_atributos

        else:

            receita_intermediação_financeira = df.query("numero_conta == '3.01'")['valor_conta'].iat[0] 

            despesas_intermediação = df.query("numero_conta == '3.02'")['valor_conta'].iat[0] 

            lucro_bruto = df.query("numero_conta == '3.03'")['valor_conta'].iat[0] 

            outras_despesas = df.query("numero_conta == '3.04'")['valor_conta'].iat[0] 

            resultado_antes_do_ir = df.query("numero_conta == '3.07'")['valor_conta'].iat[0] 

            ir = df.query("numero_conta == '3.08'")['valor_conta'].iat[0] 

            lucro_liquido = df.query("numero_conta == '3.13'")['valor_conta'].iat[0] 

            lista_atributos = [receita_intermediação_financeira, despesas_intermediação, lucro_bruto, outras_despesas,
                                resultado_antes_do_ir, ir, lucro_liquido]

            return lista_atributos



    def verificando_a_existencia_de_3_referencias_anteriores(self, cod_empresa, data_reb, data_ref):

        referencias_antigas = self.dados_fundamentalistas_antigos[(self.dados_fundamentalistas_antigos["cod_empresa"] == cod_empresa)]
        referencias_antigas = referencias_antigas[referencias_antigas["data_reb"] < data_reb]
        referencias_antigas = referencias_antigas[referencias_antigas["data_ref"] < data_ref]
        referencias_antigas = referencias_antigas[referencias_antigas["periodicidade"] == "3m"]



        referencias_para_filtro = referencias_antigas['data_ref'].sort_values(ascending=False).unique()

        possui_refencias = True

        referencias_para_filtro = referencias_para_filtro[:3]

        if len(referencias_para_filtro) != 3:

            possui_refencias = False

        self.referencias_antigas = referencias_antigas
        self.referencias_para_filtro = referencias_para_filtro

        return possui_refencias


    def puxando_dados_9m_bancario(self, referencias_para_filtro, referencias_antigas, nome_documento, lista_atributo):

        receita_9m = 0
        despesas_intermediação_9m = 0
        lucro_bruto_9m = 0
        outras_despesas_9m = 0
        resultado_antes_do_ir_9m = 0
        ir_9m = 0
        lucro_liquido_9m = 0

        for referencia in referencias_para_filtro:

            dado_mais_att = referencias_antigas[referencias_antigas['data_ref'] == referencia]

            dado_mais_att = dado_mais_att[dado_mais_att['data_reb'] == max(dado_mais_att['data_reb'])]

            receita_9m = dado_mais_att.query("nome_indicador == 'receita_intermediação_financeira' ")['valor'].iat[0] + receita_9m

            despesas_intermediação_9m = dado_mais_att.query("nome_indicador == 'despesas_intermediação' ")['valor'].iat[0] + despesas_intermediação_9m

            lucro_bruto_9m = dado_mais_att.query("nome_indicador == 'lucro_bruto' ")['valor'].iat[0] + lucro_bruto_9m

            outras_despesas_9m = dado_mais_att.query("nome_indicador == 'outras_despesas' ")['valor'].iat[0] + outras_despesas_9m

            resultado_antes_do_ir_9m = dado_mais_att.query("nome_indicador == 'resultado_antes_do_ir' ")['valor'].iat[0] + resultado_antes_do_ir_9m

            ir_9m = dado_mais_att.query("nome_indicador == 'ir' ")['valor'].iat[0] + ir_9m

            lucro_liquido_9m = dado_mais_att.query("nome_indicador == 'lucro_liquido' ")['valor'].iat[0] + lucro_liquido_9m


        lista_atributos_calculado = []

        lista_atributos_9m = [receita_9m, despesas_intermediação_9m, lucro_bruto_9m, outras_despesas_9m,
                                resultado_antes_do_ir_9m, ir_9m, lucro_liquido_9m]

        for a, atributo in enumerate(lista_atributo):

            if nome_documento == "DFP":

                dado = atributo - lista_atributos_9m[a]

                lista_atributos_calculado.append(dado)

            else:

                dado = atributo + lista_atributos_9m[a]

                lista_atributos_calculado.append(dado)

        return lista_atributos_calculado
    
    def puxando_dados_9m(self, referencias_para_filtro, referencias_antigas, nome_documento, lista_atributo):

        receita_9m = 0
        cpv_9m = 0
        lucro_bruto_9m = 0
        despesa_operacional_9m = 0
        ebit_9m = 0 
        resultado_financeiro_9m = 0
        resultado_antes_do_ir_9m = 0
        ir_9m = 0
        lucro_liquido_9m = 0

        for referencia in referencias_para_filtro:

            dado_mais_att = referencias_antigas[referencias_antigas['data_ref'] == referencia]

            dado_mais_att = dado_mais_att[dado_mais_att['data_reb'] == max(dado_mais_att['data_reb'])]

            receita_9m = dado_mais_att.query("nome_indicador == 'receita' ")['valor'].iat[0] + receita_9m

            cpv_9m = dado_mais_att.query("nome_indicador == 'cpv' ")['valor'].iat[0] + cpv_9m

            lucro_bruto_9m = dado_mais_att.query("nome_indicador == 'lucro_bruto' ")['valor'].iat[0] + lucro_bruto_9m

            despesa_operacional_9m = dado_mais_att.query("nome_indicador == 'despesa_operacional' ")['valor'].iat[0] + despesa_operacional_9m

            ebit_9m = dado_mais_att.query("nome_indicador == 'ebit' ")['valor'].iat[0] + ebit_9m

            resultado_financeiro_9m = dado_mais_att.query("nome_indicador == 'resultado_financeiro' ")['valor'].iat[0] + resultado_financeiro_9m

            resultado_antes_do_ir_9m = dado_mais_att.query("nome_indicador == 'resultado_antes_do_ir' ")['valor'].iat[0] + resultado_antes_do_ir_9m

            ir_9m = dado_mais_att.query("nome_indicador == 'ir' ")['valor'].iat[0] + ir_9m

            lucro_liquido_9m = dado_mais_att.query("nome_indicador == 'lucro_liquido' ")['valor'].iat[0] + lucro_liquido_9m


        lista_atributos_calculado = []

        lista_atributos_9m = [receita_9m, cpv_9m, lucro_bruto_9m, despesa_operacional_9m, ebit_9m, resultado_financeiro_9m,
                                resultado_antes_do_ir_9m, ir_9m, lucro_liquido_9m]

        for a, atributo in enumerate(lista_atributo):

            if nome_documento == "DFP":

                dado = atributo - lista_atributos_9m[a]

                lista_atributos_calculado.append(dado)

            else:

                dado = atributo + lista_atributos_9m[a]

                lista_atributos_calculado.append(dado)

        return lista_atributos_calculado

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

        print(f'comecou o {i}')

        teste_indicador = dre_indicadores(primeiro_dia_indicador = data_inicio_do_periodo, ultimo_dia_indicador = data_final_do_periodo)

        teste_indicador.coletando_balancos_cvm()
        teste_indicador.coletando_dados_de_cadastro()
        teste_indicador.pegando_indicadores_dre()

        #teste_indicador.colocando_indicadores_na_base()